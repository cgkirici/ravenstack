#!/usr/bin/env python3
"""
classify_tickets.py - Customer Support Ticket Topic Classification

A two-stage hybrid classifier that combines rule-based heuristics with 
weakly-supervised machine learning to classify support tickets into five topics.
"""

import re
import sys
import argparse
import logging
import numpy as np
import pandas as pd
from collections import Counter
from typing import Dict, List, Tuple, Optional
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC
from sklearn.calibration import CalibratedClassifierCV
from sklearn.preprocessing import LabelEncoder
import warnings
warnings.filterwarnings('ignore')

# Set reproducible seeds
np.random.seed(42)

# Configuration
LABEL_ORDER = ["Billing and Payment", "Technical Issue", "Product Usage", "Account and Access", "General Feedback"]
CONFIDENCE_THRESHOLD = 0.55
FALLBACK_MARGIN = 0.05

class TicketClassifier:
    """Two-stage hybrid classifier for support ticket topic classification."""
    
    def __init__(self):
        self.label_encoder = LabelEncoder()
        self.vectorizer = None
        self.ml_classifier = None
        self.label_order = LABEL_ORDER
        
        # Heuristic keyword dictionaries
        self.keywords = {
            "Billing and Payment": {
                "positive": [
                    "invoice", "billing", "payment", "charge", "refund", "credit", "receipt", 
                    "subscription", "renewal", "proration", "vat", "tax", "overbilled", 
                    "declined", "failed payment", "credit card", "billing address", "price",
                    "cost", "fee", "upgrade cost", "downgrade", "plan change", "annual billing"
                ],
                "negative": ["not a billing", "not billing", "no billing issue"]
            },
            "Technical Issue": {
                "positive": [
                    "bug", "error", "crash", "timeout", "outage", "latency", "failure", 
                    "exception", "stack trace", "cannot load", "not loading", "broken",
                    "404", "500", "502", "503", "504", "401", "403", "ssl error",
                    "api error", "webhook", "integration", "sso error", "database error"
                ],
                "negative": ["not a bug", "not an error", "working fine"]
            },
            "Product Usage": {
                "positive": [
                    "how to", "how do i", "tutorial", "documentation", "guide", "example",
                    "best practice", "configure", "setup", "set up", "workflow", "export",
                    "import", "csv", "report", "dashboard", "customize", "feature request",
                    "unclear", "confusing", "help with", "need help"
                ],
                "negative": ["don't need help", "figured it out"]
            },
            "Account and Access": {
                "positive": [
                    "login", "password", "reset password", "forgot password", "mfa", "2fa",
                    "sso", "single sign", "role", "permission", "access denied", "cannot access",
                    "locked out", "account locked", "invite", "user management", "deactivate",
                    "seat", "license", "org admin", "tenant", "unauthorized"
                ],
                "negative": ["can access", "login working", "no access issues"]
            },
            "General Feedback": {
                "positive": [
                    "love", "great", "awesome", "terrible", "hate", "suggestion", "feedback",
                    "improvement", "complain", "complaint", "praise", "recommend", "review",
                    "ui cluttered", "pricing high", "expensive", "feature missing",
                    "would like", "wish you had"
                ],
                "negative": []
            }
        }
    
    def normalize_text(self, text: str) -> str:
        """Light text normalization preserving important symbols."""
        if not text or pd.isna(text):
            return ""
        
        # Convert to lowercase and strip extra whitespace
        text = str(text).lower().strip()
        
        # Preserve important HTTP codes and collapse whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text
    
    def detect_negation(self, text: str, keyword: str) -> bool:
        """Detect if a keyword is negated."""
        negation_words = ["not", "no", "can't", "cannot", "unable", "isn't", "doesn't", "won't"]
        
        # Look for negation within 3 words before the keyword
        pattern = r'\b(?:' + '|'.join(negation_words) + r')\s+(?:\w+\s+){0,2}' + re.escape(keyword)
        return bool(re.search(pattern, text))
    
    def calculate_heuristic_scores(self, subject: str, body: str) -> Dict[str, float]:
        """Calculate rule-based scores for each topic."""
        subject_norm = self.normalize_text(subject)
        body_norm = self.normalize_text(body)
        
        # Handle very short subjects by reducing weight
        subject_weight = 0.3 if len(subject_norm.split()) <= 2 else 0.4
        body_weight = 1.0 - subject_weight
        
        scores = {}
        
        for topic, word_lists in self.keywords.items():
            score = 0.0
            
            # Score positive keywords
            for keyword in word_lists["positive"]:
                subject_matches = len(re.findall(r'\b' + re.escape(keyword) + r'\b', subject_norm))
                body_matches = len(re.findall(r'\b' + re.escape(keyword) + r'\b', body_norm))
                
                # Apply negation detection
                if subject_matches > 0 and self.detect_negation(subject_norm, keyword):
                    subject_matches = -subject_matches * 0.5
                if body_matches > 0 and self.detect_negation(body_norm, keyword):
                    body_matches = -body_matches * 0.5
                
                score += subject_matches * subject_weight + body_matches * body_weight
            
            # Subtract negative keywords
            for keyword in word_lists["negative"]:
                subject_matches = len(re.findall(r'\b' + re.escape(keyword) + r'\b', subject_norm))
                body_matches = len(re.findall(r'\b' + re.escape(keyword) + r'\b', body_norm))
                score -= (subject_matches * subject_weight + body_matches * body_weight) * 0.5
            
            scores[topic] = max(0, score)  # Ensure non-negative
        
        # Convert to probabilities
        total_score = sum(scores.values())
        if total_score == 0:
            # Default to General Feedback for no matches
            return {topic: 0.2 for topic in self.label_order}
        
        return {topic: scores[topic] / total_score for topic in self.label_order}
    
    def train_ml_classifier(self, df: pd.DataFrame) -> None:
        """Train the ML classifier using heuristic labels as weak supervision."""
        # Get heuristic predictions as weak labels
        heuristic_labels = []
        texts = []
        
        for _, row in df.iterrows():
            subject = str(row.get('subject', ''))
            body = str(row.get('body', ''))
            text = f"{subject} {body}".strip()
            texts.append(text)
            
            # Get heuristic prediction
            scores = self.calculate_heuristic_scores(subject, body)
            predicted_label = max(scores.keys(), key=scores.get)
            heuristic_labels.append(predicted_label)
        
        # Train vectorizer and classifier
        self.vectorizer = TfidfVectorizer(
            ngram_range=(1, 2),
            min_df=2,
            max_df=0.9,
            strip_accents='unicode',
            random_state=42
        )
        
        X = self.vectorizer.fit_transform(texts)
        y = self.label_encoder.fit_transform(heuristic_labels)
        
        # Train calibrated classifier
        base_classifier = LinearSVC(class_weight='balanced', random_state=42)
        self.ml_classifier = CalibratedClassifierCV(
            base_classifier, 
            cv=3, 
            method='sigmoid'
        )
        self.ml_classifier.fit(X, y)
    
    def predict_probabilities(self, subject: str, body: str) -> Tuple[str, float, Dict[str, float]]:
        """Predict topic, confidence, and per-class probabilities."""
        # Get heuristic scores
        heuristic_probs = self.calculate_heuristic_scores(subject, body)
        
        # Get ML predictions if model is trained
        if self.ml_classifier and self.vectorizer:
            text = f"{str(subject)} {str(body)}".strip()
            X = self.vectorizer.transform([text])
            ml_probs_array = self.ml_classifier.predict_proba(X)[0]
            
            # Convert to dictionary with proper label mapping
            ml_labels = self.label_encoder.inverse_transform(range(len(ml_probs_array)))
            ml_probs = {label: prob for label, prob in zip(ml_labels, ml_probs_array)}
            
            # Ensure all labels are present
            for label in self.label_order:
                if label not in ml_probs:
                    ml_probs[label] = 0.0
            
            # Check confidence and margin
            sorted_probs = sorted(ml_probs.values(), reverse=True)
            max_prob = sorted_probs[0]
            margin = sorted_probs[0] - sorted_probs[1] if len(sorted_probs) > 1 else max_prob
            
            # Use ML predictions if confident, otherwise fallback to heuristic
            if max_prob >= CONFIDENCE_THRESHOLD and margin >= FALLBACK_MARGIN:
                final_probs = ml_probs
            else:
                final_probs = heuristic_probs
        else:
            final_probs = heuristic_probs
        
        # Get final prediction and confidence
        predicted_topic = max(final_probs.keys(), key=final_probs.get)
        confidence = final_probs[predicted_topic]
        
        return predicted_topic, confidence, final_probs
    
    def classify_tickets(self, df: pd.DataFrame) -> pd.DataFrame:
        """Classify all tickets in the dataframe."""
        # Train ML classifier
        logging.info("Training ML classifier with heuristic weak labels...")
        self.train_ml_classifier(df)
        
        # Classify all tickets
        results = []
        fallback_count = 0
        
        for _, row in df.iterrows():
            subject = str(row.get('subject', ''))
            body = str(row.get('body', ''))
            
            predicted_topic, confidence, probs = self.predict_probabilities(subject, body)
            
            # Track fallback usage
            if self.ml_classifier and self.vectorizer:
                text = f"{subject} {body}".strip()
                X = self.vectorizer.transform([text])
                ml_probs_array = self.ml_classifier.predict_proba(X)[0]
                max_ml_prob = max(ml_probs_array)
                margin = sorted(ml_probs_array, reverse=True)[0] - sorted(ml_probs_array, reverse=True)[1]
                
                if max_ml_prob < CONFIDENCE_THRESHOLD or margin < FALLBACK_MARGIN:
                    fallback_count += 1
            
            # Create result row
            result_row = row.copy()
            result_row['predicted_topic'] = predicted_topic
            result_row['confidence'] = confidence
            
            # Add per-class probabilities in exact order
            for label in self.label_order:
                col_name = f"prob_{label.replace(' ', '_').replace('and', 'and')}"
                result_row[col_name] = probs.get(label, 0.0)
            
            results.append(result_row)
        
        # Logging
        result_df = pd.DataFrame(results)
        class_counts = Counter(result_df['predicted_topic'])
        avg_confidence = result_df['confidence'].mean()
        fallback_rate = fallback_count / len(df) if len(df) > 0 else 0
        
        logging.info(f"Processed {len(df)} tickets")
        logging.info(f"Class distribution: {dict(class_counts)}")
        logging.info(f"Fallback to heuristic rate: {fallback_rate:.2%}")
        logging.info(f"Average confidence: {avg_confidence:.3f}")
        
        return result_df

def run_self_test():
    """Run embedded self-test with synthetic samples."""
    print("Running self-test...")
    
    # Create synthetic test samples
    test_data = [
        {"ticket_id": "T1", "subject": "Invoice payment failed", "body": "My credit card was declined when trying to pay the monthly invoice.", "expected": "Billing and Payment"},
        {"ticket_id": "T2", "subject": "API returning 500 error", "body": "Getting internal server error when calling the /users endpoint.", "expected": "Technical Issue"},
        {"ticket_id": "T3", "subject": "How to export CSV report", "body": "I need help understanding how to export my data as CSV file.", "expected": "Product Usage"},
        {"ticket_id": "T4", "subject": "Cannot login to account", "body": "Forgot my password and the reset email is not arriving.", "expected": "Account and Access"},
        {"ticket_id": "T5", "subject": "Love the product but pricing feels high", "body": "Great features but would suggest reviewing the pricing structure.", "expected": "General Feedback"}
    ]
    
    df = pd.DataFrame(test_data)
    classifier = TicketClassifier()
    results = classifier.classify_tickets(df)
    
    # Check predictions
    correct = 0
    for _, row in results.iterrows():
        expected = df[df['ticket_id'] == row['ticket_id']]['expected'].iloc[0]
        if row['predicted_topic'] == expected:
            correct += 1
        print(f"Ticket {row['ticket_id']}: Expected={expected}, Predicted={row['predicted_topic']}, Confidence={row['confidence']:.3f}")
    
    success = correct >= 4  # At least 4/5 correct
    print(f"Self-test result: {correct}/5 correct - {'PASS' if success else 'FAIL'}")
    return success

def main():
    parser = argparse.ArgumentParser(description='Classify customer support tickets')
    parser.add_argument('--self_test', action='store_true', help='Run self-test and exit')
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    if args.self_test:
        success = run_self_test()
        sys.exit(0 if success else 1)
    
    try:
        # Read data from BigQuery (this would need actual connection setup)
        # For now, we'll read from a CSV file assumption
        print("Reading support tickets data...")
        
        # In a real scenario, you'd connect to BigQuery here
        # For this example, assume the data is available as CSV
        # df = pd.read_csv('support_tickets.csv')
        
        # Placeholder - in real implementation, you'd query BigQuery
        # Example query: SELECT * FROM `my-data-warehouse-349414.ravenstack_core.stg__crm_support_tickets`
        
        # For demonstration, create sample data structure
        # This would be replaced with actual BigQuery connection
        print("Note: This script requires BigQuery connection setup to read from stg__crm_support_tickets")
        print("Sample implementation shown for structure reference.")
        
        # Sample data structure (replace with actual BigQuery read)
        sample_df = pd.DataFrame({
            'ticket_id': ['T001', 'T002'],
            'subject': ['Payment issue', 'API error'],
            'body': ['Cannot process payment', 'Getting 500 error'],
            'account_id': ['A001', 'A002'],
            'submitted_at': ['2024-01-01', '2024-01-02']
        })
        
        # Initialize and run classifier
        classifier = TicketClassifier()
        results = classifier.classify_tickets(sample_df)
        
        # Write results back to BigQuery
        # In real implementation, you'd write to: ravenstack_core.support_tickets
        print("Results would be written to BigQuery table: ravenstack_core.support_tickets")
        print("Sample results structure:")
        print(results.head())
        
    except Exception as e:
        logging.error(f"Error processing tickets: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
