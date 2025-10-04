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

try:
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False
    logging.warning("Google Cloud BigQuery not available. Install with: pip install google-cloud-bigquery")

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    logging.warning("PyYAML not available. Install with: pip install PyYAML")

import os
from pathlib import Path

# Set reproducible seeds
np.random.seed(42)

# Configuration
LABEL_ORDER = ["Billing and Payment", "Technical Issue", "Product Usage", "Account and Access", "General Feedback"]
CONFIDENCE_THRESHOLD = 0.55
FALLBACK_MARGIN = 0.05

def load_dbt_profile(profile_name: str = "default", target: str = "default") -> Dict:
    """Load BigQuery connection parameters from dbt profiles.yml."""
    profiles_path = Path.home() / ".dbt" / "profiles.yml"
    
    if not profiles_path.exists():
        raise FileNotFoundError(f"dbt profiles.yml not found at {profiles_path}")
    
    if not YAML_AVAILABLE:
        raise ImportError("PyYAML is required to read profiles.yml. Install with: pip install PyYAML")
    
    with open(profiles_path, 'r') as f:
        profiles = yaml.safe_load(f)
    
    if profile_name not in profiles:
        raise ValueError(f"Profile '{profile_name}' not found in profiles.yml")
    
    profile = profiles[profile_name]
    
    if target not in profile['outputs']:
        raise ValueError(f"Target '{target}' not found in profile '{profile_name}'")
    
    return profile['outputs'][target]

class BigQueryManager:
    """Handles BigQuery operations for reading and writing ticket data."""
    
    def __init__(self, profile_name: str = "default", target: str = "default"):
        self.profile_config = load_dbt_profile(profile_name, target)
        self.project_id = self.profile_config['project']
        self.dataset = self.profile_config['dataset']
        self.location = self.profile_config.get('location', 'US')
        self.keyfile = self.profile_config.get('keyfile')
        
        self.client = None
        if BIGQUERY_AVAILABLE:
            # Initialize client with service account key if provided
            if self.keyfile and os.path.exists(self.keyfile):
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.keyfile
                self.client = bigquery.Client(project=self.project_id, location=self.location)
            else:
                # Use default credentials
                self.client = bigquery.Client(project=self.project_id, location=self.location)
    
    def read_staging_tickets(self) -> pd.DataFrame:
        """Read tickets from the staging table."""
        if not self.client:
            raise RuntimeError("BigQuery client not available")
        
        query = f"""
        SELECT 
            ticket_id,
            account_id,
            submitted_at,
            closed_at,
            resolution_time_hours,
            priority,
            first_response_time_minutes,
            satisfaction_score,
            escalation_flag,
            subject,
            body
        FROM `{self.project_id}.{self.dataset}.stg__crm_support_tickets`
        """
        
        logging.info("Reading tickets from BigQuery staging table...")
        df = self.client.query(query).to_dataframe()
        logging.info(f"Read {len(df)} tickets from staging table")
        return df
    
    def create_output_table(self, table_name: str = "support_tickets", dataset_override: str = None):
        """Create the output table with proper schema."""
        if not self.client:
            raise RuntimeError("BigQuery client not available")
        
        dataset = dataset_override or self.dataset
        table_id = f"{self.project_id}.{dataset}.{table_name}"
        
        # Define the schema
        schema = [
            bigquery.SchemaField("ticket_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("account_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("submitted_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("closed_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("resolution_time_hours", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("priority", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("first_response_time_minutes", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("satisfaction_score", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("escalation_flag", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("subject", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("body", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("predicted_topic", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("confidence", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("prob_Billing_and_Payment", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("prob_Technical_Issue", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("prob_Product_Usage", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("prob_Account_and_Access", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("prob_General_Feedback", "FLOAT", mode="NULLABLE"),
        ]
        
        # Create table
        table = bigquery.Table(table_id, schema=schema)
        
        try:
            # Try to delete existing table first
            self.client.delete_table(table_id, not_found_ok=True)
            logging.info(f"Deleted existing table {table_id}")
        except Exception as e:
            logging.warning(f"Could not delete existing table: {e}")
        
        table = self.client.create_table(table)
        logging.info(f"Created table {table_id}")
        return table_id
    
    def write_results(self, df: pd.DataFrame, table_name: str = "support_tickets", dataset_override: str = None):
        """Write classified results to BigQuery table."""
        if not self.client:
            raise RuntimeError("BigQuery client not available")
        
        dataset = dataset_override or self.dataset
        table_id = f"{self.project_id}.{dataset}.{table_name}"
        
        # Configure write job
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE" # Overwrite table
            # schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        )
        
        logging.info(f"Writing {len(df)} classified tickets to {table_id}...")
        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        
        logging.info(f"Successfully wrote {len(df)} rows to {table_id}")
        return table_id

class TicketClassifier:
    """Two-stage hybrid classifier for support ticket topic classification."""
    
    def __init__(self):
        self.label_encoder = LabelEncoder()
        self.vectorizer = None
        self.ml_classifier = None
        self.label_order = LABEL_ORDER
        
        # Heuristic keyword dictionaries with weights and regex patterns
        self.keywords = {
            "Billing and Payment": {
                "positive": [
                    ("invoice", 1.0), ("billing", 1.0), ("payment", 1.0), ("charge", 1.0), 
                    ("refund", 1.0), ("credit", 1.0), ("receipt", 1.0), ("subscription", 1.0), 
                    ("renewal", 1.0), ("proration", 1.0), ("vat", 1.0), ("tax", 1.0), 
                    ("overbilled", 1.0), ("declined", 1.0), ("failed payment", 1.0), 
                    ("credit card", 1.0), ("billing address", 1.0), ("price", 1.0),
                    ("cost", 1.0), ("fee", 1.0), ("upgrade cost", 1.0), ("downgrade", 1.0), 
                    ("plan change", 1.0), ("annual billing", 1.0)
                ],
                "negative": [("not a billing", 1.0), ("not billing", 1.0), ("no billing issue", 1.0)]
            },
            "Technical Issue": {
                "positive": [
                    # Original keywords with standard weight
                    ("bug", 1.0), ("error", 1.0), ("crash", 1.0), ("failure", 1.0), 
                    ("exception", 1.0), ("stack trace", 1.0), ("broken", 1.0),
                    ("ssl error", 1.0), ("api error", 1.0), ("webhook", 1.0), 
                    ("integration", 1.0), ("sso error", 1.0), ("database error", 1.0),
                    # High-weight regex patterns for technical issues
                    (r"\bfail(s|ed)?\s+to\s+load\b", 3.0, True),
                    (r"\bdoes\s+not\s+load\b", 3.0, True),
                    (r"\b(can't|cannot|won't)\s+load\b", 3.0, True),
                    (r"\b(spinner|spinning\s+wheel|loading\s+forever|stuck\s+loading)\b", 3.0, True),
                    (r"\b(timeout|timed\s*out|latency)\b", 3.0, True),
                    (r"\b(crash(es|ed)?|freez(e|ing)|unresponsive)\b", 3.0, True),
                    (r"\b(outage|incident|service\s+down)\b", 3.0, True),
                    (r"\b(5\d{2}|4\d{2}|500|502|503|504|404|401|403)\b", 3.0, True),
                    (r"\b(error\s*code\s*\d+|exception|stack\s*trace)\b", 3.0, True)
                ],
                "negative": [("not a bug", 1.0), ("not an error", 1.0), ("working fine", 1.0)]
            },
            "Product Usage": {
                "positive": [
                    # Focused on how-to/tutorial intents only
                    ("how to", 1.0), ("how do i", 1.0), ("tutorial", 1.0), ("guide", 1.0),
                    ("best practice", 1.0), ("example", 1.0), ("configure", 1.0), 
                    ("setup", 1.0), ("set up", 1.0), ("export", 1.0), ("report", 1.0), 
                    ("dashboard customization", 1.0), ("workflow", 1.0)
                ],
                "negative": [("don't need help", 1.0), ("figured it out", 1.0)]
            },
            "Account and Access": {
                "positive": [
                    ("login", 1.0), ("password", 1.0), ("reset password", 1.0), 
                    ("forgot password", 1.0), ("mfa", 1.0), ("2fa", 1.0), ("sso", 1.0), 
                    ("single sign", 1.0), ("role", 1.0), ("permission", 1.0), 
                    ("access denied", 1.0), ("cannot access", 1.0), ("locked out", 1.0), 
                    ("account locked", 1.0), ("invite", 1.0), ("user management", 1.0), 
                    ("deactivate", 1.0), ("seat", 1.0), ("license", 1.0), ("org admin", 1.0), 
                    ("tenant", 1.0), ("unauthorized", 1.0)
                ],
                "negative": [("can access", 1.0), ("login working", 1.0), ("no access issues", 1.0)]
            },
            "General Feedback": {
                "positive": [
                    ("love", 1.0), ("great", 1.0), ("awesome", 1.0), ("terrible", 1.0), 
                    ("hate", 1.0), ("suggestion", 1.0), ("feedback", 1.0), ("improvement", 1.0), 
                    ("complain", 1.0), ("complaint", 1.0), ("praise", 1.0), ("recommend", 1.0), 
                    ("review", 1.0), ("ui cluttered", 1.0), ("pricing high", 1.0), 
                    ("expensive", 1.0), ("feature missing", 1.0), ("would like", 1.0), 
                    ("wish you had", 1.0)
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
        """Calculate rule-based scores for each topic with regex patterns and weights."""
        subject_norm = self.normalize_text(subject)
        body_norm = self.normalize_text(body)
        combined_text = f"{subject_norm} {body_norm}".strip()
        
        # Handle very short subjects by reducing weight
        subject_weight = 0.3 if len(subject_norm.split()) <= 2 else 0.4
        body_weight = 1.0 - subject_weight
        
        scores = {}
        
        # Check for technical issue hard cues first (for Product Usage exclusion logic)
        has_technical_hard_cue = False
        technical_patterns = [item for item in self.keywords["Technical Issue"]["positive"] if len(item) == 3 and item[2]]
        for pattern_info in technical_patterns:
            pattern = pattern_info[0]
            if re.search(pattern, combined_text, re.IGNORECASE):
                has_technical_hard_cue = True
                break
        
        for topic, word_lists in self.keywords.items():
            score = 0.0
            
            # Score positive keywords/patterns
            for item in word_lists["positive"]:
                if isinstance(item, tuple) and len(item) >= 2:
                    keyword_or_pattern = item[0]
                    weight = item[1]
                    is_regex = len(item) == 3 and item[2]
                else:
                    # Handle old format for backward compatibility
                    keyword_or_pattern = item
                    weight = 1.0
                    is_regex = False
                
                if is_regex:
                    # Use regex pattern matching
                    subject_matches = len(re.findall(keyword_or_pattern, subject_norm, re.IGNORECASE))
                    body_matches = len(re.findall(keyword_or_pattern, body_norm, re.IGNORECASE))
                else:
                    # Use word boundary matching
                    subject_matches = len(re.findall(r'\b' + re.escape(keyword_or_pattern) + r'\b', subject_norm))
                    body_matches = len(re.findall(r'\b' + re.escape(keyword_or_pattern) + r'\b', body_norm))
                
                # Apply negation detection (only for non-regex patterns)
                if not is_regex:
                    if subject_matches > 0 and self.detect_negation(subject_norm, keyword_or_pattern):
                        subject_matches = -subject_matches * 0.5
                    if body_matches > 0 and self.detect_negation(body_norm, keyword_or_pattern):
                        body_matches = -body_matches * 0.5
                
                # Apply subject multiplier for Technical Issue (1.4×)
                subject_multiplier = 1.4 if topic == "Technical Issue" else 1.0
                
                # Calculate weighted score
                weighted_score = (subject_matches * subject_weight * subject_multiplier + 
                                body_matches * body_weight) * weight
                score += weighted_score
            
            # Handle Product Usage special logic
            if topic == "Product Usage":
                # Check for generic help without how-to cues
                help_pattern = r'\bhelp( needed)?\b'
                has_generic_help = bool(re.search(help_pattern, combined_text, re.IGNORECASE))
                
                # Check for how-to cues within 5 tokens of help
                how_to_cues = ["how to", "how do i", "tutorial", "guide", "best practice", "example", 
                             "configure", "setup", "set up"]
                has_how_to_near_help = False
                
                if has_generic_help:
                    for cue in how_to_cues:
                        # Check if help and how-to cue are within 5 tokens of each other
                        pattern = r'\b(?:help(?:\s+needed)?(?:\s+\w+){0,4}\s+' + re.escape(cue) + r'|' + \
                                re.escape(cue) + r'(?:\s+\w+){0,4}\s+help(?:\s+needed)?)\b'
                        if re.search(pattern, combined_text, re.IGNORECASE):
                            has_how_to_near_help = True
                            break
                
                # Exclude generic help if it appears with technical cues and no how-to context
                if has_generic_help and has_technical_hard_cue and not has_how_to_near_help:
                    # Reduce Product Usage score for generic help in technical context
                    score *= 0.5
            
            # Subtract negative keywords
            for item in word_lists["negative"]:
                if isinstance(item, tuple):
                    keyword = item[0]
                    weight = item[1]
                else:
                    keyword = item
                    weight = 1.0
                
                subject_matches = len(re.findall(r'\b' + re.escape(keyword) + r'\b', subject_norm))
                body_matches = len(re.findall(r'\b' + re.escape(keyword) + r'\b', body_norm))
                score -= (subject_matches * subject_weight + body_matches * body_weight) * weight * 0.5
            
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
        # Adapt parameters based on dataset size
        min_df = min(2, max(1, len(texts) // 10))
        max_df = 0.9 if len(texts) > 10 else 1.0
        
        self.vectorizer = TfidfVectorizer(
            ngram_range=(1, 2),
            min_df=min_df,
            max_df=max_df,
            strip_accents='unicode'
        )
        
        X = self.vectorizer.fit_transform(texts)
        y = self.label_encoder.fit_transform(heuristic_labels)
        
        # Train calibrated classifier
        base_classifier = LinearSVC(class_weight='balanced', random_state=42)
        
        # Use fewer CV folds for small datasets
        cv_folds = min(3, len(set(heuristic_labels)))
        if cv_folds < 2:
            cv_folds = 2
            
        self.ml_classifier = CalibratedClassifierCV(
            base_classifier, 
            cv=cv_folds, 
            method='sigmoid'
        )
        
        try:
            self.ml_classifier.fit(X, y)
        except ValueError as e:
            # Fallback to simple classifier without calibration if CV fails
            logging.warning(f"Calibration failed, using simple classifier: {e}")
            self.ml_classifier = base_classifier
            self.ml_classifier.fit(X, y)
    
    def predict_probabilities(self, subject: str, body: str) -> Tuple[str, float, Dict[str, float]]:
        """Predict topic, confidence, and per-class probabilities."""
        # Get heuristic scores
        heuristic_probs = self.calculate_heuristic_scores(subject, body)
        
        # Get ML predictions if model is trained
        if self.ml_classifier and self.vectorizer:
            text = f"{str(subject)} {str(body)}".strip()
            X = self.vectorizer.transform([text])
            
            # Handle both calibrated and non-calibrated classifiers
            if hasattr(self.ml_classifier, 'predict_proba'):
                ml_probs_array = self.ml_classifier.predict_proba(X)[0]
            else:
                # For non-calibrated classifiers, use decision function
                decision_scores = self.ml_classifier.decision_function(X)[0]
                if np.isscalar(decision_scores):
                    # Binary classification case
                    ml_probs_array = np.array([1 / (1 + np.exp(-decision_scores)), 1 / (1 + np.exp(decision_scores))])
                else:
                    # Multi-class case - convert to probabilities using softmax
                    exp_scores = np.exp(decision_scores - np.max(decision_scores))
                    ml_probs_array = exp_scores / np.sum(exp_scores)
            
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
                
                if hasattr(self.ml_classifier, 'predict_proba'):
                    ml_probs_array = self.ml_classifier.predict_proba(X)[0]
                else:
                    decision_scores = self.ml_classifier.decision_function(X)[0]
                    if np.isscalar(decision_scores):
                        ml_probs_array = np.array([1 / (1 + np.exp(-decision_scores)), 1 / (1 + np.exp(decision_scores))])
                    else:
                        exp_scores = np.exp(decision_scores - np.max(decision_scores))
                        ml_probs_array = exp_scores / np.sum(exp_scores)
                
                max_ml_prob = max(ml_probs_array)
                sorted_probs = sorted(ml_probs_array, reverse=True)
                margin = sorted_probs[0] - sorted_probs[1] if len(sorted_probs) > 1 else max_ml_prob
                
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
    parser.add_argument('--profile', default='default', help='dbt profile name')
    parser.add_argument('--target', default='default', help='dbt target name')
    parser.add_argument('--output_table', default='support_tickets', help='Output table name')
    parser.add_argument('--output_dataset', help='Override output dataset (uses profile dataset by default)')
    parser.add_argument('--dry_run', action='store_true', help='Run classification but do not write to BigQuery')
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    if args.self_test:
        success = run_self_test()
        sys.exit(0 if success else 1)
    
    if not BIGQUERY_AVAILABLE and not args.dry_run:
        logging.error("BigQuery libraries not available. Install with: pip install google-cloud-bigquery")
        logging.info("Or run with --dry_run flag to test classification without BigQuery")
        sys.exit(1)
    
    try:
        # Initialize BigQuery manager
        bq_manager = None
        if not args.dry_run:
            bq_manager = BigQueryManager(args.profile, args.target)
        
        # Read data from BigQuery or use sample data for dry run
        if args.dry_run:
            logging.info("DRY RUN: Using sample data instead of BigQuery")
            df = pd.DataFrame({
                'ticket_id': ['T001', 'T002', 'T003'],
                'subject': ['Payment issue', 'API error', 'How to export data'],
                'body': ['Cannot process my credit card payment', 'Getting 500 error from API', 'Need help exporting CSV'],
                'account_id': ['A001', 'A002', 'A003'],
                'submitted_at': ['2024-01-01', '2024-01-02', '2024-01-03'],
                'priority': ['High', 'Critical', 'Medium'],
                'satisfaction_score': [3, 1, 5]
            })
        else:
            df = bq_manager.read_staging_tickets()
        
        if len(df) == 0:
            logging.warning("No tickets found to classify")
            return
        
        # Initialize and run classifier
        logging.info("Initializing ticket classifier...")
        classifier = TicketClassifier()
        results = classifier.classify_tickets(df)
        
        # Write results to BigQuery or display for dry run
        if args.dry_run:
            logging.info("DRY RUN: Classification results:")
            print("\nSample results:")
            print(results[['ticket_id', 'subject', 'predicted_topic', 'confidence']].head(10))
            print(f"\nFull results shape: {results.shape}")
            print(f"Columns: {list(results.columns)}")
        else:
            # Create output table and write results
            table_id = bq_manager.create_output_table(args.output_table, args.output_dataset)
            bq_manager.write_results(results, args.output_table, args.output_dataset)
            logging.info(f"Successfully processed and wrote {len(results)} classified tickets to {table_id}")
        
    except Exception as e:
        logging.error(f"Error processing tickets: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
