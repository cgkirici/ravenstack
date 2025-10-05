# RavenStack Analytics #

A comprehensive dbt project that transforms raw RavenStack CRM data into validated, modular models for revenue analysis, customer churn prediction, conversion tracking, and feature retention insights.

## 🎯 Data Pipeline Overview

This pipeline processes SaaS business data to enable:
- **Revenue Analytics**: MRR tracking, growth metrics, and segmentation analysis
- **Customer Success**: Churn analysis, support ticket correlation, and satisfaction scoring  
- **Product Analytics**: Feature usage patterns and retention correlation
- **Conversion Analysis**: Trial-to-paid and tier upgrade tracking

## 🏗️ Architecture

### Data Flow
```
Source Systems → Staging → Intermediate → Marts → BI Tools
     ↓              ↓           ↓          ↓         ↓
   CRM Data    → Cleaning → Aggregation → Analysis → Insights
```

### Layer Details

#### **Source Systems**
- **data dump from CRM Database**: Customer accounts, subscriptions, churn events, feature usage aggregations

  - **ML Enhancement**: `classify_tickets.py` - Hybrid ML classifier for automated ticket topic classification

#### **Data Warehouse**
- **Platform**: BigQuery (Google Cloud)
- **Pattern**: ELT (Extract, Load, Transform)
- **Storage**: Structured tables

#### **dbt Transformation Layers**

**🔄 Staging Layer** (`models/staging/`)
- Raw data cleaning and standardization
- Column renaming and type casting
- Data quality filtering (e.g., duplicate removal)
- Models: `stg__crm_accounts`, `stg__crm_subscriptions`, `stg__crm_support_tickets`, etc.

**⚙️ Intermediate Layer** (`models/marts/*/intermediate/`)
- Business logic aggregations
- Account-level and time-series rollups
- Cross-functional data joins
- Models: `int_support_metrics_by_account`, `int_mrr_normalised`, etc.

**📊 Marts Layer** (`models/marts/`)
- **Revenue**: `monthly_recurring_revenue`, `conversion_metrics`, `retention_metrics`
- **Customer Success**: `customer_churn_analysis`, `monthly_churn_metrics`
- Business-ready datasets for analytics and BI

#### **Semantic Layer**
- **MetricFlow Integration**: Standardized metric definitions
- **Self-Service Analytics**: Consistent business logic across tools
- **Example**: `retention_metrics` semantic model with popularity and stickiness scores

## 📋 Project Conventions

### Naming Standards
- **Staging**: `stg__[source]_[table]` (e.g., `stg__crm_accounts`)
- **Intermediate**: `int_[description]` (e.g., `int_support_metrics_by_account`)
- **Marts**: `[business_concept]` (e.g., `customer_churn_analysis`)

### Folder Structure
```
models/
├── staging/crm/          # Source system staging models
├── marts/
│   ├── revenue/          # Revenue & growth analytics
│   ├── customer_success/ # Churn & support analysis
│   └── intermediate/     # Reusable business logic
└── metricflow_time_spine.sql  # Time dimension for semantic layer
```

### Documentation Standards
- All models include business context descriptions
- Field-level documentation with calculation logic
- Consistent metric definitions across the project
- AI/LLM-optimized language for semantic understanding

## 📊 Key Models & Metrics

### Revenue Analytics
- **MRR Growth**: Month-over-month recurring revenue tracking
- **Cohort Analysis**: Customer acquisition and retention by segments
- **Conversion Rates**: Trial-to-paid and tier upgrade analysis

### Customer Success
- **Churn Prediction**: Risk scoring based on satisfaction and support patterns  
- **Support Correlation**: Ticket volume vs. churn relationship analysis
- **Satisfaction Tracking**: CSAT trends and low-satisfaction flagging
- **Ticket Classification**: Automated ML-powered topic classification (Billing, Technical, Product Usage, Account Access, General Feedback)

### Product Analytics  
- **Feature Stickiness**: Retention correlation by feature usage
- **Adoption Gaps**: Feature usage differences between retained vs. churned customers
- **Beta Performance**: Beta feature success metrics

## 🔍 Data Quality & Testing

- **dbt standard tests** ensuring data integrity
- **Uniqueness & referential integrity** validation
- **Business rule testing** (e.g., MRR growth logic)
- **Range validation** for key metrics

## 📈 Business Intelligence

**View Live Dashboard**: [RavenStack Analysis on Tableau Public](https://public.tableau.com/views/RavenStackAnalysis/RavenStack?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)

The Tableau dashboard provides interactive visualizations of:
- Revenue growth trends
- Customer churn analysis  
- Conversion funnel analysis for trial-to-paid and tier-upgrades
- Product usage analysis for retention

## 🚀 Getting Started

### Prerequisites
- dbt Core 1.8+ 
- BigQuery connection configured
- dbt_utils package installed

### Quick Start
```bash
# Install dependencies
dbt deps

# Run staging and marts models
dbt run

# Execute data quality tests
dbt test

# Generate and serve documentation
dbt docs generate && dbt docs serve
```

### Ticket Classification
The project includes an intelligent ticket classifier that enhances support data:

```bash
# Run ticket classification (requires Python dependencies)
python classify_tickets.py

# Test classifier with sample data
python classify_tickets.py --self_test

# Dry run without writing to BigQuery
python classify_tickets.py --dry_run
```

**Features:**
- **Hybrid Approach**: Combines rule-based heuristics with weakly-supervised ML
- **5 Topic Categories**: Billing & Payment, Technical Issues, Product Usage, Account & Access, General Feedback
- **Auto-Enhancement**: Reads from `stg__crm_support_tickets`, adds topic classifications with confidence scores
- **BigQuery Integration**: Seamlessly integrates with dbt pipeline using profiles.yml configuration

---

*Built with ❤️ using dbt for scalable, reliable analytics engineering*