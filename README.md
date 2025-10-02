# ravenstack

A dbt (data build tool) project for managing data transformations.

## About this project

This is a dbt project initialized with the standard dbt structure. It provides a framework for transforming data in your analytics warehouse using SQL and Jinja templating.

## Project Structure

```
ravenstack/
├── analyses/          # SQL queries for ad-hoc analysis
├── dbt_project.yml    # Project configuration file
├── macros/            # Reusable SQL macros
├── models/            # SQL models (transformations)
│   └── example/       # Example models to get started
├── seeds/             # CSV files for static data
├── snapshots/         # Type-2 slowly changing dimension snapshots
└── tests/             # Custom data tests
```

## Getting Started

### Prerequisites

- Python 3.7+
- dbt-core
- dbt database adapter (e.g., dbt-postgres, dbt-snowflake, dbt-bigquery)

### Installation

1. Install dbt:
```bash
pip install dbt-core dbt-postgres  # or your preferred adapter
```

2. Set up your `profiles.yml` file in `~/.dbt/`:
```yaml
ravenstack:
  target: dev
  outputs:
    dev:
      type: postgres  # or your database type
      host: localhost
      user: your_username
      password: your_password
      port: 5432
      dbname: your_database
      schema: your_schema
      threads: 4
```

### Running the Project

Try running the following commands:

```bash
# Install dependencies (if any)
dbt deps

# Run all models
dbt run

# Test your models
dbt test

# Generate documentation
dbt docs generate

# View documentation
dbt docs serve
```

### Example Models

This project includes example models in the `models/example/` directory:
- `my_first_dbt_model.sql` - A simple model that creates sample data
- `my_second_dbt_model.sql` - A model that references the first model

These examples demonstrate:
- Basic SQL transformations
- Model references using `{{ ref() }}`
- Model configuration
- Testing with schema.yml

## Resources

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
