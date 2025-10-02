# Contributing to ravenstack

Thank you for your interest in contributing to the ravenstack dbt project!

## Getting Started

### Prerequisites

1. Python 3.7 or higher
2. pip (Python package installer)
3. Access to a data warehouse (PostgreSQL, Snowflake, BigQuery, Redshift, etc.)

### Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/cgkirici/ravenstack.git
   cd ravenstack
   ```

2. **Install dbt**
   ```bash
   pip install dbt-core dbt-postgres  # or your database adapter
   ```

3. **Configure your profile**
   - Copy `profiles.yml.example` to `~/.dbt/profiles.yml`
   - Update with your database credentials
   - Never commit credentials to the repository!

4. **Test your setup**
   ```bash
   dbt debug
   ```

## Development Workflow

### Creating New Models

1. Create a new SQL file in the `models/` directory
2. Use the `{{ ref() }}` function to reference other models
3. Add documentation in a `schema.yml` file
4. Add tests to ensure data quality

Example model:
```sql
-- models/my_model.sql
{{ config(materialized='table') }}

select
    id,
    name,
    created_at
from {{ ref('source_table') }}
where created_at >= current_date - interval '30 days'
```

### Testing

Always test your models before committing:

```bash
# Run models
dbt run

# Run tests
dbt test

# Run specific model
dbt run --select my_model

# Run models and their downstream dependencies
dbt run --select my_model+
```

### Documentation

Document your models in `schema.yml` files:

```yaml
version: 2

models:
  - name: my_model
    description: "Description of what this model does"
    columns:
      - name: id
        description: "Unique identifier"
        data_tests:
          - unique
          - not_null
```

### Code Style

- Use lowercase for SQL keywords and function names
- Use snake_case for table and column names
- Indent with 4 spaces
- Add comments for complex logic
- Use CTEs (Common Table Expressions) for readability

Example:
```sql
with source_data as (
    select *
    from {{ ref('raw_data') }}
),

transformed_data as (
    select
        id,
        lower(name) as name,
        created_at
    from source_data
    where is_active = true
)

select * from transformed_data
```

## Project Structure

- `models/` - SQL transformation models
  - Organize by business domain (e.g., `models/finance/`, `models/marketing/`)
- `tests/` - Custom data tests
- `macros/` - Reusable SQL functions
- `seeds/` - Static CSV data
- `snapshots/` - Type-2 slowly changing dimensions
- `analyses/` - Ad-hoc analytical queries

## Submitting Changes

1. Create a new branch for your changes
2. Make your changes and test thoroughly
3. Update documentation as needed
4. Commit with clear, descriptive messages
5. Push your branch and create a pull request

## Best Practices

1. **Keep models focused**: Each model should do one thing well
2. **Test your data**: Add tests for uniqueness, not null, relationships
3. **Document everything**: Future you will thank current you
4. **Use version control**: Commit often, with meaningful messages
5. **Review the DAG**: Use `dbt docs generate` to visualize dependencies
6. **Performance matters**: Be mindful of query performance and cost

## Getting Help

- Check the [dbt documentation](https://docs.getdbt.com/)
- Join the [dbt Slack community](https://community.getdbt.com/)
- Review existing models for examples
- Ask questions in pull request reviews

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.
