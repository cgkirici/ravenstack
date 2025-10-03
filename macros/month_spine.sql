{#
    Generates a spine of month start dates from a given start date to an end date.
    
    This macro creates a table with one column containing the first day of each month
    in the range from the specified start date through the end date (defaults to current date).
    Useful for creating date dimension tables or filling gaps in time-series data.
    
    Args:
        start_date (string): The starting date in YYYY-MM-DD format. The spine will
                            begin with the month containing this date.
        end_date (string, optional): The ending date in YYYY-MM-DD format. The spine will
                                    end with the month containing this date. If not provided,
                                    defaults to current_date().
    
    Returns:
        A table with one column 'month_start' containing date values representing
        the first day of each month in the specified range.
        
    Examples:
        {{ generate_month_spine('2023-01-15') }}
        -- Returns: 2023-01-01, 2023-02-01, 2023-03-01, ... up to current month
        
        {{ generate_month_spine('2023-01-15', '2023-06-30') }}
        -- Returns: 2023-01-01, 2023-02-01, 2023-03-01, 2023-04-01, 2023-05-01, 2023-06-01
#}

{% macro generate_month_spine(start_date, end_date=none) %}
    with month_spine as (
        select
            date_trunc(month_date, month) as month_start
        from
            unnest(
                generate_date_array(
                    date('{{ start_date }}'),
                    {% if end_date %}date('{{ end_date }}'){% else %}current_date(){% endif %},
                    interval 1 month
                )
            ) as month_date
    )
    select * from month_spine

{% endmacro %}
