{{ config(materialized='table') }}

with date_spine as (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="cast('2023-01-01' as date)",
      end_date="current_date()"
  ) }}
)

select 
  date_day,
  extract(year from date_day) as date_year,
  extract(month from date_day) as date_month,
  extract(day from date_day) as date_day_of_month,
  extract(quarter from date_day) as date_quarter,
  extract(dayofweek from date_day) as date_day_of_week,
  extract(week from date_day) as date_week,
  date_trunc(date_day, month) as date_month_start,
  date_trunc(date_day, quarter) as date_quarter_start,
  date_trunc(date_day, year) as date_year_start
from date_spine
