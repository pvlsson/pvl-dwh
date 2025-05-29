select
    date_day as date,
    day_of_week_iso,
    day_of_week_name_short,
    iso_week_of_year as week_iso,
    month_of_year as month,
    month_name_short,
    year_number as year,
    format_date('%Y-%m', date_day) as year_month,
    format_date('%G-%V', date_day) as year_week_iso
from {{ ref('stg_calendar') }}
