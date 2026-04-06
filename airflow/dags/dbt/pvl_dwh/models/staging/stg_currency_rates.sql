with source as (
    select
        date,
        currency,
        rate_to_eur
    from {{ source('personal_finance', 'currency_rates') }}
),

/*
    ECB only publishes rates on business days; CBR also skips Russian public
    holidays. Forward-fill using LAST_VALUE IGNORE NULLS so that every
    calendar day carries the most recent known rate. This ensures weekend and
    holiday transactions always resolve to a non-null rate.

    Strategy:
      1. Generate every calendar day in the full date range per currency.
      2. Left-join the raw rates onto that spine.
      3. Forward-fill rate_to_eur over nulls (gaps = weekends / holidays).
*/

date_spine as (
    select
        calendar.date,
        currencies.currency
    from {{ ref('calendar') }} as calendar
    cross join (
        select distinct currency
        from source
    ) as currencies
),

with_gaps as (
    select
        date_spine.date,
        date_spine.currency,
        source.rate_to_eur
    from date_spine
    left join source
        on source.date     = date_spine.date
        and source.currency = date_spine.currency
),

forward_filled as (
    select
        date,
        currency,
        last_value(rate_to_eur ignore nulls) over (
            partition by currency
            order by date
            rows between unbounded preceding and current row
        ) as rate_to_eur
    from with_gaps
)

select *
from forward_filled
where rate_to_eur is not null
