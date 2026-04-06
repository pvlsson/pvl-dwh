select
    prep.* except (date),
    calendar.*
from {{ ref('prep_financial_transactions') }} as prep
left join {{ ref('calendar') }} as calendar
    on date(prep.transaction_datetime) = calendar.date
