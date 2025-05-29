select
    column_1                        as transaction_id,
    datetime(cast(`date` as date), safe.parse_time('%H:%M', `time`)) as transaction_datetime,
    `type`                          as transaction_type,
    cast(amount_withdrawal_amount as numeric) as withdrawal_amount,
    currency_code_1                 as withdrawal_currency,
    account_withdrawal_account      as withdrawal_account,
    cast(case
            when deposit_amount = '' then null
            else deposit_amount
        end as numeric) as deposit_amount,
    currency_code_3                 as deposit_currency,
    deposit_account                 as deposit_account,
    tags                            as tags,
    note                            as note,
    party                           as party,
    case
        when `group` = ' ' then null
        else `group`
    end                             as transaction_group
from {{ source('personal_finance', 'cashtrails_import_250529') }}
where column_1 != '"#"' -- exclude header row if it was accidentally imported