select
    column_1                        as transaction_id,
    datetime(cast(`date` as date), safe.parse_time('%H:%M', `time`)) as transaction_datetime,
    `type`                          as transaction_type,
    amount_withdrawal_amount        as withdrawal_amount,
    currency_code_1                 as withdrawal_currency,
    account_withdrawal_account      as withdrawal_account,
    deposit_amount                  as deposit_amount,
    currency_code_3                 as deposit_currency,
    deposit_account                 as deposit_account,
    tags                            as tags,
    note                            as note,
    party                           as party,
    `group`                         as transaction_group
from {{ source('personal_finance', 'cashtrails_import') }}
where column_1 != '"#"' -- exclude header row