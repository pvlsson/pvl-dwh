{% docs transaction_id %}
The unique key for each transaction based on sort order, renewed upon each table refresh
{% enddocs %}

{% docs transaction_datetime %}
Date and time on which transaction was registered
{% enddocs %}

{% docs transaction_type %}
The type of the transaction (e.g. Expense, Income, Transfer, Balance)
{% enddocs %}

{% docs withdrawal_amount %}
The amount withdrawn in the transaction
{% enddocs %}

{% docs withdrawal_currency %}
The currency in which the withdrawal was made
{% enddocs %}

{% docs withdrawal_account %}
The account from which the amount was withdrawn
{% enddocs %}

{% docs deposit_amount %}
The amount deposited in the transaction
{% enddocs %}

{% docs deposit_currency %}
The currency in which the deposit was made
{% enddocs %}

{% docs deposit_account %}
The account to which the deposit was made
{% enddocs %}

{% docs tags %}
Any labels or categories associated with the transaction
{% enddocs %}

{% docs note %}
A free-text note providing additional details about the transaction
{% enddocs %}

{% docs party %}
The external party or counterparty involved in the transaction
{% enddocs %}

{% docs transaction_group %}
A grouping identifier used to associate the transaction with a batch or related set
{% enddocs %}