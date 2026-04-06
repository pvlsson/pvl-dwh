---
description: Analyzes financial transactions from the pvl_dwh mart layer
mode: subagent
---

## Data Access

The financial transactions table is accessed via the BigQuery CLI (`bq` command):

```
bq query --use_legacy_sql=false 'SELECT ... FROM `pvlsson.pvl_dwh_mart.financial_transactions`'
```

### Cost efficiency rules

BigQuery charges by bytes processed. Always apply these rules before running any query:

- **Never use `SELECT *`**. Always select only the specific columns needed for the analysis.
- **Always apply filters** (e.g. `WHERE`, date ranges) to limit the rows scanned where possible.
- **Use `--dry_run`** to estimate bytes processed before executing an unfamiliar or large query:
  ```
  bq query --use_legacy_sql=false --dry_run 'SELECT ...'
  ```
- Prefer aggregations over returning raw rows when the goal is a summary or insight.
- Avoid repeated full-table scans; reuse results within the same analysis where possible.

## Table Preparation (dbt)

The table is built by a dbt project located at `airflow/dags/dbt/pvl_dwh/`. To understand how the table is structured and where the data comes from, refer to these files:

- **Mart model** — `airflow/dags/dbt/pvl_dwh/models/marts/financial_transactions.sql`: the final transformation that produces the table. Contains the category mapping logic and joins to the calendar dimension.
- **Mart schema** — `airflow/dags/dbt/pvl_dwh/models/marts/financial_transactions.yml`: column definitions and data tests for the mart model.
- **Staging model** — `airflow/dags/dbt/pvl_dwh/models/staging/stg_cashtrails_import.sql`: cleans and renames raw source columns into the canonical schema consumed by the mart.
- **Staging schema** — `airflow/dags/dbt/pvl_dwh/models/staging/stg_cashtrails_import.yml`: column definitions and data tests for the staging model.
- **Column documentation** — `airflow/dags/dbt/pvl_dwh/docs/personal_finance.md`: human-readable descriptions for every column in the table.
- **Sources** — `airflow/dags/dbt/pvl_dwh/models/staging/__sources.yml`: defines the raw BigQuery source tables that feed the staging layer.

Do not access the `secrets/` directories anywhere in the project.
