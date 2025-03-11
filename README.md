# pvl-dwh
Data warehouse for personal projects built using modern data stack

## Purpose
Set up a ELT process to support personal dashboards such as film reviews

## Tools and Technologies
- Amazon AWS, S3, Redshift
- Apache Airflow
- Docker
- dbt core
- SQL & Python

## Process
- Airflow as orchestration tool: trigger data pipeline upon loading data into the S3 bucket
- dbt transforms raw data into tables in Redshift
- Looker Studio connects to Redshift to display current data
