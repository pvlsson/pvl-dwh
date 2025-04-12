# pvl-dwh
Data warehouse for personal projects built using modern data stack

## Purpose
Set up a ELT process to support personal dashboards such as film reviews

## Tools and Technologies
- GCS
- BigQuery
- Airbyte
- Apache Airflow
- Docker
- dbt core
- SQL & Python

## Process
1. Raw data is uploaded to GCS
2. Airflow pipeline is triggered, performing following tasks:
    1. Trigger Airbyte sync, which reads the file from GCS and loads raw data into BigQuery staging layer
    2. Apply dbt transformations and write results into mart tables in BigQuery
3. Looker Studio connects to BigQuery to display a dashboard

## Limitations
In this setup, Airflow runs locally to simplify iteration and debugging. In a production setting, the Airflow instance would typically be deployed in the cloud, e.g. on Kubernetes using Cloud Composer. The ideal design would involve a sensor that triggers the Airflow instance automatically once a new file appears in GCS.
