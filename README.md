# pvl-dwh
Data warehouse for personal projects built using modern data stack

## Purpose
Set up a ELT process to support personal dashboards.

### Film Ratings Report
I like to watch movies and rate them with a 0-10 score on IMDB.com. I would like to export these ratings once in a while, combine them with my historical ratings on a different website (KP), and display the results inside a Looker Studio dashboard. Ideally, I would like to stream IMDB data into the dashboard in realtime, but IMDB doesn't provide an API to achieve this. I can export all my ratings from IMDB as a .csv file.

This is a continuation and improvement of my earlier project (https://github.com/pvlsson/film-ratings). Historical ratings from KP are exported via web scrapting.

## Tools and Technologies
- GCS
- BigQuery
- Airbyte
- Apache Airflow (Python) deployed locally via Astronomer's Cosmos
- Docker
- dbt core
- SQL

## Process
1. First setup:
    - Raw data is uploaded manually to GCS. Airbyte sync is manually triggered, which reads the file from GCS and loads raw data into BigQuery staging layer.
    - Raw data is loaded in static batches at irregular intervals.
2. Adding new data:
    - New CSV tables are added directly in BigQuery via file upload.
3. Airflow pipeline is triggered, applying dbt transformations, cleaning and writing views and tables into BigQuery mart layer
4. Looker Studio connects to BigQuery to display a dashboard:
    - Film Ratings Report: https://lookerstudio.google.com/reporting/2b455111-1ec1-4664-bdf8-74ebeea181b7

## Limitations
Airflow runs locally to simplify iteration and debugging. In a production setting, the Airflow instance would typically be deployed in the cloud, e.g. on Kubernetes using Cloud Composer. The ideal design would involve a sensor that triggers the Airflow instance automatically once a new file appears in GCS.

To automate the process, Airbyte should also be deployed in the cloud.

## Next steps
- [ ] Attempt web scraping new IMDB ratings (my ratings page is public)
- [ ] Add SQLfluff formatting with lowercase SQL formatting as a pre-commit check
- [ ] Add a configuration file for an extra Docker container taht will hold dbt project (possibly instead of Python venv), use Dev Environments
- [ ] Add freshness checks
- [ ] Decide how to handle duplicate movie reviews (in case same movie is reviewed in IMDB and KP)
- [ ] Add additional source with metadate about each movie