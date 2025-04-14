FROM quay.io/astronomer/astro-runtime:12.7.1

# install dbt into a venv to avoid package dependency conflicts
RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate