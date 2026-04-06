"""
fetch_currency_rates.py

Fetches daily EUR exchange rates and loads the result into BigQuery as a
full replace. Two data sources are used:

  1. ECB SDMX 2.1 REST API — for USD, HUF, BGN, TRY (all dates) and RUB
     (up to ~2022-03-01, when ECB stopped publishing EUR/RUB).

  2. CBR (Central Bank of Russia) XML API — for RUB from 2022-03-02 onwards.
     CBR publishes RUB rates against all major currencies daily. We fetch
     EUR/RUB and invert to get rate_to_eur for RUB.

     ECB data takes priority where both sources overlap for RUB.

Usage:
    python fetch_currency_rates.py [--start-date YYYY-MM-DD] [--project GCP_PROJECT]

Defaults:
    --start-date  2000-01-01
    --project     pvlsson

Target BigQuery table:
    <project>.pvl_dwh_raw.currency_rates

Schema:
    date         DATE     — calendar date of the exchange rate
    currency     STRING   — ISO 4217 currency code (e.g. USD)
    rate_to_eur  FLOAT64  — units of EUR per 1 unit of the foreign currency
                            (i.e. to convert an amount in currency X to EUR:
                             amount_eur = amount * rate_to_eur)

Re-running the script performs a full replace of the table — safe and
idempotent to run at any time.

Requirements:
    # Create and activate the venv (one-time setup, run from this directory):
    python3 -m venv .venv
    .venv/bin/pip install -r requirements.txt

    # Run the script using the venv's Python directly:
    .venv/bin/python fetch_currency_rates.py

    bq CLI on PATH (installed via google-cloud-sdk / Homebrew)
    gcloud auth login  (or Application Default Credentials)
"""

from __future__ import annotations

import argparse
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from datetime import date, timedelta

import pandas as pd
import requests

# ── Configuration ─────────────────────────────────────────────────────────────

# Currencies fetched entirely from ECB
ECB_CURRENCIES = ["USD", "HUF", "BGN", "TRY"]

# RUB: fetched from ECB where available, then supplemented from CBR
RUB_ECB_END = "2022-03-01"       # last reliable ECB RUB date
CBR_EUR_CODE = "R01239"          # CBR internal code for EUR
CBR_CHUNK_DAYS = 364             # CBR API works best with < 1 year chunks

TARGET_DATASET = "pvl_dwh_raw"
TARGET_TABLE = "currency_rates"

ECB_API_BASE = "https://data-api.ecb.europa.eu/service/data/EXR"
CBR_DYNAMIC_URL = "https://www.cbr.ru/scripts/XML_dynamic.asp"

# ── ECB fetch ─────────────────────────────────────────────────────────────────

def fetch_ecb(currencies: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch daily rates from ECB for the given currencies.
    ECB returns units of foreign currency per 1 EUR.
    We invert: rate_to_eur = 1 / obs_value.
    """
    if not currencies:
        return pd.DataFrame(columns=["date", "currency", "rate_to_eur"])

    key = "+".join(currencies)
    url = (
        f"{ECB_API_BASE}/D.{key}.EUR.SP00.A"
        f"?format=csvdata&startPeriod={start_date}&endPeriod={end_date}"
    )
    print(f"[ECB] Fetching {', '.join(currencies)} from {start_date} to {end_date}")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    df = pd.read_csv(io.StringIO(resp.text))
    df = df[["CURRENCY", "TIME_PERIOD", "OBS_VALUE"]].copy()
    df.columns = ["currency", "date", "obs_value"]
    df = df.dropna(subset=["obs_value"])
    df["obs_value"] = pd.to_numeric(df["obs_value"], errors="coerce")
    df = df.dropna(subset=["obs_value"])
    df["rate_to_eur"] = 1.0 / df["obs_value"]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[["date", "currency", "rate_to_eur"]]

    print(f"[ECB] Fetched {len(df):,} rows")
    return df


# ── CBR fetch ─────────────────────────────────────────────────────────────────

def _fetch_cbr_chunk(start: date, end: date) -> pd.DataFrame:
    """
    Fetch one date-range chunk of EUR/RUB from CBR.
    CBR returns: <ValCurs> <Record Date="DD.MM.YYYY"> <Nominal>N</Nominal> <Value>R</Value>
    Rate means: N EUR = R RUB  →  rate_to_eur (RUB→EUR) = nominal / value
    """
    params = {
        "date_req1": start.strftime("%d/%m/%Y"),
        "date_req2": end.strftime("%d/%m/%Y"),
        "VAL_NM_RQ": CBR_EUR_CODE,
    }
    resp = requests.get(CBR_DYNAMIC_URL, params=params, timeout=60)
    resp.raise_for_status()

    # CBR returns windows-1251 encoded XML
    content = resp.content.decode("windows-1251")
    root = ET.fromstring(content)

    rows = []
    for record in root.findall("Record"):
        raw_date = record.get("Date")           # DD.MM.YYYY
        nominal_el = record.find("Nominal")
        value_el = record.find("Value")
        if raw_date and nominal_el is not None and value_el is not None:
            try:
                d = date(int(raw_date[6:]), int(raw_date[3:5]), int(raw_date[:2]))
                nominal = float(nominal_el.text.replace(",", "."))
                value = float(value_el.text.replace(",", "."))
                # nominal EUR = value RUB → 1 RUB = nominal/value EUR
                rate_to_eur = nominal / value
                rows.append({"date": d, "currency": "RUB", "rate_to_eur": rate_to_eur})
            except (ValueError, ZeroDivisionError):
                continue

    return pd.DataFrame(rows, columns=["date", "currency", "rate_to_eur"])


def fetch_cbr_rub(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch EUR/RUB from CBR for the full date range, chunked by year
    to stay within API limits.
    """
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    if start > end:
        return pd.DataFrame(columns=["date", "currency", "rate_to_eur"])

    print(f"[CBR] Fetching RUB from {start_date} to {end_date} (chunked)")
    chunks = []
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + timedelta(days=CBR_CHUNK_DAYS), end)
        chunk = _fetch_cbr_chunk(chunk_start, chunk_end)
        chunks.append(chunk)
        print(f"  chunk {chunk_start} → {chunk_end}: {len(chunk):,} rows")
        chunk_start = chunk_end + timedelta(days=1)
        time.sleep(0.3)  # be polite to CBR

    if not chunks:
        return pd.DataFrame(columns=["date", "currency", "rate_to_eur"])

    df = pd.concat(chunks, ignore_index=True)
    print(f"[CBR] Total RUB rows: {len(df):,}")
    return df


# ── Combine RUB from both sources ─────────────────────────────────────────────

def fetch_rub(start_date: str) -> pd.DataFrame:
    """
    Fetch RUB rates:
    - ECB for [start_date .. RUB_ECB_END]
    - CBR for [RUB_ECB_END+1 .. today]
    ECB takes priority where both overlap.
    """
    today = date.today().isoformat()

    ecb_rub = fetch_ecb(["RUB"], start_date, RUB_ECB_END)
    cbr_start = (date.fromisoformat(RUB_ECB_END) + timedelta(days=1)).isoformat()
    cbr_rub = fetch_cbr_rub(cbr_start, today)

    combined = pd.concat([ecb_rub, cbr_rub], ignore_index=True)

    # Deduplicate: ECB takes priority (it appears first, keep first occurrence)
    combined = combined.drop_duplicates(subset=["date", "currency"], keep="first")
    combined = combined.sort_values("date").reset_index(drop=True)

    print(f"[RUB] Combined: {len(combined):,} rows total")
    return combined


# ── BigQuery load ─────────────────────────────────────────────────────────────

def load_to_bigquery(df: pd.DataFrame, project: str) -> None:
    """
    Write the DataFrame to a temporary NDJSON file and load into BigQuery
    using bq CLI with --replace (full table replace, idempotent).

    The schema is written to a separate temp file because passing it as an
    inline JSON string causes bq to reject DATE-typed fields.
    """
    table_ref = f"{project}:{TARGET_DATASET}.{TARGET_TABLE}"
    schema = [
        {"name": "date",        "type": "DATE"},
        {"name": "currency",    "type": "STRING"},
        {"name": "rate_to_eur", "type": "FLOAT64"},
    ]

    # Write schema to temp file
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as schema_tmp:
        json.dump(schema, schema_tmp)
        schema_path = schema_tmp.name

    # Write data to temp NDJSON file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        for _, row in df.iterrows():
            record = {
                "date": row["date"].isoformat(),
                "currency": row["currency"],
                "rate_to_eur": float(row["rate_to_eur"]),
            }
            tmp.write(json.dumps(record) + "\n")
        tmp_path = tmp.name

    print(f"\nLoading {len(df):,} rows into {table_ref} ...")
    try:
        result = subprocess.run(
            [
                "bq", "load",
                "--project_id", project,
                "--source_format=NEWLINE_DELIMITED_JSON",
                "--replace",
                "--schema", schema_path,
                table_ref,
                tmp_path,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        print(result.stdout or "Load complete.")
    except subprocess.CalledProcessError as exc:
        print("bq load failed:", file=sys.stderr)
        print(exc.stdout, file=sys.stderr)
        print(exc.stderr, file=sys.stderr)
        raise
    finally:
        os.unlink(tmp_path)
        os.unlink(schema_path)

    print(f"Successfully loaded {len(df):,} rows into {table_ref}.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch daily EUR exchange rates from ECB (USD, HUF, BGN, TRY, RUB pre-2022) "
            "and CBR (RUB from 2022 onwards), then load into BigQuery."
        )
    )
    parser.add_argument(
        "--start-date",
        default="2000-01-01",
        help="Earliest date to fetch (YYYY-MM-DD). Default: 2000-01-01",
    )
    parser.add_argument(
        "--project",
        default="pvlsson",
        help="GCP project ID. Default: pvlsson",
    )
    args = parser.parse_args()

    today = date.today().isoformat()

    # Fetch ECB currencies (non-RUB)
    ecb_df = fetch_ecb(ECB_CURRENCIES, args.start_date, today)

    # Fetch RUB (ECB up to 2022-03-01, CBR from 2022-03-02 onwards)
    rub_df = fetch_rub(args.start_date)

    # Combine
    all_df = pd.concat([ecb_df, rub_df], ignore_index=True)
    all_df = all_df.sort_values(["currency", "date"]).reset_index(drop=True)

    print(f"\nTotal rows to load: {len(all_df):,}")
    for ccy, count in all_df.groupby("currency").size().items():
        print(f"  {ccy}: {count:,} rows")

    load_to_bigquery(all_df, args.project)


if __name__ == "__main__":
    main()
