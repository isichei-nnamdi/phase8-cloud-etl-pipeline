#!/usr/bin/env bash
# ====================================================================
# Runs the ETL pipeline once and appends the output to etl_log.txt.
# cron calls this file on macOS / Linux (see README.md, Part 6, Option A).
# Settings come from the .env file at the repository root.
# ====================================================================

# Go to the repository root (two folders up from this file)
cd "$(dirname "$0")/../.." || exit 1

echo "===== $(date) =====" >> etl_log.txt
.venv/bin/python src/etl_to_bigquery.py >> etl_log.txt 2>&1
echo "exit code $?" >> etl_log.txt
