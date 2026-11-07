@echo off
REM ====================================================================
REM Runs the ETL pipeline once and appends the output to etl_log.txt.
REM Windows Task Scheduler calls this file (see README.md, Part 6, Option A).
REM Settings come from the .env file at the repository root.
REM ====================================================================

REM Go to the repository root (two folders up from this file)
cd /d "%~dp0..\.."

echo ===== %date% %time% ===== >> etl_log.txt
".venv\Scripts\python.exe" src\etl_to_bigquery.py >> etl_log.txt 2>&1
echo exit code %errorlevel% >> etl_log.txt
