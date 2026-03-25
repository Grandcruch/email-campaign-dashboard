@echo off
REM ─── Weekly CSV Export ─────────────────────────────────────────────────────
REM Scheduled to run every Sunday.
REM Generates: output\Historical Email Offer Performance(2026.3.9~).csv
REM ────────────────────────────────────────────────────────────────────────────

cd /d "%~dp0"
python export_historical_csv.py >> output\export_log.txt 2>&1
