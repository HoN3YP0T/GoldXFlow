@echo off
REM ============================================================================================================
REM  GoldXFlow - process the downloaded history: validation, all analytics, signals, reports.
REM
REM  - NEVER downloads anything. It uses the data already in data\ (download it first with download_data.bat).
REM  - NEVER changes the downloaded data: raw archives and the frozen range are only read.
REM  - Safe to run again: steps that are already done and still current are skipped
REM      normalize  -> only days not yet normalised
REM      validate   -> skipped if the last validation already covers the current data, code and config
REM      pipeline   -> skipped if the last run used exactly the same data, config and code
REM
REM    run_windows.bat          the steps above
REM    run_windows.bat live     ... then live paper mode (Ctrl-C to stop; needs Binance REST, i.e. not a US host)
REM ============================================================================================================
setlocal
cd /d "%~dp0"

python -m pip install -q -r requirements.txt || goto :fail

python -m goldflow.cli data-status >nul 2>&1
if errorlevel 1 (
    echo.
    echo The history is not downloaded yet, or the download is incomplete:
    python -m goldflow.cli data-status
    echo.
    echo Run download_data.bat first, then run this file again.
    exit /b 1
)

REM ---- Phase 3: validated Parquet partitions (only new days) and an independent re-check (only if something changed)
python -m goldflow.cli normalize || goto :fail
python -m goldflow.cli validate --if-changed || goto :fail

REM ---- Phases 5-11: all analytics + signals in one replay, then every independent verifier (skipped if up to date)
python -m goldflow.cli pipeline --name main --if-changed || goto :fail

REM ---- Reports and status
python -m goldflow.cli providers-report --no-probe
python -m goldflow.cli ops-check

REM ---- Phases 12-13 need your broker data (decision D2): fill the "broker" block in the config, then
REM   python -m goldflow.cli broker-import --file XAUUSD_ticks.csv --format mt5
REM   python -m goldflow.cli pipeline --name bt1 --backtest
REM   python -m goldflow.cli perf-report --name bt1

if /I "%1"=="live" python -m goldflow.cli live-run --name live1
echo.
echo Done. Results: data\reports\pipeline_main.json   Dashboard: python -m goldflow.cli dashboard  (http://127.0.0.1:8765/)
exit /b 0

:fail
echo A step failed (exit code %errorlevel%). Fix the reported problem and run this file again: finished steps are skipped.
exit /b 1
