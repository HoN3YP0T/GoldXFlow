@echo off
REM GoldXFlow end to end on the Windows trading PC (fapi.binance.com is reachable from India; HTTP 451 from US cloud IPs).
REM Each step is resumable: re-running this file continues where it stopped.
REM   run_windows.bat            history + all analytics + signals + reports
REM   run_windows.bat live       ... then live ingestion (paper mode, Ctrl-C to stop)
setlocal
python -m pip install -r requirements.txt || goto :fail

REM ---- Phase 2-3: data (6-month frozen range in data\state\range.json)
python -m goldflow.cli discover || goto :fail
python -m goldflow.cli acquire || goto :fail
python -m goldflow.cli normalize || goto :fail
python -m goldflow.cli validate || goto :fail

REM ---- Phases 5-11 in one replay + every independent verifier (resumable with --resume)
python -m goldflow.cli pipeline --name main || goto :fail

REM ---- Phase 14 parity check on the last archived day, Phase 15 venues, Phase 17 status
python -m goldflow.cli providers-report
python -m goldflow.cli ops-check

REM ---- Phase 12-13 need your broker data (decision D2): fill the "broker" block, then
REM   python -m goldflow.cli broker-import --file XAUUSD_ticks.csv --format mt5
REM   python -m goldflow.cli pipeline --name bt1 --backtest
REM   python -m goldflow.cli perf-report --name bt1

if /I "%1"=="live" python -m goldflow.cli live-run --name live1
echo Done. Dashboard: python -m goldflow.cli dashboard  (http://127.0.0.1:8765/)
exit /b 0
:fail
echo A step failed (exit code %errorlevel%). Fix the reported problem and re-run: completed steps are skipped or resumed.
exit /b 1
