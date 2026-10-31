@echo off
REM Phase 2 acquisition on Windows (fapi.binance.com is reachable from India; it returns HTTP 451 from US cloud IPs)
python -m pip install -r requirements.txt
python -m goldflow.cli discover
python -m goldflow.cli acquire --months 6
python -m goldflow.cli manifest
