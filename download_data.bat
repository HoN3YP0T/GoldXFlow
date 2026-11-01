@echo off
REM ============================================================================================================
REM  GoldXFlow - download the Binance XAUUSDT history into THIS project's data\ folder. Nothing else.
REM
REM  - Saves raw archives in data\raw\binance\ (checksum-verified) and the catalog in data\state\acquisition.sqlite.
REM  - Period: the frozen range in data\state\range.json (2026-04-30 -> 2026-10-30 as shipped).
REM  - Safe to run again:
REM      * download already complete  -> says so and does nothing (no network traffic);
REM      * download interrupted        -> resumes; files already downloaded and verified are never downloaded again.
REM  - Run it on your PC in India (Binance REST answers HTTP 451 to US hosts).
REM ============================================================================================================
setlocal
cd /d "%~dp0"

python -m pip install -q -r requirements.txt || goto :fail

python -m goldflow.cli data-status >nul 2>&1
if not errorlevel 1 (
    echo.
    echo The history is already downloaded and verified. Nothing to do.
    python -m goldflow.cli data-status
    exit /b 0
)

echo.
echo Downloading the history into %CD%\data\ ...
python -m goldflow.cli discover
if errorlevel 1 echo (instrument specification not refreshed: REST unreachable; the archive download continues)

python -m goldflow.cli acquire
if errorlevel 1 goto :incomplete

python -m goldflow.cli data-status
if errorlevel 1 goto :incomplete
echo.
echo Download complete. Next: run_windows.bat (it uses this data and never downloads again).
exit /b 0

:incomplete
echo.
echo The download is not complete yet (network problem or a failed file). Run download_data.bat again:
echo it resumes and skips every file that is already downloaded and verified.
exit /b 1

:fail
echo Installing the requirements failed (exit code %errorlevel%). Check Python 3.10+ and your internet connection.
exit /b 1
