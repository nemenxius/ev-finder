@echo off
setlocal

cd /d "%~dp0"

if not exist logs mkdir logs

py -3 value_bet_alerts.py >> logs\value_bet_alerts.log 2>&1

endlocal
