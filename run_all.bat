@echo off
cd /d "%~dp0"
python scripts\run_all.py %*
exit /b %errorlevel%
