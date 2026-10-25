@echo off
cd /d "%~dp0"
python scripts\run_demo.py %*
exit /b %errorlevel%
