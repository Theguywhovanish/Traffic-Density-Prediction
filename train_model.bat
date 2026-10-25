@echo off
cd /d "%~dp0"
python scripts\train_model.py %*
exit /b %errorlevel%
