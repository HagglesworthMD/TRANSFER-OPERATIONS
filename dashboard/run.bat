@echo off
cd /d "%~dp0"
echo Starting Transfer-Bot Dashboard...
echo Open http://localhost:8050 in your browser
echo.
python -m backend.server
pause
