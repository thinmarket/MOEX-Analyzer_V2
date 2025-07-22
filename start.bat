@echo off
chcp 65001 > nul
set "PYTHONIOENCODING=UTF-8"
echo Starting automated analysis...
python "ANALIZ_final/run_analysis.py"
echo.
echo Analysis complete. Press any key to exit.
pause > nul
