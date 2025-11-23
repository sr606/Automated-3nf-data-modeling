@echo off
REM ============================================================================
REM Automated 3NF Data Modeling System - Quick Start Script
REM ============================================================================

echo.
echo ========================================================================
echo   AUTOMATED 3NF DATA MODELING SYSTEM
echo ========================================================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in PATH
    echo Please install Python 3.10 or higher from https://www.python.org/
    pause
    exit /b 1
)

echo [INFO] Python detected:
python --version
echo.

REM Check if requirements are installed
echo [INFO] Checking dependencies...
python -c "import pandas" >nul 2>&1
if errorlevel 1 (
    echo [WARNING] Dependencies not installed. Installing now...
    echo.
    pip install -r requirements.txt
    if errorlevel 1 (
        echo [ERROR] Failed to install dependencies
        pause
        exit /b 1
    )
    echo.
    echo [SUCCESS] Dependencies installed successfully
    echo.
) else (
    echo [INFO] Dependencies already installed
    echo.
)

REM Check if input files exist
if not exist "input_files\*.csv" if not exist "input_files\*.json" (
    echo [WARNING] No CSV or JSON files found in input_files folder
    echo Please add your data files to: input_files\
    echo.
    echo Sample data files are already included. You can test with those.
    echo.
    set /p continue="Do you want to continue with existing files? (Y/N): "
    if /i not "%continue%"=="Y" (
        echo.
        echo Exiting...
        pause
        exit /b 0
    )
)

echo.
echo ========================================================================
echo   RUNNING NORMALIZATION WORKFLOW
echo ========================================================================
echo.

REM Run the main script
python main.py

if errorlevel 1 (
    echo.
    echo [ERROR] Workflow failed. Check the error messages above.
    pause
    exit /b 1
)

echo.
echo ========================================================================
echo   WORKFLOW COMPLETED
echo ========================================================================
echo.
echo Check the following folders for outputs:
echo   - normalized_output\  (Normalized CSV/JSON files)
echo   - sql_output\         (SQL DDL scripts)
echo   - erd\                (ERD diagrams)
echo.
echo To view the SQL script:
echo   notepad sql_output\normalized_schema.sql
echo.
echo To view the ERD:
echo   start erd\normalized_erd.png
echo.

pause
