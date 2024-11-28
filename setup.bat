@echo off

REM Create and activate virtual environment
echo Creating virtual environment...
python -m venv venv

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip

REM Install requirements
echo Installing requirements...
pip install -r requirements.txt

REM Create necessary directories
echo Creating directories...
mkdir data 2>nul
mkdir logs 2>nul
mkdir state 2>nul

echo Setup complete! Activate the virtual environment with:
echo venv\Scripts\activate.bat

pause 