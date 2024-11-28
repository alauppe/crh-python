#!/bin/bash

# Create and activate virtual environment
echo "Creating virtual environment..."
python3 -m venv venv-mm

# Activate virtual environment
source venv-mm/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo "Installing requirements..."
pip install -r requirements.txt

# Create necessary directories
echo "Creating directories..."
mkdir -p data logs state

echo "Setup complete! Activate the virtual environment with:"
echo "source venv/bin/activate" 
