#!/bin/sh

# Setup python
if ! [ -e .venv/bin/activate ]; then
    python3 -m venv .venv --copies
fi
. .venv/bin/activate
pip install -e .

# Download dataset
python download.py

# Import the two datasets
python parse_deces.py
python repartition_ages.py

# Do the computation
python esperance.py
