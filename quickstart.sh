#!/bin/bash

python setup/getsymbols.py
python main/pull_data.py
python main/historical_aggregation.py 