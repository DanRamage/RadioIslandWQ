#!/bin/bash

source /usr/local/virtualenv/pyenv-3.8.5/bin/activate
cd /home/xeniaprod/scripts/RadioIslandWQ/scripts/historical_data

python get_observation_data.py --ConfigFile=/home/xeniaprod/scripts/RadioIslandWQ/config/radioisland_historical_build_config.ini --EnteroDataDirectory=/home/xeniaprod/scripts/RadioIslandWQ/data/historical/sampling