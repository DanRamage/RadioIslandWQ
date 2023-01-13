#!/bin/bash

source /usr/local/virtualenv/pyenv-3.8.5/bin/activate

cd /home/xeniaprod/scripts/RadioIslandWQ/scripts;

python radioisland_wq_prediction_engine.py --ConfigFile=/home/xeniaprod/scripts/RadioIslandWQ/config/folly_prediction_engine.ini >> /home/xeniaprod/tmp/log/radioisland_wq_prediction_engine_sh.log 2>&1