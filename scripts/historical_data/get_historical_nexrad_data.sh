#!/bin/bash

source /usr/local/virtualenv/pyenv-3.8.5/bin/activate
cd /home/xeniaprod/scripts/RadioIslandWQ/scripts/historical_data

python /home/xeniaprod/scripts/RadioIslandWQ/commonfiles/python/wqXMRGProcessing.py --ConfigFile=/home/xeniaprod/scripts/RadioIslandWQ/config/radioisland_historical_build_config.ini --ImportData=/mnt/xmrg/2010/Jan,/mnt/xmrg/2010/Feb,/mnt/xmrg/2010/Mar,/mnt/xmrg/2010/Apr,/mnt/xmrg/2010/May,/mnt/xmrg/2010/Jun,/mnt/xmrg/2010/Jul,/mnt/xmrg/2010/Aug,/mnt/xmrg/2010/Sep,/mnt/xmrg/2010/Oct,/mnt/xmrg/2010/Nov,/mnt/xmrg/2010/Dec