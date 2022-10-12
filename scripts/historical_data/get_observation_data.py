import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append("../../commonfiles/python")
import logging.config

from datetime import datetime, timedelta
import optparse
import configparser
import json
from noaa_coops import Station
from wq_sites import wq_sample_sites

from nc_bacteria_data import get_entero_dates
from build_historical_db import build_database

def main():
    parser = optparse.OptionParser()
    parser.add_option("--ConfigFile", dest="config_file",
                      help="")
    parser.add_option("--EnteroDataDirectory", dest="entero_data_dir",
                      help="")

    (options, args) = parser.parse_args()

    config_file = configparser.RawConfigParser()
    config_file.read(options.config_file)

    log_config_file = config_file.get("logging", "config_file")
    logging.config.fileConfig(log_config_file)
    logger = logging.getLogger()
    logger.info("Log file opened.")

    datasaver_config_file = config_file.get("logging", "platform_data")


    platform_config = config_file.get("platform_configuration", "setup_file")
    tide_data_directory = config_file.get("tide_stations", "data_directory")


    wq_sites = wq_sample_sites()
    wq_sites.load_sites(file_name=config_file.get("boundaries_settings", "sample_sites"), boundary_file="")

    entero_data_dir = config_file.get("entero_limits", "data_directory")
    historic_db_file = config_file.get("historical_database", "name")
    with open(platform_config, "r") as platform_json_file:
        platforms_config_json = json.load(platform_json_file)


    for site in wq_sites:
        platform_config = platforms_config_json[site.name]
        platform_data = build_database(historic_db_file, platform_config, datasaver_config_file)
        entero_data_file = os.path.join(entero_data_dir, "{site}.csv".format(site=site.name))
        entero_dates = get_entero_dates(entero_data_file)
        platform_data.initialize()
        for entero_date in entero_dates:
            logger.info("Site: {sitename} Date: {date}".format(sitename=site.name, date=entero_date))
            platform_data.query_data(entero_date)
        platform_data.finalize()

    return

if __name__ == "__main__":
     main()