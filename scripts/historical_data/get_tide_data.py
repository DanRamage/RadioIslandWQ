import sys

sys.path.append("../../commonfiles/python")

import configparser
import logging.config
import optparse
import os
from datetime import datetime

from build_tide_file import create_tide_data_file_mp
from pytz import timezone
from wq_sites import wq_sample_sites

logger = logging.getLogger()


def build_tide_data_file(tide_station, tide_output_file, unique_dates, log_conf_file):
    eastern_tz = timezone("US/Eastern")
    tide_dates = []
    for date_rec in unique_dates:
        tide_date = eastern_tz.localize(date_rec)
        tide_date = tide_date.replace(hour=0, minute=0, second=0)
        tide_dates.append(tide_date)

    create_tide_data_file_mp(
        tide_station, tide_dates, tide_output_file, 4, log_conf_file, True
    )


def main():
    parser = optparse.OptionParser()
    parser.add_option("--ConfigFile", dest="config_file", help="")
    parser.add_option("--EnteroDataDirectory", dest="entero_data_dir", help="")

    (options, args) = parser.parse_args()

    config_file = configparser.RawConfigParser()
    config_file.read(options.config_file)

    stations = config_file.get("tide_stations", "stations").split(",")
    data_directory = config_file.get("tide_stations", "data_directory")
    tide_log_file = config_file.get("logging", "tide_log_file")

    wq_sites = wq_sample_sites()
    wq_sites.load_sites(
        file_name=config_file.get("boundaries_settings", "sample_sites"),
        boundary_file="",
    )
    for site in wq_sites:
        dates = []
        # Open the entero data file, then create a list of all the sample dates.
        entero_data_file = os.path.join(
            options.entero_data_dir, "{site}.csv".format(site=site.name)
        )
        with open(entero_data_file, "r") as entero_data_file:
            for row_ndx, row in enumerate(entero_data_file):
                columns = row.split(",")
                if row_ndx > 0:
                    # 2010-01-25 16:40:00
                    date_time = datetime.strptime(columns[1], "%Y-%m-%d %H:%M:%S")
                    dates.append(date_time)
        # build_tide_data_file(tide_station, tide_output_file, unique_dates, log_conf_file):
        for station in stations:
            tide_data_file = os.path.join(
                data_directory, "{station}.csv".format(station=station)
            )
            build_tide_data_file(station, tide_data_file, dates, tide_log_file)

    return


if __name__ == "__main__":
    main()
