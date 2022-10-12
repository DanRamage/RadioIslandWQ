import os, sys
import traceback

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import optparse
import xlrd
from datetime import datetime
from pytz import timezone
from nc_bacteria_data import parse_excel_data
from wq_sites import wq_sample_sites
from wq_output_results import wq_samples_collection,wq_station_advisories_file

import logging.config
import configparser

logger = logging.getLogger()



def main():
    parser = optparse.OptionParser()
    parser.add_option("--SampleFilesDir", dest="sample_files_dir",
                      help="Destination directory to save the sampling data.")
    parser.add_option("--AdvisoriesFilesDir", dest="advisory_files_dir",
                      help="Destination directory to save the sites advisory file.")
    parser.add_option("--ConfigFile", dest="config_file",
                      help="Destination directory to save the sampling data.")

    (options, args) = parser.parse_args()

    config_file = configparser.RawConfigParser()
    config_file.read(options.config_file)

    log_config_file = config_file.get("logging", "config_file")
    logging.config.fileConfig(log_config_file)
    logger = logging.getLogger()

    logger.info("Log file opened.")

    wq_sites = wq_sample_sites()
    wq_sites.load_sites(file_name=config_file.get("boundaries_settings", "sample_sites"), boundary_file="")

    sample_files = os.listdir(options.sample_files_dir)
    sample_files.reverse()
    wq_collection = wq_samples_collection()
    for file in sample_files:
        logger.info("Processing file: {file}".format(file=file))
        full_file_path = os.path.join(options.sample_files_dir, file)
        parse_excel_data(full_file_path, wq_sites, wq_collection)
    try:
      for site in wq_sites:
        enetero_filename = os.path.join(options.advisory_files_dir, "{site}.csv".format(site=site.name))
        logger.debug("Creating site: {site_name} advisories file: {file}".format(site_name=site.name, file=enetero_filename))
        with open(enetero_filename, "w") as entero_file_obj:
            entero_file_obj.write("Station,DateTime,Entero SSM,Entero GM,Entero SSM_CFU,Entero GM2\n")
            station_samples = wq_collection[site.description]
            station_samples_date_sorted = sorted(station_samples, key=lambda x: x.date_time, reverse=False)
            for sample_results in station_samples_date_sorted:
                entero_file_obj.write("{station},{date_time},{ssm},{gm},{ssm_cfu},{gm2}\n".format(
                    station=sample_results.station,
                    date_time=sample_results.date_time.strftime("%Y-%m-%d %H:%M:%S"),
                    ssm=sample_results.entero_ssm,
                    gm=sample_results.entero_gm,
                    ssm_cfu=sample_results.entero_ssm_cfu,
                    gm2=sample_results.entero_gm2
                ))
    except Exception as e:
      logger.exception(e)

    return

if __name__ == "__main__":
    main()