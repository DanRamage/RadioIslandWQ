import os
import sys

sys.path.append("../../commonfiles/python")
import configparser
import logging.config
import time
import traceback
from datetime import datetime
from multiprocessing import Process

import data_collector_plugin as my_plugin
import requests
from nc_bacteria_data import parse_excel_data
from wq_output_results import (wq_advisories_file, wq_samples_collection,
                               wq_station_advisories_file)
from wq_sites import wq_sample_sites
from yapsy.IPlugin import IPlugin


class sample_data_collector_plugin(my_plugin.data_collector_plugin):
    def initialize_plugin(self, **kwargs):
        try:
            Process.__init__(self)
            IPlugin.__init__(self)

            logger = logging.getLogger(self.__class__.__name__)
            self.plugin_details = kwargs["details"]
            self.ini_file = self.plugin_details.get("Settings", "ini_file")
            self.log_config = self.plugin_details.get("Settings", "log_config")

            self._data_url = self.plugin_details.get("source_data", "url")
            self._download_dir = self.plugin_details.get(
                "source_data", "download_directory"
            )
            self._advisory_file_directory = self.plugin_details.get(
                "output", "advisory_directory"
            )
            return True
        except Exception as e:
            logger.exception(e)
        return False

    def download_sample_data(self, destination_dir, start_year, end_year, logger):
        start_time = time.time()
        try:
            data_filename = os.path.join(
                destination_dir, f"{start_year}-{end_year}_sampledata.xls"
            )
            with open(data_filename, "wb") as output_file:
                full_url = self._data_url.format(
                    start_year=start_year, end_year=end_year
                )
                req = requests.get(full_url)
                if req.status_code == 200:
                    for chunk in req.iter_content(chunk_size=1024):
                        output_file.write(chunk)
                    logger.info(
                        f"Saved sample data in {time.time() - start_time} seconds"
                    )

                else:
                    logger.info(
                        f"ERROR: Unable to download data. Status Code: {req.status_code}"
                    )
        except (IOError, Exception) as e:
            traceback.print_exc()
        return data_filename

    def run(self):
        logger = None
        try:
            start_time = time.time()
            logging.config.fileConfig(self.log_config)
            logger = logging.getLogger()
            logger.debug("sample_data_collector_plugin run started.")

            config_file = configparser.RawConfigParser()
            config_file.read(self.ini_file)

            download_start_year = datetime.now().year
            data_file = self.download_sample_data(
                self._download_dir, download_start_year, download_start_year + 1, logger
            )
            # Now process the file.
            wq_sites = wq_sample_sites()
            wq_sites.load_sites(
                file_name=config_file.get("boundaries_settings", "sample_sites"),
                boundary_file="",
            )

            sample_files = os.listdir(self._download_dir)
            sample_files.reverse()
            wq_collection = wq_samples_collection()
            logger.info(f"Processing file: {data_file}")
            parse_excel_data(data_file, wq_sites, wq_collection)

            json_results_file = os.path.join(
                self._advisory_file_directory, "radioisland_beach_advisories.json"
            )
            logger.debug("Creating beach advisories file: {json_results_file}")
            try:
                current_advisories = wq_advisories_file(wq_sites)
                current_advisories.create_file(json_results_file, wq_collection)
            except Exception as e:
                logger.exception(e)

            for site in wq_sites:
                try:
                    enetero_filename = os.path.join(
                        self._advisory_file_directory,
                        "{site}.csv".format(site=site.name),
                    )
                    logger.debug(
                        f"Creating site: {site.name} advisories file: {enetero_filename}"
                    )
                    with open(enetero_filename, "w") as entero_file_obj:
                        entero_file_obj.write(
                            "Station,DateTime,Entero SSM,Entero GM,Entero SSM_CFU,Entero GM2\n"
                        )
                        station_samples = wq_collection[site.name]
                        station_samples_date_sorted = sorted(
                            station_samples, key=lambda x: x.date_time, reverse=False
                        )
                        for sample_results in station_samples_date_sorted:
                            entero_file_obj.write(
                                "{station},{date_time},{ssm},{gm},{ssm_cfu},{gm2}\n".format(
                                    station=sample_results.station,
                                    date_time=sample_results.date_time.strftime(
                                        "%Y-%m-%d %H:%M:%S"
                                    ),
                                    ssm=sample_results.entero_ssm,
                                    gm=sample_results.entero_gm,
                                    ssm_cfu=sample_results.entero_ssm_cfu,
                                    gm2=sample_results.entero_gm2,
                                )
                            )

                    logger.debug("Creating site: %s advisories file" % (site.name))
                    site_advisories = wq_station_advisories_file(site)
                    site_advisories.create_file(
                        self._advisory_file_directory, wq_collection
                    )

                except Exception as e:
                    logger.exception(e)

        except Exception as e:
            logger.exception(e)
        logger.debug(
            f"sample_data_collector_plugin run finished in {time.time() - start_time} seconds."
        )

        # mp_logging.shutdown_logging()
        return

    def finalize(self):
        return
