import json
import sys

sys.path.append("../commonfiles/python")

import configparser
import logging
import logging.config
import multiprocessing
import optparse
import time
from datetime import datetime
from multiprocessing import Queue

from pytz import timezone
from yapsy.PluginManager import PluginManager

multiprocessing.set_start_method("fork")
from cat_boost_model import cbm_model_classifier
from data_collector_plugin import data_collector_plugin
from model_ensemble import model_ensemble
from radioisland_data import RadioIslandData
from wq_prediction_engine import data_result_types, wq_prediction_engine
from wq_sites import wq_sample_sites


class radioisland_prediction_engine(wq_prediction_engine):
    def __init__(self):
        self.logger = logging.getLogger()

    def collect_data(self, **kwargs):
        self.logger.info("Begin collect_data")
        try:
            simplePluginManager = PluginManager()

            yapsy_log = logging.getLogger("yapsy")
            yapsy_log.setLevel(logging.DEBUG)
            yapsy_log.disabled = False

            simplePluginManager.setCategoriesFilter(
                {"DataCollector": data_collector_plugin}
            )

            # Tell it the default place(s) where to find plugins
            self.logger.debug(
                "Plugin directories: %s" % (kwargs["data_collector_plugin_directories"])
            )
            simplePluginManager.setPluginPlaces(
                kwargs["data_collector_plugin_directories"]
            )

            simplePluginManager.collectPlugins()

            output_queue = Queue()
            plugin_cnt = 0
            plugin_start_time = time.time()
            for plugin in simplePluginManager.getAllPlugins():
                if plugin.details.getboolean("Settings", "Enabled"):
                    self.logger.info("Starting plugin: %s" % (plugin.name))
                    if plugin.plugin_object.initialize_plugin(
                            details=plugin.details, queue=output_queue
                    ):
                        plugin.plugin_object.start()
                    else:
                        self.logger.error(
                            "Failed to initialize plugin: %s" % (plugin.name)
                        )
                    plugin_cnt += 1
                else:
                    self.logger.info("Plugin: %s not enabled." % (plugin.name))

            # Wait for the plugings to finish up.
            self.logger.info("Waiting for %d plugins to complete." % (plugin_cnt))
            for plugin in simplePluginManager.getAllPlugins():
                if plugin.details.getboolean("Settings", "Enabled"):
                    plugin.plugin_object.join()
                    plugin.plugin_object.finalize()
            while not output_queue.empty():
                results = output_queue.get()
                if results[0] == data_result_types.MODEL_DATA_TYPE:
                    self.site_data = results[1]

            self.logger.info(
                "%d Plugins completed in %f seconds"
                % (plugin_cnt, time.time() - plugin_start_time)
            )
        except Exception as e:
            self.logger.exception(e)

    def output_results(self, **kwargs):

        self.logger.info("Begin output_results")

        simplePluginManager = PluginManager()
        # logging.getLogger("yapsy").setLevel(logging.DEBUG)
        simplePluginManager.setCategoriesFilter(
            {"OutputResults": data_collector_plugin}
        )

        # Tell it the default place(s) where to find plugins
        self.logger.debug(
            "Plugin directories: %s" % (kwargs["output_plugin_directories"])
        )
        simplePluginManager.setPluginPlaces(kwargs["output_plugin_directories"])
        yapsy_logger = logging.getLogger("yapsy")
        yapsy_logger.setLevel(logging.DEBUG)
        # yapsy_logger.parent.level = logging.DEBUG
        yapsy_logger.disabled = True

        simplePluginManager.collectPlugins()

        plugin_cnt = 0
        plugin_start_time = time.time()
        for plugin in simplePluginManager.getAllPlugins():
            try:
                if plugin.details.getboolean("Settings", "Enabled"):
                    self.logger.info("Starting plugin: %s" % (plugin.name))
                    if plugin.plugin_object.initialize_plugin(
                            details=plugin.details,
                            prediction_date=kwargs["prediction_date"]
                                    .astimezone(timezone("US/Eastern"))
                                    .strftime("%Y-%m-%d %H:%M:%S"),
                            execution_date=kwargs["prediction_run_date"].strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
                            ensemble_tests=kwargs["site_model_ensemble"],
                            nowcast_site=kwargs["nowcast_site"],
                    ):
                        plugin.plugin_object.start()
                        plugin_cnt += 1
                    else:
                        self.logger.error(
                            "Failed to initialize plugin: %s" % (plugin.details)
                        )
                else:
                    self.logger.info("Plugin: %s not enabled." % (plugin.name))

            except Exception as e:
                self.logger.exception(e)
        # Wait for the plugings to finish up.
        self.logger.info("Waiting for %d plugins to complete." % (plugin_cnt))
        for plugin in simplePluginManager.getAllPlugins():
            if plugin.details.getboolean("Settings", "Enabled"):
                plugin.plugin_object.join()
                plugin.plugin_object.finalize()

        self.logger.debug(
            "%d output plugins run in %f seconds"
            % (plugin_cnt, time.time() - plugin_start_time)
        )
        self.logger.info("Finished output_results")

    def run_wq_models(self, **kwargs):

        prediction_testrun_date = datetime.now()
        try:
            config_file = configparser.RawConfigParser()
            config_file.read(kwargs["config_file_name"])

            data_collector_plugin_directories = config_file.get(
                "data_collector_plugins", "plugin_directories"
            )
            enable_data_collector_plugins = config_file.getboolean(
                "data_collector_plugins", "enable_plugins"
            )
            if enable_data_collector_plugins and len(data_collector_plugin_directories):
                data_collector_plugin_directories = (
                    data_collector_plugin_directories.split(",")
                )
                self.collect_data(
                    data_collector_plugin_directories=data_collector_plugin_directories
                )

            nowcast_site_name = config_file.get("settings", "nowcast_site_name")
            boundaries_location_file = config_file.get(
                "boundaries_settings", "boundaries_file"
            )
            sites_location_file = config_file.get("boundaries_settings", "sample_sites")
            units_file = config_file.get("units_conversion", "config_file")
            output_plugin_dirs = config_file.get(
                "output_plugins", "plugin_directories"
            ).split(",")
            enable_output_plugins = config_file.getboolean(
                "output_plugins", "enable_plugins"
            )

            xenia_nexrad_db_file = config_file.get("database", "name")

            # MOve xenia obs db settings into standalone ini. We can then
            # check the main ini file into source control without exposing login info.
            db_settings_ini = config_file.get(
                "password_protected_configs", "settings_ini"
            )
            xenia_obs_db_config_file = configparser.RawConfigParser()
            xenia_obs_db_config_file.read(db_settings_ini)

            xenia_obs_db_host = xenia_obs_db_config_file.get(
                "xenia_observation_database", "host"
            )
            xenia_obs_db_user = xenia_obs_db_config_file.get(
                "xenia_observation_database", "user"
            )
            xenia_obs_db_password = xenia_obs_db_config_file.get(
                "xenia_observation_database", "password"
            )
            xenia_obs_db_name = xenia_obs_db_config_file.get(
                "xenia_observation_database", "database"
            )

        except (configparser.Error, Exception) as e:
            self.logger.exception(e)

        else:
            # Load the sample site information. Has name, location and the boundaries that contain the site.
            wq_sites = wq_sample_sites()
            wq_sites.load_sites(
                file_name=sites_location_file, boundary_file=boundaries_location_file
            )

            # First pass we want to get all the data, after that we only need to query
            # the site specific pieces.
            reset_site_specific_data_only = False
            total_time = 0
            site_model_ensemble = []
            for site in wq_sites:
                try:
                    # Get all the models used for the particular sample site.
                    model_list = self.build_test_objects(
                        config_file=config_file, site_name=site.name
                    )
                    if len(model_list) == 0:
                        self.logger.error("No models found for site: %s" % (site.name))

                except (configparser.Error, Exception) as e:
                    self.logger.exception(e)
                else:
                    try:
                        if len(model_list):
                            tide_station = config_file.get(site.name, "tide_station")

                            obs_platform_config = config_file.get(
                                site.name, "platform_configuration"
                            )
                            with open(obs_platform_config, "r") as platform_json_file:
                                platforms_config_json = json.load(platform_json_file)

                            wq_data = RadioIslandData()
                            if wq_data.initialize(
                                    site.name,
                                    platforms_config_json,
                                    tide_station,
                                    xenia_nexrad_db_file,
                                    "",
                                    "postgresql",
                                    xenia_obs_db_user,
                                    xenia_obs_db_password,
                                    xenia_obs_db_host,
                                    xenia_obs_db_name
                            ):
                                wq_data.query_data(
                                    kwargs["begin_date"], kwargs["begin_date"]
                                )
                                pd_df = wq_data.get_data_frame()
                                model_list.runTestsDF(pd_df)
                                total_test_time = sum(
                                    model.test_time for model in model_list.models
                                )
                                self.logger.debug(
                                    "Site: %s total time to execute models: %f ms"
                                    % (site.name, total_test_time * 1000)
                                )

                                model_list.overall_prediction()
                                site_model_ensemble.append(
                                    {
                                        "metadata": site,
                                        "models": model_list,
                                        "statistics": None,
                                        "entero_value": None,
                                        "model_data": pd_df,
                                        "platform_configuration": platforms_config_json
                                    }
                                )
                            else:
                                self.logger.error("Unable to initialize the data object, cannot process models.")
                    except Exception as e:
                        self.logger.exception(e)

            self.logger.debug(
                f"Total time to execute all sites models: {total_time * 1000} ms"
            )

            try:
                if enable_output_plugins:
                    self.output_results(
                        output_plugin_directories=output_plugin_dirs,
                        site_model_ensemble=site_model_ensemble,
                        prediction_date=kwargs["begin_date"],
                        prediction_run_date=prediction_testrun_date,
                        nowcast_site=nowcast_site_name,
                    )
            except Exception as e:
                self.logger.exception(e)

        return

    def build_test_objects(self, site_name, config_file):
        model_list = None
        try:
            model_config = config_file.get(site_name, "prediction_config")
            entero_lo_limit = config_file.getint("entero_limits", "limit_lo")
            entero_hi_limit = config_file.getint("entero_limits", "limit_hi")
        except configparser.Error as e:
            self.logger.exception(e)
        else:
            self.logger.debug(f"Site: {site_name} Model Config File: {model_config}")
            model_config_file = configparser.RawConfigParser()
            model_config_file.read(model_config)

            site_name = model_config_file.get("settings", "site")
            machine_learning_model_count = model_config_file.getint(
                "settings", "machine_learning_model_count"
            )
            if machine_learning_model_count:
                machine_learning_model_count = model_config_file.getint(
                    "settings", "machine_learning_model_count"
                )
                self.logger.debug(
                    f"Site: {site_name} ML Model count: {machine_learning_model_count}"
                )
            model_list = []
            for cnt in range(machine_learning_model_count):
                try:
                    # Get the name of the model object this model uses.
                    model_object_name = model_config_file.get(
                        f"machine_learning_model_{(cnt + 1)}", "model_object"
                    )
                    if model_object_name == "cbm_model_classifier":
                        # Get the category of the model this represents.
                        type = model_config_file.get(
                            f"machine_learning_model_{(cnt + 1)}", "type"
                        )
                        # This is the name we use on the reports to identify the model.
                        model_name = model_config_file.get(
                            f"machine_learning_model_{(cnt + 1)}", "name"
                        )
                        # This is the filename of the model we user for the nowcast.
                        model_filename = model_config_file.get(
                            f"machine_learning_model_{(cnt + 1)}", "model_file"
                        )
                        false_positive_threshold = model_config_file.get(
                            f"machine_learning_model_{(cnt + 1)}",
                            "false_positive_threshold",
                            fallback=None,
                        )
                        false_negative_threshold = model_config_file.get(
                            f"machine_learning_model_{(cnt + 1)}",
                            "false_negative_threshold",
                            fallback=None,
                        )
                        model_object = cbm_model_classifier(
                            site_name=site_name,
                            model_name=model_name,
                            model_type=type,
                            model_file=model_filename,
                            false_positive_threshold=false_positive_threshold,
                            false_negative_threshold=false_negative_threshold,
                            model_data_list=None,
                        )
                        self.logger.debug(
                            f"Site: {site_name} Model name: {model_name} model file: {model_filename}"
                        )
                        model_list.append(model_object)

                except Exception as e:
                    self.logger.exception(e)

            site_ensemble = model_ensemble(site_name, model_list)

        return site_ensemble


def main():
    parser = optparse.OptionParser()
    parser.add_option(
        "-c", "--ConfigFile", dest="config_file", help="INI Configuration file."
    )
    parser.add_option(
        "-s",
        "--StartDateTime",
        dest="start_date_time",
        help="A date to re-run the predictions for, if not provided, the default is the current day. Format is YYYY-MM-DD HH:MM:SS.",
    )

    (options, args) = parser.parse_args()

    if options.config_file is None:
        parser.print_help()
        sys.exit(-1)

    try:
        config_file = configparser.RawConfigParser()
        config_file.read(options.config_file)

        logConfFile = config_file.get("logging", "prediction_engine")
        logging.config.fileConfig(logConfFile)
        logger = logging.getLogger(__name__)
        logger.info("Log file opened.")

    except (configparser.Error, Exception) as e:
        import traceback

        traceback.print_exc(e)
        sys.exit(-1)
    else:
        dates_to_process = []
        if options.start_date_time is not None:
            # Can be multiple dates, so let's split on ','
            collection_date_list = options.start_date_time.split(",")
            # We are going to process the previous day, so we get the current date, set the time to midnight, then convert
            # to UTC.
            eastern = timezone("US/Eastern")
            try:
                for collection_date in collection_date_list:
                    est = eastern.localize(
                        datetime.strptime(collection_date, "%Y-%m-%dT%H:%M:%S")
                    )
                    # Convert to UTC
                    begin_date = est.astimezone(timezone("UTC"))
                    dates_to_process.append(begin_date)
            except Exception as e:
                if logger:
                    logger.exception(e)
        else:
            # We are going to process the previous day, so we get the current date, set the time to midnight, then convert
            # to UTC.
            est = datetime.now(timezone("US/Eastern"))
            est = est.replace(hour=0, minute=0, second=0, microsecond=0)
            # Convert to UTC
            begin_date = est.astimezone(timezone("UTC"))
            dates_to_process.append(begin_date)

        try:
            for process_date in dates_to_process:
                prediction_engine = radioisland_prediction_engine()
                prediction_engine.run_wq_models(
                    begin_date=process_date, config_file_name=options.config_file
                )
                # run_wq_models(begin_date=process_date,
                #              config_file_name=options.config_file)
        except Exception as e:
            logger.exception(e)

    if logger:
        logger.info("Log file closed.")

    return


if __name__ == "__main__":
    main()
