import configparser
import csv
import logging.config
import optparse
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
from NOAATideData import noaaTideDataExt
from pytz import timezone
from sqlalchemy import or_

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append("../../commonfiles/python")

from PlatformMapping import ModelSitesPlatforms
from stats import calcAvgSpeedAndDirV2
from wq_sites import wq_sample_sites
from wqXMRGProcessing import wqDB
from xenia import qaqcTestFlags
from xeniaSQLAlchemy import xeniaAlchemy
from xeniaSQLiteAlchemy import multi_obs as sl_multi_obs
from xeniaSQLiteAlchemy import xeniaAlchemy as sl_xeniaAlchemy

logger = logging.getLogger()

meters_per_second_to_mph = 2.23694
mm_to_inches = 0.0393701


class RadioIslandData:
    def __init__(self):
        self._site = None
        self._db_obj = None
        self._nexrad_db = None
        self._df_column_names = []
        self._platforms_config_json = None
        self._tide_station = None
        self._data_frame = None
        self._platform_mappings = {}
        self._nexrad_mappings = {}
        self._row_id = 0
        return

    def get_data_frame(self):
        return self._data_frame

    def connect_observation_db(self, **db_config):
        ret_val = False
        if "sqlite_database_file" in db_config:
            db_file = db_config["sqlite_database_file"]
            self._db_obj = sl_xeniaAlchemy()
            if self._db_obj.connectDB("sqlite", None, None, db_file, None, False):
                logger.info("Succesfully connect to DB: %s" % (db_file))
                ret_val = True
            else:
                logger.error("Unable to connect to DB: %s" % (db_file))
        else:
            try:
                # Connect to the xenia database we use for observations aggregation.
                self.xenia_obs_db = xeniaAlchemy()
                if self.xenia_obs_db.connectDB(
                        db_config["xenia_obs_db_type"],
                        db_config["xenia_obs_db_user"],
                        db_config["xenia_obs_db_password"],
                        db_config["xenia_obs_db_host"],
                        db_config["xenia_obs_db_name"],
                        False,
                ):
                    logger.info(
                        f"Succesfully connect to DB: {db_config['xenia_obs_db_name']} at "
                        f"{db_config['xenia_obs_db_host']}"
                    )
                    ret_val = True
                else:
                    logger.error(
                        "Unable to connect to DB: {db_config['xenia_obs_db_name']} at "
                        f"{db_config['xenia_obs_db_host']}"
                    )

            except Exception as e:
                logger.exception(e)
                raise

        return ret_val

    def connect_nexrad_db(self, **db_config):
        ret_val = False
        if "nexrad_database_file" in db_config:
            db_file = db_config["nexrad_database_file"]
            try:
                self._nexrad_db = wqDB(db_file, __name__)
                ret_val = True
            except Exception as e:
                logger.exception(e)

        return ret_val

    def load_platform_configurations(
            self, platform_cfg, observation_db_config, nexrad_db_config
    ):
        # The observations and NEXRAD platform data are stored in different databases in production.
        nexrad_cfg = {}
        obs_cfg = {}
        for site in platform_cfg:
            for platform in platform_cfg[site]:
                org, name, type = platform.split(".")
                if type != "radarcoverage":
                    if site not in obs_cfg:
                        obs_cfg[site] = {}
                        obs_cfg[site][platform] = {}
                    obs_cfg[site][platform] = platform_cfg[site][platform]
                else:
                    if site not in nexrad_cfg:
                        nexrad_cfg[site] = {}
                        nexrad_cfg[site][platform] = {}
                    nexrad_cfg[site][platform] = platform_cfg[site][platform]

        self._platform_mappings = ModelSitesPlatforms()

        self._platform_mappings.from_json(
            obs_cfg,
            db_type="postgresql",
            db_user=observation_db_config["xenia_obs_db_user"],
            db_password=observation_db_config["xenia_obs_db_password"],
            db_host=observation_db_config["xenia_obs_db_host"],
            db_name=observation_db_config["xenia_obs_db_name"],
        )

        self._nexrad_mappings = ModelSitesPlatforms()
        self._nexrad_mappings.from_json(
            nexrad_cfg, sqlite_database_file=nexrad_db_config["nexrad_database_file"]
        )

    def initialize(
            self,
            site,
            platform_configuration,
            tide_station,
            observation_db_config,
            nexrad_db_config,
    ):
        self._site = site
        self._tide_station = tide_station
        self.load_platform_configurations(
            platform_configuration, observation_db_config, nexrad_db_config
        )
        try:
            if not self.connect_observation_db(**observation_db_config):
                logger.error("Unable to connect to observation database.")
                return False
        except Exception as e:
            logger.exception(e)
            return False
        try:
            if not self.connect_nexrad_db(**nexrad_db_config):
                logger.error("Unable to connect to NEXRAD database.")
                return False
        except Exception as e:
            logger.exception(e)
            return False

        self._platforms_config = platform_configuration
        if len(self._df_column_names) <= 0:
            self._df_column_names.append("entero_date")
            # self._df_column_names.append('entero_date_utc')
            self._df_column_names.append("enterococcus_value")
            self._df_column_names.append("{}_tide_range".format(self._tide_station))
            self._df_column_names.append("{}_tide_hi".format(self._tide_station))
            self._df_column_names.append("{}_tide_lo".format(self._tide_station))
            self._df_column_names.append("{}_tide_stage".format(self._tide_station))
            #
            model_site_platforms = self._platform_mappings.get_site(self._site)
            for platform_handle in model_site_platforms.platforms:
                org, name, type = platform_handle.split(".")
                prev_hours = model_site_platforms.get_platform_parameter(
                    platform_handle, "previous_hours"
                )
                obs_mappings = model_site_platforms.platform_observation_mapping(
                    platform_handle
                )
                if prev_hours:
                    for hour in range(24, prev_hours + 24, 24):
                        for obs in obs_mappings:
                            if type != "radarcoverage":
                                # We want to handle wind_speed and direction seperatly since they will
                                # have vector place holders as well.
                                if (
                                        obs.target_obs != "wind_speed"
                                        and obs.target_obs != "wind_from_direction"
                                ):
                                    col_name = "{name}_{obs}_{hour}".format(
                                        name=name, obs=obs.target_obs, hour=hour
                                    )
                                    self._df_column_names.append(col_name)
                                else:
                                    if obs.target_obs == "wind_speed":
                                        # We want to have vector wind speed/dir as well as the magnitude and direction
                                        col_name = "{name}_wind_v_{hour}".format(
                                            name=name, hour=hour
                                        )
                                        self._df_column_names.append(col_name)
                                        col_name = "{name}_wind_speed_{hour}".format(
                                            name=name, hour=hour
                                        )
                                        self._df_column_names.append(col_name)
                                    if obs.target_obs == "wind_from_direction":
                                        col_name = (
                                            "{name}_wind_from_direction_{hour}".format(
                                                name=name, hour=hour
                                            )
                                        )
                                        self._df_column_names.append(col_name)
                                        col_name = "{name}_wind_u_{hour}".format(
                                            name=name, hour=hour
                                        )
                                        self._df_column_names.append(col_name)
                else:
                    col_name = "{name}_{obs}".format(name=name, obs=obs.target_obs)
                    self._df_column_names.append(col_name)

            model_site_nexrad_platforms = self._nexrad_mappings.get_site(self._site)
            for platform_handle in model_site_nexrad_platforms.platforms:
                org, name, type = platform_handle.split(".")
                prev_hours = model_site_nexrad_platforms.get_platform_parameter(
                    platform_handle, "previous_hours"
                )
                obs_mappings = model_site_nexrad_platforms.platform_observation_mapping(
                    platform_handle
                )
                if prev_hours:
                    for hour in range(24, prev_hours + 24, 24):
                        for obs in obs_mappings:
                            # NEXRAD radar has some extra columns that go along.
                            # For first loop iteration, we add the extra nexrad data columns
                            if hour == 24:
                                self._df_column_names.append(
                                    "{name}_nexrad_total_1_day_delay".format(name=name)
                                )
                                self._df_column_names.append(
                                    "{name}_nexrad_total_2_day_delay".format(name=name)
                                )
                                self._df_column_names.append(
                                    "{name}_nexrad_total_3_day_delay".format(name=name)
                                )
                                self._df_column_names.append(
                                    "{name}_nexrad_dry_days_count".format(name=name)
                                )
                                self._df_column_names.append(
                                    "{name}_nexrad_rainfall_intensity".format(name=name)
                                )

                            self._df_column_names.append(
                                "{name}_nexrad_summary_{hour}".format(
                                    name=name, hour=hour
                                )
                            )
            self._data_frame = pd.DataFrame(columns=self._df_column_names)
        return True

    def query_data(self, start_date, end_date):
        logger.debug(f"Site: {self._site} start query data for datetime: {start_date}")
        self._data_frame.at[self._row_id, "entero_date"] = start_date

        self.get_tide_data(start_date)

        model_site_platforms = self._platform_mappings.get_site(self._site)
        for platform_handle in model_site_platforms.platforms:
            # Get the previous_hours setting if we are using that.
            prev_hours = model_site_platforms.get_platform_parameter(
                platform_handle, "previous_hours"
            )
            platform_observations = model_site_platforms.platform_observation_mapping(
                platform_handle
            )

            if prev_hours:
                # For observation platforms, we use the get_platform_data function.
                for hour in range(24, prev_hours + 24, 24):
                    self.get_platform_data(
                        platform_handle, platform_observations, start_date, hour
                    )
            else:
                self.get_platform_data(
                    platform_handle, platform_observations, start_date, prev_hours
                )
        # Get NEXRAD Data
        model_site_nexrad_platforms = self._nexrad_mappings.get_site(self._site)
        for platform_handle in model_site_nexrad_platforms.platforms:
            # Get the previous_hours setting if we are using that.
            prev_hours = model_site_nexrad_platforms.get_platform_parameter(
                platform_handle, "previous_hours"
            )
            # platform_handle, start_date, prev_hours, row_id):
            self.get_nexrad_data(platform_handle, start_date, prev_hours)

        self._row_id += 1
        return

    def get_tide_data(self, start_date):
        start_time = time.time()
        primary_tide_station = self._tide_station
        logger.debug(
            f"Start retrieving tide data for station: {primary_tide_station} date: {start_date}"
        )

        tide = noaaTideDataExt(use_raw=True, logger=logger)

        tide_start_time = start_date - timedelta(hours=24)
        tide_end_time = start_date

        # Try and query the NOAA soap service. We give ourselves 5 tries.
        for x in range(0, 5):
            if logger:
                logger.debug(f"Attempt: {x + 1} retrieving tide data for station.")
                tide_data = tide.calcTideRangePeakDetect(
                    beginDate=tide_start_time,
                    endDate=tide_end_time,
                    station=primary_tide_station,
                    datum="MLLW",
                    units="feet",
                    timezone="GMT",
                    smoothData=False,
                    write_tide_data=False,
                )
            if tide_data is not None:
                break
        if tide_data is not None:
            tide_range = tide_data["HH"]["value"] - tide_data["LL"]["value"]
            self._data_frame.at[
                self._row_id, f"{self._tide_station}_tide_range"
            ] = tide_range
            self._data_frame.at[self._row_id, f"{self._tide_station}_tide_hi"] = float(
                tide_data["HH"]["value"]
            )
            self._data_frame.at[self._row_id, f"{self._tide_station}_tide_lo"] = float(
                tide_data["LL"]["value"]
            )
            self._data_frame.at[
                self._row_id, f"{self._tide_station}_tide_stage"
            ] = tide_data["tide_stage"]

            logger.debug(
                f"Finished retrieving tide data for station: {self._tide_station} date: {start_date} in "
                f"{time.time() - start_time} seconds"
            )

        return

    def get_platform_data(
            self, platform_handle, platform_observations, start_date, prev_hour
    ):
        org, name, type = platform_handle.split(".")
        end_date = start_date
        # We have to pair up the wind speed and direction, so we store them in this dict until we finish
        # processing other obs.
        wind_speed_direction = {}
        if prev_hour:
            begin_date = start_date - timedelta(hours=prev_hour)
        else:
            begin_date = start_date
        for observation in platform_observations:
            try:
                obs_recs = (
                    self.xenia_obs_db.session.query(sl_multi_obs)
                    .filter(
                        sl_multi_obs.m_date >= begin_date.strftime("%Y-%m-%dT%H:%M:%S")
                    )
                    .filter(
                        sl_multi_obs.m_date < end_date.strftime("%Y-%m-%dT%H:%M:%S")
                    )
                    .filter(sl_multi_obs.sensor_id == observation.sensor_id)
                    .filter(
                        or_(
                            sl_multi_obs.qc_level == qaqcTestFlags.DATA_QUAL_GOOD,
                            sl_multi_obs.qc_level == None,
                        )
                    )
                    .order_by(sl_multi_obs.m_date)
                    .all()
                )
            except Exception as e:
                logger.exception(e)
            else:
                if (
                        observation.target_obs != "wind_speed"
                        and observation.target_obs != "wind_from_direction"
                ):
                    col_name = f"{name}_{observation.target_obs}_{prev_hour}"
                    if len(obs_recs):
                        avg_data = sum(rec.m_value for rec in obs_recs) / len(obs_recs)
                        logger.debug(
                            f"Platform: {platform_handle} Avg {observation.target_obs}: {avg_data} "
                            f"Records used: {len(obs_recs)}"
                        )
                        self._data_frame.at[self._row_id, col_name] = avg_data
                else:
                    # Hold the wind speed and direction for vector processing.
                    if (
                            observation.target_obs == "wind_speed"
                            or observation.target_obs == "wind_from_direction"
                    ):
                        if observation.target_obs not in wind_speed_direction:
                            wind_speed_direction[observation.target_obs] = {}
                        wind_speed_direction[observation.target_obs] = obs_recs

        if len(wind_speed_direction):
            wind_speed_data = wind_speed_direction["wind_speed"]
            wind_dir_data = wind_speed_direction["wind_from_direction"]
            wind_dir_tuples = []
            direction_tuples = []
            scalar_speed_avg = None
            speed_count = 0
            for wind_speed_row in wind_speed_data:
                for wind_dir_row in wind_dir_data:
                    if wind_speed_row.m_date == wind_dir_row.m_date:
                        if scalar_speed_avg is None:
                            scalar_speed_avg = 0
                        scalar_speed_avg += wind_speed_row.m_value
                        speed_count += 1
                        # Vector using both speed and direction.
                        wind_dir_tuples.append(
                            (wind_speed_row.m_value, wind_dir_row.m_value)
                        )
                        # Vector with speed as constant(1), and direction.
                        direction_tuples.append((1, wind_dir_row.m_value))
                        break

            if len(wind_dir_tuples):
                # Unity components, just direction with speeds all 1.
                avg_dir_components = calcAvgSpeedAndDirV2(direction_tuples)
                scalar_speed_avg = scalar_speed_avg / speed_count
                avg_wnd_speed = scalar_speed_avg * meters_per_second_to_mph
                avg_wnd_direction = avg_dir_components["scalar"][1]

                avg_spd_dir_all = calcAvgSpeedAndDirV2(wind_dir_tuples)
                wind_spd_var_name_u = avg_spd_dir_all["vector"][0]
                wind_dir_var_name_v = avg_spd_dir_all["vector"][1]

                col_name = f"{name}_wind_speed_{prev_hour}"
                self._data_frame.at[self._row_id, col_name] = avg_wnd_speed
                col_name = f"{name}_wind_from_direction_{prev_hour}"
                self._data_frame.at[self._row_id, col_name] = avg_wnd_direction

                col_name = f"{name}_wind_u_{prev_hour}"
                self._data_frame.at[self._row_id, col_name] = wind_spd_var_name_u
                col_name = f"{name}_wind_v_{prev_hour}"
                self._data_frame.at[self._row_id, col_name] = wind_dir_var_name_v

                logger.debug(
                    f"Platform: {platform_handle} Avg Wind Speed: {scalar_speed_avg}(m_s-1) {scalar_speed_avg * meters_per_second_to_mph}(mph) Direction: {avg_dir_components['scalar'][1]} U: {wind_spd_var_name_u} V: {wind_dir_var_name_v} Rec Cnt: {len(wind_speed_data)}"
                )

    def get_nexrad_data(self, platform_handle, start_date, previous_hours):
        logger.debug(
            f"{platform_handle} start retrieving nexrad data datetime: {start_date.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        org, name, type = platform_handle.split(".")
        # Get the radar data for <previous_hours> in 24 hour intervals
        for prev_hour in range(24, previous_hours + 24, 24):
            col_name = f"{name}_nexrad_summary_{prev_hour}"
            radar_val = self._nexrad_db.getLastNHoursSummaryFromRadarPrecip(
                platform_handle,
                start_date,
                prev_hour,
                "precipitation_radar_weighted_average",
                "mm",
            )
            if radar_val != None:
                # Convert mm to inches
                current_value = radar_val * mm_to_inches
                self._data_frame.at[self._row_id, col_name] = current_value
            else:
                current_value = None
                logger.error(
                    f"No data available for boundary: {col_name} Date: {start_date}."
                )

        # calculate the X day delay totals
        # 1 Day Delay
        hour_24 = self._data_frame.at[self._row_id, f"{name}_nexrad_summary_24"]
        hour_48 = self._data_frame.at[self._row_id, f"{name}_nexrad_summary_48"]
        hour_72 = self._data_frame.at[self._row_id, f"{name}_nexrad_summary_72"]
        hour_92 = self._data_frame.at[self._row_id, f"{name}_nexrad_summary_96"]
        if not pd.isna(hour_24) and not pd.isna(hour_48):
            dly_col_name = f"{name}_nexrad_total_1_day_delay"
            self._data_frame.at[self._row_id, dly_col_name] = hour_48 - hour_24
        if not pd.isna(hour_48) and not pd.isna(hour_72):
            dly_col_name = f"{name}_nexrad_total_2_day_delay"
            self._data_frame.at[self._row_id, dly_col_name] = hour_72 - hour_48
        if not pd.isna(hour_72) and not pd.isna(hour_92):
            dly_col_name = f"{name}_nexrad_total_3_day_delay"
            self._data_frame.at[self._row_id, dly_col_name] = hour_92 - hour_72

        prev_dry_days = self._nexrad_db.getPrecedingRadarDryDaysCount(
            platform_handle, start_date, "precipitation_radar_weighted_average", "mm"
        )
        if prev_dry_days is not None:
            col_name = "{name}_nexrad_dry_days_count".format(name=name)
            self._data_frame.at[self._row_id, col_name] = prev_dry_days

        rainfall_intensity = self._nexrad_db.calcRadarRainfallIntensity(
            platform_handle,
            start_date,
            60,
            "precipitation_radar_weighted_average",
            "mm",
        )
        if rainfall_intensity is not None:
            col_name = "{name}_nexrad_rainfall_intensity".format(name=name)
            self._data_frame.at[self._row_id, col_name] = rainfall_intensity

        logger.debug(
            "Finished retrieving nexrad data datetime: %s"
            % (start_date.strftime("%Y-%m-%d %H:%M:%S"))
        )

    def to_csv(self, output_file):
        if self._data_frame is not None:
            try:
                self._data_frame.to_csv(path_or_buf=output_file)
            except Exception as e:
                logger.exception(e)
                raise e


class RadioIslandHistoricalData(RadioIslandData):
    def __init__(self, tide_file):
        super().__init__()
        self._tide_data_file = tide_file
        self._tide_data_obj = None
        self.load_tide_file()
        return

    def load_tide_file(self):
        # Build the historical tide data dict.
        with open(self._tide_data_file, "r") as tide_data_file:
            self._tide_data_obj = {}
            header = [
                "Station",
                "Date",
                "Range",
                "HH",
                "HH Date",
                "LL",
                "LL Date",
                "Tide Stage",
            ]
            data_csv = csv.DictReader(tide_data_file, fieldnames=header)
            line_num = 0
            for row in data_csv:
                if line_num:
                    self._tide_data_obj[row["Date"]] = {
                        "station": row["Station"],
                        "range": row["Range"],
                        "hh": row["HH"],
                        "hh_date": row["HH Date"],
                        "ll": row["LL"],
                        "ll_date": row["LL Date"],
                        "tide_stage": row["Tide Stage"],
                    }
                line_num += 1

    def query_historic_data(self, start_date, end_date, etcoc_value):
        self._data_frame.at[self._row_id, "enterococcus_value"] = etcoc_value
        try:
            super().query_data(start_date, end_date)
        except Exception as e:
            raise e

    def get_platform_data(self, platform_handle, start_date, prev_hour):
        org, name, type = platform_handle.split(".")
        platform_observations = self._platforms_config.platform_observation_mapping(
            platform_handle
        )
        end_date = start_date
        # We have to pair up the wind speed and direction, so we store them in this dict until we finish
        # processing other obs.
        wind_speed_direction = {}
        if prev_hour:
            begin_date = start_date - timedelta(hours=prev_hour)
        else:
            begin_date = start_date
        for observation in platform_observations:
            try:
                obs_recs = (
                    self._db_obj.session.query(sl_multi_obs)
                    .filter(
                        sl_multi_obs.m_date >= begin_date.strftime("%Y-%m-%dT%H:%M:%S")
                    )
                    .filter(
                        sl_multi_obs.m_date < end_date.strftime("%Y-%m-%dT%H:%M:%S")
                    )
                    .filter(sl_multi_obs.sensor_id == observation.sensor_id)
                    .filter(
                        or_(
                            sl_multi_obs.qc_level == qaqcTestFlags.DATA_QUAL_GOOD,
                            sl_multi_obs.qc_level == None,
                        )
                    )
                    .order_by(sl_multi_obs.m_date)
                    .all()
                )
            except Exception as e:
                logger.exception(e)
            else:
                if (
                        observation.target_obs != "wind_speed"
                        and observation.target_obs != "wind_from_direction"
                ):
                    col_name = "{name}_{obs}_{hour}".format(
                        name=name, obs=observation.target_obs, hour=prev_hour
                    )
                    if len(obs_recs):
                        avg_data = sum(rec.m_value for rec in obs_recs) / len(obs_recs)
                        logger.debug(
                            "Platform: {platform} Avg {obs}: {value} Records used: {rec_count}".format(
                                platform=platform_handle,
                                obs=observation.target_obs,
                                value=avg_data,
                                rec_count=len(obs_recs),
                            )
                        )
                        self._data_frame.at[self._row_id, col_name] = avg_data
                else:
                    # Hold the wind speed and direction for vector processing.
                    if (
                            observation.target_obs == "wind_speed"
                            or observation.target_obs == "wind_from_direction"
                    ):
                        if observation.target_obs not in wind_speed_direction:
                            wind_speed_direction[observation.target_obs] = {}
                        wind_speed_direction[observation.target_obs] = obs_recs

        if len(wind_speed_direction):
            wind_speed_data = wind_speed_direction["wind_speed"]
            wind_dir_data = wind_speed_direction["wind_from_direction"]
            wind_dir_tuples = []
            direction_tuples = []
            scalar_speed_avg = None
            speed_count = 0
            for wind_speed_row in wind_speed_data:
                for wind_dir_row in wind_dir_data:
                    if wind_speed_row.m_date == wind_dir_row.m_date:
                        if scalar_speed_avg is None:
                            scalar_speed_avg = 0
                        scalar_speed_avg += wind_speed_row.m_value
                        speed_count += 1
                        # Vector using both speed and direction.
                        wind_dir_tuples.append(
                            (wind_speed_row.m_value, wind_dir_row.m_value)
                        )
                        # Vector with speed as constant(1), and direction.
                        direction_tuples.append((1, wind_dir_row.m_value))
                        break

            if len(wind_dir_tuples):
                # Unity components, just direction with speeds all 1.
                avg_dir_components = calcAvgSpeedAndDirV2(direction_tuples)
                scalar_speed_avg = scalar_speed_avg / speed_count
                avg_wnd_speed = scalar_speed_avg * meters_per_second_to_mph
                avg_wnd_direction = avg_dir_components["scalar"][1]

                avg_spd_dir_all = calcAvgSpeedAndDirV2(wind_dir_tuples)
                wind_spd_var_name_u = avg_spd_dir_all["vector"][0]
                wind_dir_var_name_v = avg_spd_dir_all["vector"][1]

                col_name = f"{name}_wind_speed_{prev_hour}"
                self._data_frame.at[self._row_id, col_name] = avg_wnd_speed
                col_name = f"{name}_wind_from_direction_{prev_hour}"
                self._data_frame.at[self._row_id, col_name] = avg_wnd_direction

                col_name = f"{name}_wind_u_{prev_hour}"
                self._data_frame.at[self._row_id, col_name] = wind_spd_var_name_u
                col_name = f"{name}_wind_v_{prev_hour}"
                self._data_frame.at[self._row_id, col_name] = wind_dir_var_name_v

                logger.debug(
                    f"Platform: {platform_handle} Avg Wind Speed: {scalar_speed_avg}(m_s-1) {scalar_speed_avg * meters_per_second_to_mph}(mph) Direction: {avg_dir_components['scalar'][1]} U: {wind_spd_var_name_u} V: {wind_dir_var_name_v} Rec Cnt: {len(wind_speed_data)}"
                )

    def get_tide_data(self, start_date):
        logger.debug(
            f"{self._tide_station} start retrieving tide data date: {start_date}"
        )

        if self._tide_data_obj is not None:
            date_key = start_date.strftime("%Y-%m-%dT%H:%M:%S")
            if date_key in self._tide_data_obj:
                tide_rec = self._tide_data_obj[date_key]
                self._data_frame.at[
                    self._row_id, f"{self._tide_station}_tide_range"
                ] = tide_rec["range"]
                self._data_frame.at[
                    self._row_id, f"{self._tide_station}_tide_hi"
                ] = tide_rec["hh"]
                self._data_frame.at[
                    self._row_id, f"{self._tide_station}_tide_lo"
                ] = tide_rec["ll"]
                self._data_frame.at[
                    self._row_id, f"{self._tide_station}_tide_stage"
                ] = tide_rec["tide_stage"]
            else:
                logger.error(f"Tide Data not found for: {date_key}")
            logger.debug(
                f"{self._tide_station} finished retrieving tide data for date: {start_date}"
            )


def main():
    parser = optparse.OptionParser()
    parser.add_option("--ConfigFile", dest="config_file", help="")
    parser.add_option("--EnteroDataDirectory", dest="entero_data_dir", help="")
    parser.add_option("--OutputDirectory", dest="output_directory", help="")

    (options, args) = parser.parse_args()

    config_file = configparser.RawConfigParser()
    config_file.read(options.config_file)

    log_config_file = config_file.get("logging", "config_file")
    logging.config.fileConfig(log_config_file)
    logger = logging.getLogger()
    logger.info("Log file opened.")

    wq_sites = wq_sample_sites()
    wq_sites.load_sites(
        file_name=config_file.get("boundaries_settings", "sample_sites"),
        boundary_file="",
    )

    historic_db_file = config_file.get("historical_database", "name")
    db_obj = sl_xeniaAlchemy()
    if db_obj.connectDB("sqlite", None, None, historic_db_file, None, False):
        logger.info("Succesfully connect to DB: %s" % (historic_db_file))
    else:
        logger.error("Unable to connect to DB: %s" % (historic_db_file))

    wq_site = "C57"
    entero_data_dir = config_file.get("entero_limits", "data_directory")
    entero_data_file = os.path.join(entero_data_dir, "{site}.csv".format(site=wq_site))

    tide_stations = config_file.get("tide_stations", "stations").split(",")
    tide_data_files = []
    for tide_station in tide_stations:
        tide_data_files.append(
            os.path.join(
                config_file.get("tide_stations", "data_directory"),
                "{}.csv".format(tide_station),
            )
        )

    platform_config = config_file.get("platform_configuration", "setup_file")

    sites_platform_cfg = ModelSitesPlatforms()
    sites_platform_cfg.from_json_file(
        platform_config, sqlite_database_file=historic_db_file
    )

    eastern_tz = timezone("US/Eastern")
    utc_tz = timezone("UTC")

    output_file = os.path.join(
        options.output_directory, "{}_historical.csv".format(wq_site)
    )
    wq_data = RadioIslandHistoricalData(tide_file=tide_data_files[0])
    if wq_data.initialize(
            wq_site,
            sites_platform_cfg.get_site(wq_site),
            "8656483",
            {"sqlite_database_file": historic_db_file},
            {"nexrad_database_file": historic_db_file},
    ):

        header = [
            "Station",
            "DateTime",
            "Entero SSM",
            "Entero GM",
            "Entero SSM_CFU",
            "Entero GM2",
        ]
        with open(entero_data_file, "r") as entero_data_file:
            etcoc_read = csv.DictReader(entero_data_file, fieldnames=header)
            for row_ndx, row in enumerate(etcoc_read):
                if row_ndx:
                    date_only, time_only = row["DateTime"].split(" ")
                    wq_date = eastern_tz.localize(
                        datetime.strptime(date_only, "%Y-%m-%d")
                    )
                    wq_utc_date = wq_date.astimezone(utc_tz)
                    if len(row["Entero SSM"].strip()):
                        etcoc_value = float(row["Entero SSM"])
                    else:
                        etcoc_value = float(row["Entero SSM_CFU"])
                    wq_data.query_historic_data(wq_utc_date, wq_utc_date, etcoc_value)

            wq_data.to_csv(output_file)
    return


if __name__ == "__main__":
    main()
