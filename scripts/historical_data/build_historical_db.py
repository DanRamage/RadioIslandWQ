import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append("../../commonfiles/python")
from datetime import datetime, timedelta
import time
from numpy import isnan
from noaa_coops import Station
from multiprocessing import Queue

from SQLiteMultiProcDataSaver import SQLiteMPDataSaver
from xeniaSQLiteAlchemy import xeniaAlchemy as sl_xeniaAlchemy, multi_obs as sl_multi_obs, platform as sl_platform
from xenia_obs_map import obs_map, json_obs_map
import logging.config


logger = logging.getLogger()

class build_database:
    def __init__(self, historic_db_name, platforms_config, data_saver_log_configfile):
        self._historic_db_filename = historic_db_name
        self._historical_db = sl_xeniaAlchemy()
        if self._historical_db.connectDB('sqlite', None, None, self._historic_db_filename, None, False):
            logger.info("Succesfully connect to DB: %s" % (self._historic_db_filename))
        else:
            logger.error("Unable to connect to DB: %s" % (self._historic_db_filename))

        self._platforms_config = platforms_config
        self._row_entry_date = datetime.now()
        self._obs_map = json_obs_map()
        self._data_queue = Queue()
        self._record_saver = SQLiteMPDataSaver(self._historic_db_filename, data_saver_log_configfile, self._data_queue)

        return
    def initialize(self):
        logger.info("Starting the data saver.")
        #Start the data saver process.
        self._record_saver.start()

    def query_data(self, begin_date):
        try:
            for platform_cfg in self._platforms_config:
                data_provider,platform_id,platform_type = platform_cfg.split('.')
                platform = self._platforms_config[platform_cfg]
                self.setup_platform(platform_cfg, platform['observations'])
                if data_provider == 'nos':
                    start_date = begin_date - timedelta(hours=platform["previous_hours"])
                    end_date = begin_date
                    self.get_noaa_data(platform_cfg, platform_id, start_date, end_date)

        except Exception as e:
            logger.exception(e)
        return
    '''
    Check if the platform and observation types exist in the database, if they don't add them.
    '''
    def setup_platform(self, xenia_platform, observations):
        platform_id = self._historical_db.platformExists(xenia_platform)
        # We will add the observations types into the database if they do not already exist.
        self._obs_map.load_json(observations)
        if platform_id is None:
            self._historical_db.newPlatform(self._row_entry_date, xenia_platform, 0.0, 0.0)
        #Need to add the observations into the db if they don't exist. If they exist already
        #we'll get the ids we need to save the multi_obs records.
        self._obs_map.build_db_mappings(sqlite_database_file=self._historic_db_filename,
                                        platform_handle=xenia_platform)

    def get_noaa_data(self, xenia_platform, station, begin_date, end_date):
        station_obj = Station(station)
        for obs in self._obs_map:
            if len(obs.source_obs):
                try:
                    try:
                        if self._data_queue.qsize() > 500:
                            logger.info("Waiting for queue size to shrink.")
                            time.sleep(5)

                    # We get this exception under OSX.
                    except NotImplementedError:
                        pass

                    logger.info(
                        "Query data for platform: {platform} product: {obs}({obs_id}) Start Date: {start_date} End Date: {end_date}".format(
                            platform=xenia_platform,
                            obs=obs.source_obs,
                            obs_id=obs.sensor_id,
                            start_date=begin_date.strftime("%Y%m%d"),
                            end_date=end_date.strftime("%Y%m%d")))
                    obs_df = station_obj.get_data(begin_date=begin_date.strftime("%Y%m%d"),
                                         end_date=end_date.strftime("%Y%m%d"),
                                         product=obs.source_obs,
                                         time_zone="GMT")
                    if obs.source_obs != 'wind':
                        obs_name = obs_df.axes[1][0]
                    else:
                        obs_name = 'spd'
                        wind_direction_name = 'dir'
                    #The product name that is stored in the dataframe is a truncated version of
                    #what is used to query the data with, for example air_temperature is air_temp in the data frame.
                    #Since the dataframe is(currently) 2 axes with the first  being the time and the second
                    #being the product and flags for the product.
                    for index in obs_df.index:
                        m_date = index
                        m_value = obs_df[obs_name][index]
                        add_record = True
                        if isnan(m_value):
                            add_record = False
                        #One of the quality flags indicates data suspect.
                        if obs_df['flags'][index].find('1') != -1:
                            add_record = False

                        if add_record:
                            m_obs = sl_multi_obs(row_entry_date=self._row_entry_date.strftime("%Y-%m-%d %H:%M:%S"),
                                                 platform_handle=xenia_platform,
                                                 m_lon=station_obj.lat_lon['lon'],
                                                 m_lat=station_obj.lat_lon['lat'],
                                                 sensor_id=obs.sensor_id,
                                                 m_type_id=obs.m_type_id,
                                                 m_date=index.strftime("%Y-%m-%d %H:%M:%S"),
                                                 m_value=m_value
                                                 )
                            self._data_queue.put(m_obs)
                            if obs.source_obs == 'wind':
                                wind_dir_obs = self._obs_map.get_rec_from_xenia_name('wind_from_direction')
                                m_value = obs_df[wind_direction_name][index]
                                m_obs = sl_multi_obs(row_entry_date=self._row_entry_date.strftime("%Y-%m-%d %H:%M:%S"),
                                                     platform_handle=xenia_platform,
                                                     m_lon=station_obj.lat_lon['lon'],
                                                     m_lat=station_obj.lat_lon['lat'],
                                                     sensor_id=wind_dir_obs.sensor_id,
                                                     m_type_id=wind_dir_obs.m_type_id,
                                                     m_date=index.strftime("%Y-%m-%d %H:%M:%S"),
                                                     m_value=m_value
                                                     )
                                self._data_queue.put(m_obs)


                except Exception as e:
                    logger.exception(e)
        return(obs_df)

    def finalize(self):
        logger.info("Finalizing the build.")
        self._record_saver.data_queue.put(None)
        #Now shutdown the data saver.
        self._record_saver.join()
        logger.info("Finalized the build.")
