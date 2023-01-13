import sys
sys.path.append('../../commonfiles/python')
import logging.config
import json
import time
from data_collector_plugin import data_collector_plugin

class json_output_plugin(data_collector_plugin):
  def initialize_plugin(self, **kwargs):
    try:
      self.logger.debug("json_output_plugin initialize_plugin started.")
      self._plugin_details = kwargs['details']

      self.json_outfile = self._plugin_details.get("Settings", "json_outfile")

      self.ensemble_data = kwargs.get('ensemble_tests', None)
      self.execution_date= kwargs['execution_date'],
      self.prediction_date= kwargs['prediction_date'],

      self.logger.debug("json_output_plugin initialize_plugin finished.")
      return True
    except Exception as e:
      self.logger.exception(e)
    return False

  def run(self, **kwargs):
    start_time = time.time()
    try:
      logger_conf = self._plugin_details.get('Settings', 'logging')
      #logging.config.dictConfig(self.logging_client_cfg)
      logging.config.fileConfig(logger_conf)
      logger = logging.getLogger()
      logger.debug("json output run started.")

      logger.debug("Opening JSON output file: %s" %(self.json_outfile))
      with open(self.json_outfile, 'w') as json_output_file:
        station_data = {'features' : [],
                        'type': 'FeatureCollection'}
        features = []
        if self.ensemble_data is not None:
          for rec in self.ensemble_data:
            site_metadata = rec['metadata']
            test_results = rec['models']
            if 'statistics' in rec:
              stats = rec['statistics']
            test_data = []
            for test in test_results.models:
              test_data.append({
                'name': test.name,
                'p_level': test.prediction_level.__str__(),
                'p_value': test.result,
                'data': test.model_data
              })
            features.append({
              'type': 'Feature',
              'geometry': {
                'type': 'Point',
                'coordinates': [site_metadata.object_geometry.x, site_metadata.object_geometry.y]
              },
              'properties': {
                'desc': site_metadata.name,
                'ensemble': str(test_results.ensemblePrediction),
                'station': site_metadata.name,
                'tests': test_data
              }
            })
        station_data['features'] = features
        json_data = {
          'status': {'http_code': 200},
          'contents': {
            'run_date': self.execution_date,
            'testDate': self.prediction_date,
            'stationData': station_data
          }
        }
        try:
          logger.debug("Writing JSON file: %s" % (self.json_outfile))
          json_output_file.write(json.dumps(json_data, sort_keys=True))
        except Exception as e:
          logger.exception(e)
      logger.debug("Finished json output in %f seconds." % (time.time()-start_time))
    except IOError as e:
      logger.exception(e)
    return

  def finalize(self):
    return
