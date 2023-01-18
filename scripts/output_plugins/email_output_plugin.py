import sys

sys.path.append('../../commonfiles/python')

from mako.template import Template
from mako import exceptions as makoExceptions
from smtp_utils import smtpClass
import os
import logging.config
# from output_plugin import output_plugin
from data_collector_plugin import data_collector_plugin
import configparser
import time


class email_output_plugin(data_collector_plugin):
    def initialize_plugin(self, **kwargs):
        try:
            details = kwargs['details']
            self.nowcast_site_name = kwargs['nowcast_site']
            self.prediction_date = kwargs['prediction_date']
            self.execution_date = kwargs['execution_date']
            self.ensemble_tests = kwargs['ensemble_tests']

            self._plugin_details = details
            self._passworded_options = details.get("Settings", "passworded_settings")
            self.result_outfile = details.get("Settings", "results_outfile")
            self.results_template = details.get("Settings", "results_template")
            self.report_url = details.get("Settings", "report_url")
            try:
                config_file = configparser.RawConfigParser()
                config_file.read(self._passworded_options)

                self.mailhost = config_file.get("email_report_output_plugin", "mailhost")
                self.mailport = config_file.getint("email_report_output_plugin", "port")
                self.fromaddr = config_file.get("email_report_output_plugin", "fromaddr")
                self.toaddrs = config_file.get("email_report_output_plugin", "toaddrs").split(',')
                self.subject = config_file.get("email_report_output_plugin", "subject")
                self.user = config_file.get("email_report_output_plugin", "user")
                self.password = config_file.get("email_report_output_plugin", "password")
            except Exception as e:
                self.logger.exception(e)

            return True
        except Exception as e:
            self.logger.exception(e)
        return False

    def run(self, **kwargs):
        try:
            start_time = time.time()
            logger_conf = self._plugin_details.get('Settings', 'logging')
            logging.config.fileConfig(logger_conf)
            logger = logging.getLogger()
            logger.debug("email_output_plugin run started.")
            mytemplate = Template(filename=self.results_template)
            file_ext = os.path.splitext(self.result_outfile)
            file_parts = os.path.split(file_ext[0])
            # Add the prediction date into the filename
            file_name = f"{file_parts[1]}-{self.prediction_date.replace(':', '_').replace(' ', '-')}{file_ext[1]}"
            out_filename = os.path.join(file_parts[0], file_name)
            with open(out_filename, 'w') as report_out_file:
                report_url = f"{self.report_url}/{file_name}"
                results_report = mytemplate.render(nowcast_site=self.nowcast_site_name,
                                                   ensemble_tests=self.ensemble_tests,
                                                   prediction_date=self.prediction_date,
                                                   execution_date=self.execution_date,
                                                   report_url=report_url)
                report_out_file.write(results_report)
        except TypeError as e:
            logger.exception(makoExceptions.text_error_template().render())
        except (IOError, AttributeError, Exception) as e:
            logger.exception(e)
        else:
            try:
                subject = self.subject % (self.prediction_date)
                # Now send the email.
                smtp = smtpClass(host=self.mailhost,
                                 user=self.user, password=self.password,
                                 port=self.mailport,
                                 use_tls=True)
                smtp.rcpt_to(self.toaddrs)
                smtp.from_addr(self.fromaddr)
                smtp.subject(subject)
                smtp.message(results_report)
                smtp.send(content_type="html")
            except Exception as e:
                logger.exception(e)
            logger.debug(f"email_output_plugin finished in {time.time() - start_time} seconds.")

            return

    def finalize(self):
        return
