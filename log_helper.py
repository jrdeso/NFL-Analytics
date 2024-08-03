import logging
from datetime import datetime, timedelta
import time
import sys

class NFL_Logging:
    def __init__(self):
        self.logname = 'logs/nfl_logging.log'
        logging.basicConfig(filename=self.logname, encoding='utf-8', level=logging.INFO, filemode='a')

    def get_timestamp(self):
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def label_log(self, script_name, function_name):
        logging.info(f"{self.get_timestamp()}: Starting: {script_name}: [{function_name}]")

    def info(self, message):
        logging.info(f"{self.get_timestamp()}: {message}")

    def critical(self, message):
        logging.critical(f"{self.get_timestamp()}: {message}")

