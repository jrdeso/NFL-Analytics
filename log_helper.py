import logging
from datetime import datetime, timedelta
import time
import sys

class NFL_Logging:
    def __init__(self):
        self.logname = 'logs/nfl_logging.log'
        logging.basicConfig(filename=self.logname, encoding='utf-8', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO, filemode='a')
    
    def label_log(self, script_name, function_name):
        logging.info(f"Starting: {script_name}: [{function_name}]")

    def info(self, message):
        logging.info(f"{message}")

    def warning(self, message):
        logging.warning(f"{message}")

    def critical(self, message):
        logging.critical(f"{message}")

    def reset_log_file(self):
        with open(self.logname, 'w') as log_file:
            pass

