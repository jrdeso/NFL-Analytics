import logging
from datetime import datetime, timedelta
import os

class NFL_Logging:
    _log_filename = None
    _log_initialized = False

    def __init__(self, script_name=None):
        # Create timestamped filename once
        if NFL_Logging._log_filename is None:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            os.makedirs('logs', exist_ok=True)
            NFL_Logging._log_filename = os.path.join('logs', f'nfl_logging_{timestamp}.log')

        self.logname = NFL_Logging._log_filename

        # Configure logging once
        if not NFL_Logging._log_initialized:
            logging.basicConfig(
                filename=self.logname,
                encoding='utf-8',
                format='%(asctime)s - %(levelname)s - %(message)s',
                level=logging.INFO,
                filemode='a'
            )
            NFL_Logging._log_initialized = True
    
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