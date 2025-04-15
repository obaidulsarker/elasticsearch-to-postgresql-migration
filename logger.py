import logging
import os

class Logger:
    def __init__(self, logfile) -> None:
        try:
            # Set up logging
            self.log_file = logfile
            self.log_directory, self.log_file_name = os.path.split(logfile)
            
            logging.basicConfig(filename=logfile, level=logging.INFO, format='%(asctime)s [%(levelname)s]: %(message)s')
        
        except Exception as e:
            self.log_error(f"Exception: {str(e)}")

    # Function to log errors
    def log_error(self, message):
        try:
            logging.error(message)
            #self.upload_log()
        except Exception as e:
            print(f"Exception: {str(e)}")
         
    # Function to log warnings
    def log_warning(self, message):
        try:
            logging.warning(message)
            
        except Exception as e:
            self.log_error(f"Exception: {str(e)}")
            
    # Function to log info messages
    def log_info(self, message):
        try:
            logging.info(message)
            
        except Exception as e:
            self.log_error(f"Exception: {str(e)}")
           
    # Function to log fatal messages
    def log_fatal(self, message):
        try:
            logging.fatal(message)
            
        except Exception as e:
            self.log_error(f"Exception: {str(e)}")

  