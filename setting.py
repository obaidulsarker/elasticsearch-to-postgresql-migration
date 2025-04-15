from dotenv import load_dotenv
from pathlib import Path
import os
import platform

class EnvVariables:
    def __init__(self):

        dotenv_path = None
        os_name = platform.system()

        # if (os_name=="Windows"):
        #     dotenv_path = Path('cred\.env')
        # else:
        #     dotenv_path = Path('cred/.env')

        dotenv_path = os.path.join("cred", ".env")

        #dotenv_path = '.env'

        load_dotenv(dotenv_path=dotenv_path)

        self.ES_HOST = os.getenv("ES_HOST")
        self.ES_PORT = os.getenv("ES_PORT")
        self.ES_USER = os.getenv("ES_USER")
        self.ES_PASSWORD = os.getenv("ES_PASSWORD")
        self.BATCH_SIZE = os.getenv("BATCH_SIZE")

        # TARGET DB
        self.PG_HOST = os.getenv("PG_HOST")
        self.PG_PORT = os.getenv("PG_PORT")
        self.PG_DATABASE = os.getenv("PG_DATABASE")
        self.PG_USER = os.getenv("PG_USER")
        self.PG_PASSWORD = os.getenv("PG_PASSWORD")
        self.PG_SCHEMA = os.getenv("PG_SCHEMA")

        # FOR ETL
        self.ETL_PG_HOST = os.getenv("ETL_PG_HOST")
        self.ETL_PG_PORT = os.getenv("ETL_PG_PORT")
        self.ETL_PG_DATABASE = os.getenv("ETL_PG_DATABASE")
        self.ETL_PG_USER = os.getenv("ETL_PG_USER")
        self.ETL_PG_PASSWORD = os.getenv("ETL_PG_PASSWORD")
        self.ETL_PG_SCHEMA = os.getenv("ETL_PG_SCHEMA")
        self.ETL_PG_TABLE = os.getenv("ETL_PG_TABLE")

        self.LOG_DIRECTORY = os.getenv("LOG_DIRECTORY")
        self.LOG_FILE = os.getenv("LOG_FILE")
        self.PID_FILE = os.getenv("PID_FILE")
        
        # if (os_name=="Windows"):
        #     self.LOG_DIRECTORY = os.getenv("LOG_DIRECTORY").replace("/", "\\")
        #     self.LOG_FILE = os.getenv("LOG_FILE").replace("/", "\\")
        #     self.PID_FILE = os.getenv("PID_FILE").replace("/", "\\")
        # else:
        #     self.LOG_DIRECTORY = os.getenv("LOG_DIRECTORY").replace("\\", "/")
        #     self.LOG_FILE = os.getenv("LOG_FILE").replace("\\", "/")
        #     self.PID_FILE = os.getenv("PID_FILE").replace("\\", "/")
        
def get_variables():
    try:
        env_variable = EnvVariables()

        return env_variable
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    VARIABLES = EnvVariables()
    print(f"ElasticSearch Host = {VARIABLES.ES_HOST}")
   