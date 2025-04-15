#from migrate_es_to_pg import EasticSearchToPg
from ElasticsearchToPostgres import ElasticsearchToPostgres
from logger import Logger

class Automation(Logger):
    def __init__(self, logfile, operation_id):
        super().__init__(logfile)
        self.operation_log=logfile
        self.operation_id = operation_id

    # Doing automation tasks
    def start_jobs(self):
        try:
        
            operation_log = self.operation_log

            # Migration Instance
            #db = EasticSearchToPg(operation_log)
            db = ElasticsearchToPostgres(operation_log)
            
            status = db.run_migration()

            return True
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None
        
