from automation import *
from operation import *
from logger import *
from setting import get_variables

if __name__ == "__main__":

    try:
        
        # Get PID
        pid_file = get_variables().PID_FILE
        log_file = get_variables().LOG_FILE

        # instance
        tracker = OperationTracker(pid_file=pid_file, log_file=log_file)
        operation_id = tracker.generate_operation_id()
        tracker.start_operation(operation_id)

        print(f"PID: {pid_file}")

         # Generate Log File
        log_directory = get_variables().LOG_DIRECTORY
        operation_log=tracker.generate_log_file(log_directory=log_directory, operation_id=operation_id)
        print(f"LOG: {operation_log}")

        jobs = Automation(operation_log, operation_id)
        log = Logger(logfile=operation_log)

        print("**************************Jobs are started **********************************")
        print(f"pid : {operation_id}")
        print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        log.log_info("**************************Jobs are started **********************************")
        log.log_info(f"pid : {operation_id}")
        log.log_info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        # # Upload Log, if success it is okay
        # log.upload_log()

        # start jobs
        status = jobs.start_jobs()

        print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        print("**************************Jobs are ended *************************************")
        log.log_info("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        log.log_info("**************************Jobs are ended *************************************")
        # Upload Log, if success it is okay
        #log.upload_log()

        del tracker

    except Exception as e:
        print(f"Error: {e}")
        log.log_error(f"Error: {e}")
        del tracker
        #log.log_error(f"Error: {e}")