import uuid
import os
import datetime


class OperationTracker:
    def __init__(self, pid_file, log_file):
        self.operations = {}
        self.pid_file=pid_file
        self.log_file=log_file

    # Destructor
    def __del__(self):
        self.delete_pid()
        self.delete_log()

    def generate_operation_id(self):
        operation_id = str(uuid.uuid4())
        return operation_id

    def generate_log_file(self, log_directory, operation_id):

        try:
            log_directory = log_directory

            # Create the log directory if it doesn't exist
            if not os.path.exists(log_directory):
                os.makedirs(log_directory)

            # Clearn PID
            self.delete_pid()
            self.delete_log()

            # Generate a timestamp to use in the log file name
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

            # Define the log file name with the timestamp
            log_filename = f"log_{operation_id}_{timestamp}.log"

            # Create and open the log file for writing
            log_path = os.path.join(log_directory, log_filename)
            with open(log_path, "w") as log_file:
                log_file.write("\n********* Operation: " + operation_id +" **********************")
                log_file.write("\n********* Started at: " + timestamp +" *************************")
                log_file.write("\n")

            # Save PID and Log
            self.save_pid(pid=operation_id)
            self.save_log(operational_log=log_path)

            return log_path
        except Exception as e:
            print(f"Exception: {str(e)}")
            return None
        
    # Save PID
    def save_pid(self, pid):
        try:
            with open(self.pid_file, "w") as file:
                file.write(pid)

            return True
        except Exception as e:
            print(f"Exception:{str(e)}")
            return False
    
    # Delete PID From File
    def delete_pid(self):
        try:
            with open(self.pid_file, "w") as file:
                pass

            return True
        except Exception as e:
            print(f"Exception:{str(e)}")
            return False

    # Save Operational Log Location
    def save_log(self, operational_log):
        try:
            with open(self.log_file, "w") as file:
                file.write(operational_log)

            return True
        except Exception as e:
            print(f"Exception:{str(e)}")
            return False

    # Delete Log
    def delete_log(self):
        try:
            with open(self.log_file, "w") as file:
                pass

            return True
        except Exception as e:
            print(f"Exception:{str(e)}")
            return False    

    def todo_operation(self, operation_id):
        if operation_id in self.operations:
            raise ValueError(f"Operation with ID {operation_id} already exists.")
        self.operations[operation_id] = "Pending"

    def start_operation(self, operation_id):
        if operation_id in self.operations:
            raise ValueError(f"Operation with ID {operation_id} already exists.")
        self.operations[operation_id] = "In Progress"

    def complete_operation(self, operation_id):
        if operation_id not in self.operations:
            raise ValueError(f"Operation with ID {operation_id} does not exist.")
        self.operations[operation_id] = "Completed"

    def get_operation_status(self, operation_id):
        if operation_id not in self.operations:
            return "Operation not found"
        return self.operations[operation_id]
