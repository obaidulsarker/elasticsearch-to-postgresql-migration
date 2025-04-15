from setting import get_variables
from logger import *
from datetime import datetime
import time

class email_template(Logger):
    def __init__(self, logfile, operation_id, operation_start_datetime, operation_end_datetime, operation_status, total_duration, total_task, completed_tasks, task_list) -> None:
        super().__init__(logfile)
        self.operation_id = operation_id
        self.operation_start_datetime = operation_start_datetime 
        self.operation_end_datetime = operation_end_datetime
        self.operation_status = operation_status
        self.total_duration = total_duration
        self.current_task_duration_seconds=0
        self.total_task = total_task
        self.completed_tasks = completed_tasks
        self.task_list= task_list
        self.email_template_file = get_variables().EMAIL_TEMPLATE
        self.task_tr = """<tr style="height: 18px;">
<td style="width: 35.8594px; height: 18px;">&nbsp;{task_id}</td>
<td style="width: 285.109px; height: 18px;">{task_name}</td>
<td style="width: 132.203px; height: 18px;">{start_time}</td>
<td style="width: 137.859px; height: 18px;">{task_status}</td>
<td style="width: 183.172px; height: 18px;">{task_duration}</td>
</tr>
"""
    
    # Generate duration
    def get_duration(self, start_datetime, end_datetime):
        duration = 'None'
        date_time_format = '%Y-%m-%d %H:%M:%S'
        try:
            #2023-11-10 21:39:36

            if (start_datetime=='None'):
                raise Exception("Start datetime is empty!")

            if (end_datetime=='None'):
                current_datetime = datetime.now()
                end_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

            # start datetime
            dt_start_datetime = datetime.strptime(start_datetime, date_time_format)
            dt_end_datetime = datetime.strptime(end_datetime, date_time_format)
            time_difference = dt_end_datetime - dt_start_datetime
            total_seconds = time_difference.total_seconds()
            self.current_task_duration_seconds = total_seconds
            duration = time.strftime("%H:%M:%S", time.gmtime(total_seconds))

            return duration
        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return duration
    
    # Convert Duration into seconds
    def convert_duration_into_seconds(self, duration_str):
        total_seconds = 0
        try:
            duration_obj = datetime.strptime(duration_str, "%H:%M:%S")
            total_seconds = duration_obj.hour * 3600 + duration_obj.minute * 60 + duration_obj.second
        
            return total_seconds
        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return total_seconds
            
    # Generate table for list of tasks
    def generate_task_table(self):
        task_table = ""
        self.current_task_duration_seconds = 0
        try:
            tr = self.task_tr
            for task in self.task_list:
                # replace task_id
                #print(task.task_start_datetime)

                if (str(task.task_status)=='In Progress'):
                    task.task_duration = self.get_duration(start_datetime=task.task_start_datetime, end_datetime=task.task_end_datetime)

                start_time=""
                if (task.task_start_datetime!='None'):
                    start_time = str(task.task_start_datetime).split(" ")[1]
                else:
                    start_time = task.task_start_datetime

                new_tr = tr.replace("{task_id}", str(task.task_id)).replace("{task_name}", str(task.task_name)).replace("{start_time}", start_time).replace("{task_status}", str(task.task_status)).replace("{task_duration}", str(task.task_duration))
                task_table = task_table + new_tr
            
            return task_table
        
        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return task_table

    # generate email body
    def get_email_body(self):
        email_body=""
        try:
            
            task_tr_lst = self.generate_task_table()

            template=None
            # read template
            # Open the file in read mode
            with open(self.email_template_file, 'r') as file:
                template = file.read()

            #print(f"File Content: {template}")

            if (self.operation_status=='In Progress'):
                total_seconds = self.convert_duration_into_seconds(duration_str=self.total_duration) + self.current_task_duration_seconds
                self.total_duration = time.strftime("%H:%M:%S", time.gmtime(total_seconds))

            # replace variable
            template = template.replace("{operation_id}", str(self.operation_id))
            template = template.replace("{operation_start_datetime}", str(self.operation_start_datetime))
            template = template.replace("{operation_end_datetime}", str(self.operation_end_datetime))
            template = template.replace("{operation_status}", str(self.operation_status))
            template = template.replace("{total_duration}", str(self.total_duration))
            template = template.replace("{total_task}", str(self.total_task))
            template = template.replace("{completed_tasks}", str(self.completed_tasks))
            template = template.replace("{task_list}", task_tr_lst)

            email_body = template
            
            #print(f"email_body = {email_body}")

            return email_body
        except Exception as e:
            print(f"Exception: {str(e)}")
            self.log_error(f"Exception: {str(e)}")
            return email_body