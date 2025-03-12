import ee.data
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from datetime import datetime
from fetch_drive import monitor_export_task
import multiprocessing as mp
import monitor
import ee
import os
import logging

def retrieve_unfinished_tasks():
    monitor_file_path = os.getenv('PROCESS_MONITOR_FILE_PATH')
    # read local unfinished tasks record
    unfinished_tasks = []
    with open(monitor_file_path, 'r') as f:
        lines = f.readlines()
    for line in lines:
        unfinished_tasks.append(line.split(':')[1])
    # read gee task manager unfinished tasks
    ee.Initialize(os.getenv('PROJECT_NAME'))
    tasks = ee.data.listOperations()
    for task in tasks:
        if task.state in ['READY', 'RUNNING']:
            unfinished_tasks.append(task.description)
    unfinished_tasks = list(set(unfinished_tasks))
    return unfinished_tasks

def rebuild_process_monitor(task_list):
    monitor_file_path = os.getenv('PROCESS_MONITOR_FILE_PATH')
    with open(monitor_file_path, 'w') as f:
        for file_name in task_list:
            process = mp.Process(
                    name=file_name,
                    target=monitor_export_task, 
                    args=(gauth,task,file_name,drive,folder_name,save_path),
                    daemon=False
                )
            process.start()
            logging.info(f'[{process.pid}] start export {file_name}')
            monitor.add_process(process)

def __main__():
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile('credentials.json')
    if gauth.credentials is None:
        gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)
    unfinished_tasks = retrieve_unfinished_tasks()
    rebuild_process_monitor(unfinished_tasks)   

if __name__ == '__main__':
    __main__()