import time
import logging
import os
from multiprocessing import Lock
from datetime import datetime, timedelta

def file_is_occupied(file_name):
    """
    check if the record file is occupied by other process
    """
    try:
        with open(file_name, 'r') as f:
            f.readlines()
        return False
    except IOError:
        return True
    except Exception as e:
        logging.error(f"error to check if file {file_name} is occupied: {e}")
        raise e


def is_process_counter_exceed_limit():
    process_list_file_name = os.getenv('PROCESS_MONITOR_FILE_PATH')
    while (file_is_occupied(process_list_file_name)):
        time.sleep(1)
    try:
        with open(process_list_file_name, 'r') as f:
            process_list = f.readlines()
    except Exception as e:
        logging.error(f"error to check process counter: {e}")
        raise e
    return len(process_list) > 100

def add_process(process):
    process_list_file_name = os.getenv('PROCESS_MONITOR_FILE_PATH')
    try:
        with Lock():
            while (file_is_occupied(process_list_file_name)):
                time.sleep(1)
            with open(process_list_file_name, 'a') as f:
                f.write(f'{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}-{process.pid}:{process.name}\n')
    except Exception as e:
        logging.error(f"error to add process: {e}")
        raise e

def remove_process(process_name):
    process_list_file_name = os.getenv('PROCESS_MONITOR_FILE_PATH')
    try:
        with Lock():
            while (file_is_occupied(process_list_file_name)):
                time.sleep(1)
            with open(process_list_file_name, 'r') as f:
                process_list = f.readlines()
            process_list = [line for line in process_list if line.split(':')[1].strip() != process_name]
            with open(process_list_file_name, 'w') as f:
                f.writelines(process_list)
    except Exception as e:
        logging.error(f"error to remove process: {e}")
        raise e
    
def check_and_refresh_token(gauth):
    if gauth.credentials.refresh_token is None:
        raise Exception('refresh token is None')
    delta_time = (gauth.credentials.token_expiry + timedelta(hours=8) - datetime.now()).total_seconds() # adjust for UTC+8
    if delta_time < 300:
        gauth.Refresh()
        gauth.SaveCredentialsFile(os.getenv('CREDENTIALS_FILE_PATH'))
        logging.info(f"token current expires in: {gauth.credentials.token_expiry}")