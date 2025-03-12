import time
import logging
import os
process_counter = 0

def is_process_counter_exceed_limit():
    return process_counter > 100

def increment_process_counter():
    global process_counter
    process_counter += 1

def decrement_process_counter():
    global process_counter
    process_counter -= 1

def check_and_refresh_token(gauth):
    if gauth.credentials.refresh_token is None:
        raise Exception('refresh token is None')
    if gauth.credentials.token_expiry - time.time() < 300:
        gauth.Refresh()
        gauth.SaveCredentialsFile(os.getenv('CREDENTIALS_FILE_PATH'))
        logging.info(f"token current expires in: {gauth.credentials.token_expiry}")