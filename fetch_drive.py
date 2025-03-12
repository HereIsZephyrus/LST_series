from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os
import time
from datetime import datetime
import logging
def check_task_status(task, task_identifier, gap = 20):
    """
    监控任务状态直到完成或失败
    
    Args:
        task: ee.batch.Task 对象
        timeout_minutes: 超时时间（分钟）
    
    Returns:
        bool: 任务是否成功完成
    """
    
    while True:
        status = task.status()
        state = status['state']
        if (state != 'READY'):
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {task_identifier} task state: {state}")
            if state == 'COMPLETED':
                time.sleep(10) # ensure the image is seccussfully written to drive
                print(f"✓ {task_identifier} task success")
                return True
            elif state in ['FAILED', 'CANCELLED']:
                logging.error(f"{task_identifier} failed")
                print(f"× {task_identifier} task failed")
                return False
            
        time.sleep(gap)


def create_folder(drive,parent_folder_id,folder_name):
    folder_metadata = {
        'title': folder_name,
        'parents': [{'id': parent_folder_id}],
        'mimeType': 'application/vnd.google-apps.folder'
    }
    try:
        folder = drive.CreateFile(folder_metadata)
        folder.Upload()

        print(f"已创建文件夹: {folder_name}")
        print(f"文件夹ID: {folder['id']}")
        return folder['id']
    except Exception as e:
        print(f"文件夹创建失败: {str(e)}")
        return None
    
def get_folder_id_by_name(drive, folder_name, parent_id='root'):
    """通过文件夹名称获取ID"""
    query = f"title='{folder_name}' and mimeType='application/vnd.google-apps.folder' and '{parent_id}' in parents and trashed=false"
    file_list = drive.ListFile({'q': query}).GetList()
    if file_list:
        return file_list[0]['id']
    return None

def download_and_clean(drive,folder_id, cloud_file_name, save_path):
    cloud_file_name = cloud_file_name
    os.makedirs(save_path, exist_ok=True)
    
    file_list = drive.ListFile({
        'q': f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder'",
        'maxResults': 1000
    }).GetList()
    
    is_found = False
    for file_obj in file_list:
        if (file_obj['title'].startswith(cloud_file_name)):
            is_found = True
            print(f"find file {cloud_file_name}")
            local_file_name = os.path.join(save_path, cloud_file_name)
            print(f"downloading to {local_file_name}")
            file_obj.GetContentFile(local_file_name)
            file_obj.Delete()
            print(f"delete {cloud_file_name}")
            continue
    if (not is_found):
        print(f"not find file {cloud_file_name}")
    return