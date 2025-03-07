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
        # 获取任务状态
        status = task.status()
        state = status['state']
        
        # 打印当前状态
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {task_identifier} 任务状态: {state}")
        
        # 检查是否完成
        if state == 'COMPLETED':
            print(f"✓ {task_identifier} 任务成功完成！")
            return True
            
        # 检查是否失败
        elif state in ['FAILED', 'CANCELLED']:
            logging.error(f"{task_identifier} failed")
            print(f"× {task_identifier} 任务失败")
            return False
            
        # 等待一段时间再检查
        time.sleep(gap)  # 10秒检查一次


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
    # 创建保存目录
    cloud_file_name = cloud_file_name + '.tif'
    os.makedirs(save_path, exist_ok=True)
    
    # 查询文件夹内容（排除子文件夹）
    file_list = drive.ListFile({
        'q': f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder'",
        'maxResults': 1000
    }).GetList()
    
    for file_obj in file_list:
        if (file_obj['title'] == cloud_file_name):
            print(f"找到文件: {cloud_file_name}")
            local_file_name = os.path.join(save_path, cloud_file_name)
            print(f"正在下载 {cloud_file_name}")
            file_obj.GetContentFile(local_file_name)
            file_obj.Delete()
            print(f"已成功删除{cloud_file_name}")
            return
    print(f"未找到文件: {cloud_file_name}")
    return