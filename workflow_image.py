from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from landsat_lst_image import create_lst_image
from fetch_drive import download_and_clean, check_task_status
from fetch_drive import get_folder_id_by_name as get_folder_id
from dotenv import load_dotenv
import os
import ee
import time

def create_lst_image_timeseries(folder_id,folder_name,save_path,to_drive = True):
    asset_path = 'projects/ee-channingtong/assets/'
    total_boundary = ee.FeatureCollection(asset_path + 'YZBboundary')
    total_geometry = total_boundary.union().geometry()
    total_area = total_geometry.area().getInfo()
    print("Area of YZB: ", total_area) 
    index = 0
    if (to_drive):
        gauth = GoogleAuth()
        gauth.LocalWebserverAuth()  # 首次运行需要浏览器授权
        drive = GoogleDrive(gauth)
    for city_boundary in total_boundary.getInfo()['features']:
        city_name = city_boundary['properties']['市名']
        city_code = city_boundary['properties']['市代码']
        city_geometry = city_boundary['geometry']
        asset_name = f'urban_{city_code}'
        urban_boundary = ee.FeatureCollection(asset_path + asset_name)
        urban_geometry = urban_boundary.union().geometry()
        check_city_name = urban_boundary.getInfo()['features'][0]['properties']['city_name']
        if (city_name != check_city_name):
            print("City name mismatch: ", city_name, check_city_name)
            continue
        urban_area = urban_geometry.area().getInfo()
        print("Area of ", city_name, ": ", urban_area)
        year_list = range(1984,2024)
        year_list = [2022] # for test
        for year in year_list:
            month_list = range(1,13)
            month_list = [10] # for test
            for month in month_list:
                date_start = ee.Date.fromYMD(year,month,1)
                date_end = ee.Date.fromYMD(year,month,31)
                task = create_lst_image(city_name,date_start,date_end,city_geometry,urban_geometry,folder_name,to_drive)
                if (to_drive):
                    is_success = check_task_status(task)
                    if is_success:
                        time.sleep(30) # wait for the last iamge to be created
                        folder_id = get_folder_id(drive,folder_name)
                        download_and_clean(drive,folder_id, save_path)
                index += 1
    print("Total number of images: ", index)

def __main__():
    load_dotenv()
    SAVE_PATH = os.getenv('SERIES_SAVE_PATH')
    FOLDER_ID = os.getenv('SERIES_FOLDER_ID')

    ee.Initialize(project='ee-channingtong')
    folder_name = 'landsat_lst_timeseries'

    create_lst_image_timeseries(FOLDER_ID,folder_name,SAVE_PATH,False)

if __name__ == '__main__':
    __main__()