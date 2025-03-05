from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from landsat_lst_image import create_lst_image, filter_city_bound
from fetch_drive import download_and_clean, check_task_status
from fetch_drive import get_folder_id_by_name as get_folder_id
from dotenv import load_dotenv
import os
import ee
import time
import logging

logging.basicConfig(
    filename='workflow_image.log',
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logging.getLogger('ee').setLevel(logging.WARNING)

def create_lst_image_timeseries(folder_id,folder_name,save_path,to_drive = True):
    asset_path = 'projects/ee-channingtong/assets/'
    total_boundary = ee.FeatureCollection(asset_path + 'YZBboundary')
    #total_geometry = total_boundary.union().geometry()
    #total_area = total_geometry.area().getInfo()
    #print("Area of YZB: ", total_area) 
    month_length = [31,28,31,30,31,30,31,31,30,31,30,31]
    index = 0
    if (to_drive):
        gauth = GoogleAuth()
        gauth.LocalWebserverAuth()  # 首次运行需要浏览器授权
        drive = GoogleDrive(gauth)
    for city_boundary in total_boundary.getInfo()['features']:
        city_name = city_boundary['properties']['市名']
        city_code = city_boundary['properties']['市代码']
        city_geometry = ee.Geometry(city_boundary['geometry'])
        logging.info(f"{city_name}'s administrative city area: {city_geometry.area().getInfo()}")
        asset_name = f'urban_{city_code}'
        urban_boundary = ee.FeatureCollection(asset_path + asset_name)
        urban_geometry = filter_city_bound(urban_boundary.geometry())
        check_city_name = urban_boundary.getInfo()['features'][0]['properties']['city_name']
        if (city_name != check_city_name):
            logging.warning(f"City name mismatch: {city_name}, {check_city_name}")
            continue
        year_list = range(1984,2024)
        year_list = [2022] # for test
        for year in year_list:
            month_list = range(1,13)
            month_list = [10] # for test
            for month in month_list:
                try:
                    logging.info(f"Processing: Year={year}, Month={month}")
                    
                    date_start = ee.Date.fromYMD(year, month, 1)
                    date_end = ee.Date.fromYMD(year, month, month_length[month-1]).advance(1, 'day')
                    
                    logging.debug(f"Start date: {date_start.format().getInfo()}")
                    logging.debug(f"End date: {date_end.format().getInfo()}")
                    
                    task = create_lst_image(city_name, date_start, date_end, city_geometry, urban_geometry, folder_name, to_drive)
                    if (to_drive):
                        is_success = check_task_status(task)
                        if is_success:
                            time.sleep(30) # wait for the last iamge to be created
                            folder_id = get_folder_id(drive,folder_name)
                            download_and_clean(drive,folder_id, save_path)
                except Exception as e:
                    logging.error(f"Error processing date: Year={year}, Month={month}")
                    logging.error(f"Error details: {str(e)}")
                    continue
                index += 1
    logging.info(f"Total number of images: {index}")

def __main__():
    load_dotenv()
    SAVE_PATH = os.getenv('SERIES_SAVE_PATH')
    FOLDER_ID = os.getenv('SERIES_FOLDER_ID')

    ee.Initialize(project='ee-channingtong')
    folder_name = 'landsat_lst_timeseries'

    create_lst_image_timeseries(FOLDER_ID,folder_name,SAVE_PATH,False)

if __name__ == '__main__':
    __main__()