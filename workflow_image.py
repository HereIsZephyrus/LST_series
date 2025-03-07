from concurrent.futures import ThreadPoolExecutor, as_completed
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from landsat_lst_image import export_lst_image, filter_city_bound, create_lst_image
from dotenv import load_dotenv
import os
import ee
import logging
import csv

logging.basicConfig(
    filename='workflow_image.log',
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logging.getLogger('ee').setLevel(logging.WARNING)

def init_record_file():
    record_file_path = os.getenv('RECORD_FILE_PATH') # csv
    header = ['city', 'year', 'month', 'toa_image_porpotion', 'sr_image_porpotion', 'toa_cloud_ratio', 'sr_cloud_ratio']
    with open(record_file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)

def create_lst_image_timeseries(folder_name,save_path,to_drive = True):
    asset_path = 'projects/ee-channingtong/assets/'
    total_boundary = ee.FeatureCollection(asset_path + 'YZBboundary')
    #total_geometry = total_boundary.union().geometry()
    if (to_drive):
        gauth = GoogleAuth()
        gauth.LocalWebserverAuth()  # 首次运行需要浏览器授权
        drive = GoogleDrive(gauth)
    index = 0
    for city_boundary in total_boundary.getInfo()['features']:
        index += 1
        print(f'Processing city id: {index}')
        city_name = city_boundary['properties']['市名']
        if (city_name != '武汉市'): # priority for Wuhan
            continue
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
        year_list = [2020] # for test
        for year in year_list:
            month_list = range(1,13)
            if (to_drive):
                with ThreadPoolExecutor() as executor:
                    task_states = [executor.submit(
                        export_lst_image, city_name = city_name, 
                        year = year,month = month,
                        city_geometry = city_geometry, urban_geometry = urban_geometry, 
                        folder_name = folder_name, to_drive = to_drive,
                        drive = drive, save_path = save_path
                    ) for month in month_list]
                    exported_months = [month for month in as_completed(task_states) if month is not None]
                    logging.info(f"{city_name} {year} exported months: {exported_months}")
            else:
                with ThreadPoolExecutor() as executor:
                    finish_states = [executor.submit(
                        create_lst_image, city_name = city_name, 
                        year = year,month = month,
                        city_geometry = city_geometry, urban_geometry = urban_geometry, 
                        folder_name = folder_name, to_drive = to_drive
                    ) for month in month_list]
                    exported_months = [month for month in as_completed(finish_states) if month is not None]
                    logging.info(f"{city_name} {year} exported months: {exported_months}")

def __main__():
    load_dotenv()
    SAVE_PATH = os.getenv('IMAGE_SAVE_PATH')

    ee.Initialize(project='ee-channingtong')
    folder_name = 'landsat_lst_timeseries'
    init_record_file()

    create_lst_image_timeseries(folder_name,SAVE_PATH,True)

if __name__ == '__main__':
    __main__()