from concurrent.futures import ThreadPoolExecutor, as_completed
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from landsat_lst_image import export_lst_image, filter_city_bound, create_lst_image
from dotenv import load_dotenv
from parse_record import parse_record
import os
import ee
import logging
import csv
import json

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
    monitor_file_path = os.getenv('PROCESS_MONITOR_FILE_PATH')
    header = ['city', 'year', 'month', 'toa_image_porpotion', 'sr_image_porpotion', 'toa_cloud_ratio', 'sr_cloud_ratio', 'day']
    with open(monitor_file_path, 'w', newline='') as f:
        pass
    if (not os.path.exists(record_file_path)):
        with open(record_file_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)

def authenticate_google_drive():
    gauth = GoogleAuth()
    credentials_path = os.getenv('CREDENTIALS_FILE_PATH')
    
    try:
        # Try to load saved client credentials
        gauth.LoadCredentialsFile(credentials_path)
        
        if gauth.credentials and gauth.credentials.refresh_token:
            try:
                # Try to refresh token
                gauth.Refresh()
                gauth.SaveCredentialsFile(credentials_path)
            except Exception as e:
                logging.warning(f"Token refresh failed: {e}")
                # Clear invalid credentials
                gauth.credentials = None
                # Delete the invalid credentials file
                if os.path.exists(credentials_path):
                    logging.info("Removed invalid credentials file")
        
        # If no valid credentials, do full authentication
        if not gauth.credentials:
            logging.info("Starting new authentication flow...")
            # Configure local server auth settings
            gauth.GetFlow()
            gauth.flow.params.update({'access_type': 'offline'})
            gauth.flow.params.update({'approval_prompt': 'force'})
            
            # Run local server auth flow
            gauth.LocalWebserverAuth()
            
            # Save new credentials
            gauth.SaveCredentialsFile(credentials_path)
            logging.info("New credentials saved successfully")
        
        # Create and test drive connection
        drive = GoogleDrive(gauth)
        all_files = drive.ListFile({'q': "trashed=false"}).GetList()
        logging.info(f"Successfully authenticated. Total files accessible: {len(all_files)}")
        
        return gauth, drive
        
    except Exception as e:
        logging.error(f"Authentication failed: {e}")
        # If any error occurs, try one final time with fresh authentication
        try:
            logging.info("Attempting fresh authentication...")
            
            gauth = GoogleAuth()
            gauth.GetFlow()
            gauth.flow.params.update({'access_type': 'offline'})
            gauth.flow.params.update({'approval_prompt': 'force'})
            gauth.LocalWebserverAuth()
            gauth.SaveCredentialsFile(credentials_path)
            
            drive = GoogleDrive(gauth)
            return gauth, drive
        except Exception as final_e:
            logging.error(f"Final authentication attempt failed: {final_e}")
            raise

def create_lst_image_timeseries(folder_name,save_path,to_drive = True):
    asset_path = 'projects/ee-channingtong/assets/'
    total_boundary = ee.FeatureCollection(asset_path + 'YZBboundary')
    #total_geometry = total_boundary.union().geometry()
    if (to_drive):
        gauth, drive = authenticate_google_drive()
        if gauth.credentials.refresh_token is None:
            logging.error('No refresh token available. Please re-authenticate.')
            return
        
        logging.info(f"Token expires in: {gauth.credentials.token_expiry}")
        
        #check permissions
        drive = GoogleDrive(gauth)
        all_files = drive.ListFile({'q': "trashed=false"}).GetList()
        logging.info(f"total files: {len(all_files)}")

        logging.info(f"token current expires in: {gauth.credentials.token_expiry}")
        if gauth.credentials.refresh_token is None:
            logging.warning('refresh token is None')
            return
    index = 0
    for city_boundary in total_boundary.getInfo()['features']:
        index += 1
        logging.info(f'Processing city id: {index}')
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
        year_list = range(1985,2025)
        for year in year_list:
            month_list = range(1,13)
            if (to_drive):
                with ThreadPoolExecutor(max_workers=5) as executor:
                    task_states = [executor.submit(
                        export_lst_image, 
                        gauth = gauth,
                        city_name = city_name, 
                        year = year,month = month,
                        city_geometry = city_geometry, urban_geometry = urban_geometry, 
                        folder_name = folder_name, to_drive = to_drive,
                        drive = drive, save_path = save_path
                    ) for month in month_list]
                    exported_months = [month for month in as_completed(task_states) if month is not None]
                    logging.info(f"{city_name} {year} exported months: {exported_months}")
            else:
                with ThreadPoolExecutor(max_workers=9) as executor:

                    finish_states = [executor.submit(
                        create_lst_image, city_name = city_name, 
                        year = year,month = month,
                        city_geometry = city_geometry, urban_geometry = urban_geometry, 
                        folder_name = folder_name, to_drive = to_drive
                    ) for month in month_list]
                    exported_months = [month for month in as_completed(finish_states) if month is not None]
                    logging.info(f"{city_name} {year} exported months: {exported_months}")
    logging.info("All done. >_<")
    parse_record(os.getenv('RECORD_FILE_PATH'))

def create_lst_image_timeseries_with_record(folder_name,save_path,record_file_path):
    asset_path = 'projects/ee-channingtong/assets/'
    total_boundary = ee.FeatureCollection(asset_path + 'YZBboundary')
    record = json.load(open(record_file_path))
    
    # Use the new authentication function
    gauth, drive = authenticate_google_drive()
    
    if gauth.credentials.refresh_token is None:
        logging.error('No refresh token available. Please re-authenticate.')
        return
        
    logging.info(f"Token expires in: {gauth.credentials.token_expiry}")
    
    for city_name, city_record in record.items():
        # Every few iterations, check if we need to refresh the token
        try:
            if gauth.credentials.access_token_expired:
                logging.info("Access token expired, refreshing...")
                gauth, drive = authenticate_google_drive()
                
        except Exception as e:
            logging.error(f"Error refreshing token: {e}")
            # Re-authenticate completely
            gauth, drive = authenticate_google_drive()
            
        city_boundary = total_boundary.filter(ee.Filter.eq('市名', city_name)).first().getInfo()
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
        with ThreadPoolExecutor(max_workers=5) as executor:
                task_states = [executor.submit(
                    export_lst_image, 
                    gauth = gauth,
                    city_name = city_name, 
                    year = int(date['year']),month = int(date['month']),
                    city_geometry = city_geometry, urban_geometry = urban_geometry, 
                    folder_name = folder_name, to_drive = True,
                    drive = drive, save_path = save_path
                ) for date in city_record]
        logging.info(f"{city_name} exported")

def __main__():
    load_dotenv()
    SAVE_PATH = os.getenv('IMAGE_SAVE_PATH')
    project_name = os.getenv('PROJECT_NAME')
    ee.Initialize(project=project_name)

    folder_name = 'landsat_lst_timeseries'
    init_record_file()

    #create_lst_image_timeseries(folder_name,SAVE_PATH,False)
    create_lst_image_timeseries_with_record(folder_name,SAVE_PATH,"LST_download_list.json")
    
if __name__ == '__main__':
    __main__()