import ee
import os
import folium
import time
import logging
import traceback
import multiprocessing as mp
import csv
from dotenv import load_dotenv
from pypinyin import lazy_pinyin as pinyin
from fetch_drive import download_and_clean, check_task_status
from fetch_drive import get_folder_id_by_name as get_folder_id
from ee_lst.landsat_lst import fetch_best_landsat_image

# Define a method to display Earth Engine image tiles
def add_ee_layer(self, ee_image_object, vis_params, name):
    map_id_dict = ee.Image(ee_image_object).getMapId(vis_params)
    folium.raster_layers.TileLayer(
        tiles=map_id_dict["tile_fetcher"].url_format,
        attr="Map Data &copy; Google Earth Engine",
        name=name,
        overlay=True,
        control=True,
    ).add_to(self)

def show_map(self, map_data, map_name, type = 'LST'):
    # Visualization parameters
    cmap1 = ["blue", "cyan", "green", "yellow", "red"]
    cmap2 = ["F2F2F2", "EFC2B3", "ECB176", "E9BD3A", "E6E600", "63C600", "00A600"]

    # Add EE drawing method to folium
    folium.Map.add_ee_layer = add_ee_layer

    # Create a folium map object
    geometry = map_data['geometry']
    feature_image = map_data['image']
    centerXY = geometry.centroid().getInfo()['coordinates']
    center = [centerXY[1], centerXY[0]]
    map_render = folium.Map(center, zoom_start=10, height=500)

    # Add the Earth Engine layers to the folium map
    if (type == 'TPW'):
        map_render.add_ee_layer(feature_image.select("TPW"), {"min": 0, "max": 60, "palette": cmap1}, "TCWV")
    elif (type == 'TPWpos'):
        map_render.add_ee_layer(feature_image.select("TPWpos"), {"min": 0, "max": 9, "palette": cmap1}, "TCWVpos")
    elif (type == 'FVC'):
        map_render.add_ee_layer(feature_image.select("FVC"), {"min": 0, "max": 1, "palette": cmap2}, "FVC")
    elif (type == 'EM'):
        map_render.add_ee_layer(feature_image.select("EM"), {"min": 0.9, "max": 1.0, "palette": cmap1}, "Emissivity")
    elif (type == 'B10'):
        map_render.add_ee_layer(feature_image.select("B10"), {"min": 290, "max": 320, "palette": cmap1}, "TIR BT")
    elif (type == 'LST'):
        map_render.add_ee_layer(feature_image.select("LST"), {"min": 290, "max": 320, "palette": cmap1}, "LST")

    ## add geometry boundary
    folium.GeoJson(
        geometry.getInfo(),
        name='Geometry',
    ).add_to(map_render)
    # Display the map
    map_render.save(map_name + '.html')

def filter_city_bound(city_geometry):
    """
    city geometry buffer has many scatters. select the largest polygon as the main urban area
    """
    if (city_geometry.type().getInfo() == 'Polygon'):
        return city_geometry
    geometry_num = city_geometry.geometries().length().getInfo()
    logging.debug(f"geometry_num: {geometry_num}")
    largest = None
    max_area = 0
    for i in range(0,geometry_num):
        polygon = ee.Geometry.Polygon(city_geometry.coordinates().get(i))
        area = polygon.area().getInfo()
        if area > max_area:
            max_area = area
            largest = ee.Geometry(polygon)
    logging.debug(f"max area is {max_area}")
    return largest

def create_lst_image(city_name,year,month,city_geometry,urban_geometry,folder_name,to_drive):
    # Define parameters
    month_length = [31,28,31,30,31,30,31,31,30,31,30,31]
    satellite_list = ['L8', 'L5', 'L7', 'L4']
    date_start = ee.Date.fromYMD(year, month, 1)
    date_end = ee.Date.fromYMD(year, month, month_length[month-1]).advance(1, 'day')
    use_ndvi = True
    cloud_threshold = 25
   
    load_dotenv()
    record_file_path = os.getenv('RECORD_FILE_PATH') # csv

    landsat_coll = None
    map_name = f'landsat_{city_name}'
    for satellite in satellite_list:
        try:
            landsat_coll, toa_porpotion, sr_porpotion, toa_cloud, sr_cloud = fetch_best_landsat_image(satellite, date_start, date_end, city_geometry, cloud_threshold, urban_geometry, use_ndvi)
            with open(record_file_path, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([city_name, year, month, toa_porpotion, sr_porpotion, toa_cloud, sr_cloud])
            logging.info(f"success: {satellite}")
            map_name = f'{map_name}_{satellite}_{year}_{month}'
            break
        except ValueError as ve:
            logging.info(f"no data for {satellite}({ve})")
            continue
        except Exception as e:
            logging.error(f"fetch error: {e}\n traceback: {traceback.format_exc()}")
            continue
    
    if landsat_coll is None:
        logging.error("No Landsat data found")
        return None

    image_data = {
        'geometry': city_geometry,
        'image': landsat_coll
    }
    if to_drive:
        e_city_name = ''.join(pinyin(city_name))
        descrption = f"{e_city_name}Landsat{year}{month:02}"
        try:
            task = ee.batch.Export.image.toDrive(image=landsat_coll,
                                    description=descrption,
                                    folder=f'{folder_name}',
                                    scale=30,
                                    crs='EPSG:4326',
                                    region=city_geometry,
                                    fileFormat='GeoTIFF',
                                    maxPixels=1e13)
            task.start()
            return task, descrption
        except Exception as e:
            logging.error(f"error to export: {e}\n traceback: {traceback.format_exc()}")
            return None
    else:
        try:
            show_map(None, image_data, map_name,'LST')
            logging.info(f"image saved to {map_name}.html")
            return month
        except Exception as e:
            logging.error(f"error: {e}\n traceback: {traceback.format_exc()}")
    return None

def monitor_export_task(task,file_name,drive,folder_name,save_path):
    task_identifier = file_name
    is_success = check_task_status(task,task_identifier)
    if is_success:
        time.sleep(30) # wait for the last iamge to be created
        folder_id = get_folder_id(drive,folder_name)
        try:
            download_and_clean(drive, folder_id, file_name, save_path)
        except Exception as e:
            logging.error(f'{file_name} failed to download({e})')
        logging.info(f'{file_name} exported')
    return

def export_lst_image(city_name,year,month,city_geometry,urban_geometry,folder_name,to_drive,drive, save_path):
    """
    export the lst image to the drive
    """
    try:
        try:
            task, file_name = create_lst_image(city_name,year,month,city_geometry,urban_geometry,folder_name,to_drive)
        except Exception as e:
            logging.error(f"error to create task: {e}")
            return None
        logging.info(f'start export {city_name} {year} {month}')
        if (to_drive):
            process = mp.Process(target=monitor_export_task, args=(task,file_name,drive,folder_name,save_path))
            process.start()
            print("Process PID: ", process.pid)
            logging.info(f'{city_name} {year} {month} export PID is {process.pid}')
        return month
    except Exception as e:
        logging.warning(f'{city_name} {year} {month} failed')
        return None
