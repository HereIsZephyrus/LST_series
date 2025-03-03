import ee
import ee.data
import folium
from ee_lst.landsat_lst import fetch_landsat_collection
import altair as alt
import eerepr
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from mpl_toolkits.axes_grid1 import ImageGrid

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

def create_lst_image(city_name,data_range,city_geometry,urban_geometry,folder_name,to_drive = True):
    # Define parameters
    satellite_list = ["L4", "L5", "L7", "L8"]
    use_ndvi = True
    cloud_threshold = 10
   
    landsat_coll = []
    for satellite in satellite_list:
        landsat_coll_sat = fetch_landsat_collection(
        satellite, data_range, city_geometry, cloud_threshold, urban_geometry, use_ndvi
        )
        landsat_coll.merge(landsat_coll_sat)
    
    image_num = landsat_coll.size().getInfo()
    if image_num == 0:
        print("No Landsat data found")
        return None
    
    print("total num of the month : ", image_num)

    month_average = landsat_coll.select('LST').mean().clip(city_geometry)

    image_data = {
        'geometry': city_geometry,
        'image': month_average
    }
    map_name = f'landsat_{city_name}_{date_start}_{date_end}'
    task = None
    if to_drive:
        task = ee.batch.Export.image.toDrive(image=month_average,
                                    description=map_name,
                                    folder=f'{folder_name}',
                                    scale=30,
                                    crs='EPSG:4326',
                                    region=city_geometry,
                                    fileFormat='GeoTIFF',
                                    maxPixels=1e13)
        task.start()
    else:
        show_map(None, image_data, map_name,'LST')
    return task
    
def create_lst_image_timeseries(folder_name,to_drive = True):
    asset_path = 'projects/ee-channingtong/assets/'
    total_boundary = ee.FeatureCollection(asset_path + 'YZBboundary')
    total_geometry = total_boundary.union().geometry()
    total_area = total_geometry.area().getInfo()
    print("Area of YZB: ", total_area) 
    index = 0
    for city_boundary in total_boundary.getInfo()['features']:
        city_code = city_boundary['properties']['市代码']
        city_geometry = city_boundary['geometry']
        asset_name = f'urban_{city_code}'
        urban_boundary = ee.FeatureCollection(asset_path + asset_name)
        urban_geometry = urban_boundary.union().geometry()
        city_name = urban_boundary.getInfo()['features'][0]['properties']['city_name']
        urban_area = urban_geometry.area().getInfo()
        print("Area of ", city_name, ": ", urban_area)
        year_list = range(1984,2024)
        year_list = [2022] # for test
        for year in year_list:
            month_list = range(1,13)
            month_list = [10] # for test
            for month in month_list:
                date_start = f'{year}-{month}-01'
                date_end = f'{year}-{month}-31'
                data_range = ee.DateRange(date_start, date_end)
                print("Processing ", date_start, 'to', date_end)
                create_lst_image(city_name,data_range,city_geometry,urban_geometry,folder_name,to_drive)
                index += 1
    print("Total number of images: ", index)
    if (index > 1):
        return # for test
def __main__():
    ee.Initialize(project='ee-channingtong')
    folder_name = 'landsat_lst_timeseries'
    create_lst_image_timeseries(folder_name,False)

if __name__ == '__main__':
    __main__()