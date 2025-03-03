import ee
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

def create_lst_image(year,month_list,folder_name,to_drive = True):
    # Define parameters
    boundary = ee.FeatureCollection('projects/ee-channingtong/assets/YZBboundary')
    geometry = boundary.union().geometry()
    YZB_area = geometry.area().getInfo()
    print("Area of YZB: ", YZB_area)
    satellite = "L8"
    date_start = str(year) + "-01-01"
    date_end = str(year+1) + "-01-01"
    use_ndvi = True
    cloud_threshold = 20

    try:
        landsat_coll = fetch_landsat_collection(
        satellite, date_start, date_end, geometry, cloud_threshold, use_ndvi
        )
    except ValueError as e:
        print(e)

    for month in month_list:
        current_month = landsat_coll.filter(ee.Filter.calendarRange(month, month, 'month'))
        image_num = current_month.size().getInfo()
        print("total num of the month", str(month), ':', current_month.size().getInfo())
        if (image_num == 0):
            continue
        # 先选择所有波段，然后排除 TIMESTAMP
        month_average = current_month.select('LST').mean().clip(geometry)
        
        #if (month_average.bounds().area().getInfo() < 0.9 * YZB_area):
        #    continue

        image_data = {
            'geometry': geometry,
            'image': month_average
        }
        map_name = "landsat-" + str(year) + '-' + str(month)
        task = None
        if to_drive:
            task = ee.batch.Export.image.toDrive(image=month_average,
                                        description=map_name,
                                        folder=f'{folder_name}',
                                        scale=30,
                                        crs='EPSG:4326',
                                        region=geometry,
                                        fileFormat='GeoTIFF',
                                        maxPixels=1e13)
            task.start()
        else:
            show_map(None, image_data, map_name,'LST')
        return task

def __main__():
    ee.Initialize(project='ee-channingtong')
    year = 2022
    month_list = [10] # for test
    folder_name = 'landsat_lst_timeseries'
    create_lst_image(year,month_list,folder_name,False)

if __name__ == '__main__':
    __main__()