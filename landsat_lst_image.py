import ee
import folium
from ee_lst.landsat_lst import fetch_best_landsat_image
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

def create_lst_image(city_name,date_start,date_end,city_geometry,urban_geometry,folder_name,to_drive):
    # Define parameters
    satellite_list = ['L8', 'L7', 'L5', 'L4']
    use_ndvi = True
    cloud_threshold = 20
   
    landsat_coll = None
    for satellite in satellite_list:
        try:
            landsat_coll_sat = fetch_best_landsat_image(
            satellite, date_start, date_end, city_geometry, cloud_threshold, urban_geometry, use_ndvi
            )
            landsat_coll = landsat_coll_sat
            print(f"success: {satellite}")
            break
        except ValueError as e:
            print(f"no data for {satellite}")
            continue
    
    if landsat_coll is None:
        print("No Landsat data found")
        return None

    image_data = {
        'geometry': city_geometry,
        'image': landsat_coll
    }
    map_name = f'landsat_{city_name}_{date_start}_{date_end}'
    task = None
    if to_drive:
        task = ee.batch.Export.image.toDrive(image=landsat_coll,
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