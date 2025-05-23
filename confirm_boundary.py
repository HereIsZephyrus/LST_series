import ee
import pprint

def __main__():
    ee.Initialize(project='ee-channingtong')
    boundary = ee.FeatureCollection('projects/ee-channingtong/assets/YZBboundary')
    index = 0
    for feature in boundary.getInfo()['features']:
        pprint.pprint(feature['properties'])
        urban_name = f'urban_{feature["properties"]['市代码']}'
        print(urban_name)
        index += 1
    print("Total number of cities: ", index)
        #geometry = feature['geometry']
        #coordinates = geometry['coordinates']
        #print(coordinates)

if __name__ == '__main__':
    __main__()