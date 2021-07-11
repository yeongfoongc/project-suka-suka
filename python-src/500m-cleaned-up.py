## follow requirements.txt to get all dependency installed

import json
import geojson
import requests
import re
import os
import numpy as np
import dask.array as da
import dask.dataframe as dd
from shapely.geometry import mapping, shape, MultiPoint
from shapely.ops import cascaded_union
import time
import shutil
import wget

''' FIXME: causing problems in local run
from dask.distributed import Client, progress
client = Client()
client
'''

SOURCE_DIR = "./input_files/"
try:
    os.mkdir(SOURCE_DIR)
    print("created {} directory".format(SOURCE_DIR))
except OSError as error:
    print("directory 'input_files' present")

SOURCE_FILE = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/malaysia.geojson"
wget.download(SOURCE_FILE, SOURCE_DIR)

SOURCE_FILENAME = SOURCE_FILE.split("/")[-1]
print(SOURCE_FILENAME)

'''
shutil.move(SOURCE_FILENAME, SOURCE_DIR+SOURCE_FILENAME)
'''

with open(SOURCE_DIR+'malaysia.geojson') as fopen:
    malaysia = json.load(fopen)

negeri = {k['properties']['name']:k['geometry'] for k in malaysia['features']}
negeri.keys()


# data from TindakMalaysia is more precise for these states

download = {
    'Kedah': 'https://raw.githubusercontent.com/TindakMalaysia/Kedah-Maps/master/2016/MAP/MIGRATED/result/02-Kedah-New-DM-4326.geojson',
    'Pahang': 'https://raw.githubusercontent.com/TindakMalaysia/Pahang-Maps/master/2016/MAP/MIGRATED/result/07-Pahang-New-DM-4326.geojson',
    'Johor': 'https://raw.githubusercontent.com/TindakMalaysia/Johor-Maps/master/2016/MAP/MIGRATED/result/13-Johor-New-DM-4326.geojson',
    'Perak': 'https://raw.githubusercontent.com/TindakMalaysia/Perak-Maps/master/2016/MAP/MIGRATED/result/06-Perak-New-DM-4326.geojson',
    'Labuan': 'https://raw.githubusercontent.com/TindakMalaysia/Federal-Territories-Maps/master/LABUAN/2016/MAP/MIGRATED/result/14-Labuan-New-DM-4326.geojson',
    'Terengganu': 'https://raw.githubusercontent.com/TindakMalaysia/Terengganu-Maps/master/2016/MAP/MIGRATED/result/04-Terengganu-New-DM-4326.geojson'
}

for k, v in download.items():
    wget.download(v, SOURCE_DIR)
    '''
    shutil.move(v, SOURCE_DIR)
    '''

def check(df):
    points = MultiPoint(df.values)
    try:
        ls = list(points.intersection(polygon))
        return [(l.x, l.y) for l in ls]
    except Exception as e:
        return []

DATA_PLOTS_DIR="./data-plots/"
try:
    os.mkdir(DATA_PLOTS_DIR)
    print("created {} directory".format(DATA_PLOTS_DIR))
except OSError as error:
    print("directory {} present".format(DATA_PLOTS_DIR))

# meters = 1000
# earth = 6378.137
# pi = np.pi
# cos = np.cos
# m = (1 / ((2 * pi / 360) * earth)) / 1000
# default_resolution = meters * m
# default_resolution
# default_resolution = 0.003

default_resolution = 0.0006
resolutions = {'Sabah': 0.005, 'Sarawak': 0.006}

## FIXME: bring this back after testing is good
## negeris = list(negeri.keys()) + ['Labuan']
## FIXME: use this for simple testing
negeris = ["Penang", "Perlis"]

for k in negeris:

    start = time.time()
    print(k)
    if k in download:
        print(download[k].split('/')[-1])
        with open(download[k].split('/')[-1]) as fopen:
            data = json.load(fopen)

        state = []
        for d in data['features']:
            d = d['geometry']
            g = geojson.loads(json.dumps(d))
            polygon = shape(g)
            state.append(polygon)
        polygon = cascaded_union(state)
    else:
        s = json.dumps(negeri[k])
        print("loading geojson")
        g1 = geojson.loads(s)
        print("shaping polygon")
        polygon = shape(g1)
    latmin, lonmin, latmax, lonmax = polygon.bounds 
    resolution = resolutions.get(k, default_resolution)
    lat_arange = da.arange(latmin, latmax, resolution)
    lon_arange = da.arange(lonmin, lonmax, resolution)
    x, y = da.meshgrid(lat_arange, lon_arange)
    x = da.round(x, 8)
    y = da.round(y, 8)
    x = da.reshape(x, (-1, 1))
    y = da.reshape(y, (-1, 1))
    print("Done reshapping")

    concated = da.concatenate([x, y], axis = 1)
    df = dd.from_array(concated)
    df = df.repartition(npartitions = 100)
    print("Done repartitions")

    check_partitions = df.map_partitions(check, meta=object)
    check_partitions = check_partitions.compute()
    inside = []
    for p in check_partitions:
        inside.extend(p)

    print(k, len(inside))
    with open(f'{DATA_PLOTS_DIR}/{k}-points.json', 'w') as fopen:
        json.dump(inside, fopen)

    end = time.time()
    print(f"Runtime is {end - start} seconds...")

print("Done")