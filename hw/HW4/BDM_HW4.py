#!/usr/bin/env python
# coding: utf-8

import sys
import csv
from pyspark import SparkContext
import geopandas as gpd
import rtree
import fiona
import fiona.crs
import shapely
import pyproj
import shapely.geometry as geom


def createIndex(shapefile):
    '''
    This function takes in a shapefile path, and return:
    (1) index: an R-Tree based on the geometry data in the file
    (2) zones: the original data of the shapefile
    
    Note that the ID used in the R-tree 'index' is the same as
    the order of the object in zones.
    '''
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    '''
    findZone returned the ID of the shape (stored in 'zones' with
    'index') that contains the given point 'p'. If there's no match,
    None will be returned.
    '''
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None

def processTrips(pid, records):
    '''
    Our aggregation function that iterates through records in each
    partition, checking whether we could find a zone that contain
    the pickup location.
    '''   
    # Create an R-tree index
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    n_index, n_zones = createIndex(neighborhoods_file)
    b_index, b_zones = createIndex(boroughs_file)
    
    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    
    for row in reader:
        try:
            pickup = geom.Point(proj(float(row[5]), float(row[6])))
            dropoff = geom.Point(proj(float(row[9]), float(row[10])))
        
            # Look up a matching zone, and update the count accordly if
            # such a match is found
            pickup_borough = findZone(pickup, b_index, b_zones)
            dropoff_neighborhood = findZone(dropoff, n_index, n_zones)
        except:
            continue
        
        
        if pickup_borough and dropoff_neighborhood:
            key = (pickup_borough, dropoff_neighborhood)
            counts[key] = counts.get(key, 0) + 1
    return counts.items()


if __name__ == "__main__":
    
    sc = SparkContext()
    
    neighborhoods_file = 'neighborhoods.geojson'
    boroughs_file = 'boroughs.geojson'
    
    neighborhoods = gpd.read_file(neighborhoods_file)
    boroughs = gpd.read_file(boroughs_file)
    
    taxi = sc.textFile(sys.argv[1])
    counts = taxi.filter(lambda row: len(row)>=5) \
                 .mapPartitionsWithIndex(processTrips) \
                 .reduceByKey(lambda x,y: x+y) \
                 .map(lambda x: (boroughs.boro_name[x[0][0]],(neighborhoods.neighborhood[x[0][1]],x[1]))) \
                 .groupByKey() \
                 .mapValues(list) \
                 .mapValues(lambda x: sorted(x, key=lambda x:x[1], reverse=True)[:3]) \
                 .map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1], x[1][2][0], x[1][2][1]))) \
                 .sortByKey() \
                 .saveAsTextFile(sys.argv[2])

