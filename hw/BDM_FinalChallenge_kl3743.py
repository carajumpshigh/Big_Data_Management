#!/usr/bin/env python
# coding: utf-8

# import pandas as pd
import numpy as np

import csv
import sys

import time
from datetime import timedelta

from pyspark import SparkContext
from pyspark.sql.session import SparkSession


def process_hn(house_num):
    
    # check if the house number is compound
    if '-' in house_num:
        # convert compound house number into a tuple of int
        return tuple(map(int, house_num.split('-')))
    else:
        # convert single house number into a tuple (0, int house number)
        return (0,int(house_num))


def process_ctl(pid, records):
    
    # find the indexs of useful columns
    id_idx = 0 #row.index('PHYSICALID')
    str1_idx = 28 #row.index('FULL_STREE')
    str2_idx = 10 #row.index('ST_LABEL')
    boro_idx = 13 #row.index('BOROCODE')
    l_low_idx = 2 #row.index('L_LOW_HN')
    l_high_idx = 3 #row.index('L_HIGH_HN')
    r_low_idx = 4 #row.index('R_LOW_HN')
    r_high_idx = 5 #row.index('R_HIGH_HN')
    
    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    
    for row in reader:
        try:
            # yield PHYSICALID, boro_code, FULL_STREE, ST_LABEL, 
            #       house_number1_low, house_number1_high, 
            #       house_number2_low, house_number2_high,
                
            # for odd house number range(1)
            yield (int(row[id_idx]), int(row[boro_idx]), row[str1_idx].upper(), row[str2_idx].upper(), \
            	   process_hn(row[l_low_idx])[0], process_hn(row[l_high_idx])[0], \
                   process_hn(row[l_low_idx])[1], process_hn(row[l_high_idx])[1], \
            	   1)
                
            # for even house number range(0)
            yield (int(row[id_idx]), int(row[boro_idx]), row[str1_idx].upper(), row[str2_idx].upper(), \
                   process_hn(row[r_low_idx])[0], process_hn(row[r_high_idx])[0], \
                   process_hn(row[r_low_idx])[1], process_hn(row[r_high_idx])[1], \
                   0)
                
        except:
            yield (int(row[id_idx]), -1, '', '', -1, -1, -1, -1, -1)


def process_vio(pid, records):
    
    county_to_boro = {'MAN':1,'MH':1,'MN':1,'NEWY':1,'NEW Y':1,'NY':1, \
    				  'BRONX':2,'BX':2,'PBX':2, \
    				  'BK':3,'K':3,'KING':3,'KINGS':3, \
    				  'Q':4,'QN':4,'QNS':4,'QU':4,'QUEEN':4, \
    				  'R':5,'RICHMOND':5}
    
    # find the indexs of useful columns
    hn_idx = 23 #row.index('House Number')
    str_idx = 24 #row.index('Street Name')
    county_idx = 21 #row.index('Violation County')
    date_idx = 4 #row.index('Issue Date')

     # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    
    for row in reader:
        try:
            if 2014 < int(row[date_idx][-4:]) < 2020:
                if row[county_idx] in county_to_boro.keys():
                    # yield issue_year(2015-2019), boro_code, street_name, 
                    #       house_number1, house_number2, if_house_number_even
                    yield (int(row[date_idx][-4:]), county_to_boro[row[county_idx]], row[str_idx].upper(), \
                    	   process_hn(row[hn_idx])[0], process_hn(row[hn_idx])[1],  process_hn(row[hn_idx])[1]%2)
        except:
            continue


def formatting(records):
    for row in records:
        counts = [0,0,0,0,0]
        if row[0][1] >= 0:
            counts[row[0][1]] = row[1]
            yield (row[0][0],counts)
        else:
            yield (row[0][0],counts)


def ols(counts):
    X = np.array(list(range(2015,2020)))
    Y = np.array(counts)
    return (np.sum(Y*X) - 5*np.mean(Y)*np.mean(X)) / (np.sum(X*X) - 5*np.mean(X)*np.mean(X))


if __name__ == "__main__":

    start_time = time.monotonic()
    centerline_path = 'hdfs:///tmp/bdm/nyc_cscl.csv'
    violations_path = 'hdfs:///tmp/bdm/nyc_parking_violation/'
    output = sys.argv[1]

    sc = SparkContext()
    spark = SparkSession(sc)

    centerline = sc.textFile(centerline_path).mapPartitionsWithIndex(process_ctl)
    violations = sc.textFile(violations_path).mapPartitionsWithIndex(process_vio)
    
    ctl = spark.createDataFrame(centerline, ('PHYSICALID', 'boro', 'street1', 'street2', \
    										 'low1', 'high1', 'low2', 'high2', 'if_even'))
    
    vio = spark.createDataFrame(violations, ('year', 'boro', 'street', 'house_number1', \
                                             'house_number2', 'if_even'))
    
    condition = [vio.boro == ctl.boro, 
                 (vio.street == ctl.street1) | (vio.street == ctl.street2), 
                 vio.if_even == ctl.if_even, 
                 (vio.house_number1 >= ctl.low1) & (vio.house_number1 <= ctl.high1),
                 (vio.house_number2 >= ctl.low2) & (vio.house_number2 <= ctl.high2)]

    df = ctl.join(vio, condition, how='left') \
    		.groupBy([ctl.PHYSICALID, vio.year]).count() \
    		.na.fill(0)
    
    df.rdd.map(lambda x: ((x[0], x[1]-2015), x[2])) \
    	  .mapPartitions(formatting) \
    	  .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4])) \
    	  .mapValues(lambda x: (x,ols(x))) \
    	  .sortByKey() \
    	  .map(lambda x: (x[0],x[1][0][0],x[1][0][1],x[1][0][2],x[1][0][3],x[1][0][4],x[1][1])) \
    	  .saveAsTextFile(output)
    
    end_time = time.monotonic()
    print('Total Run Time :', timedelta(seconds = end_time - start_time))

