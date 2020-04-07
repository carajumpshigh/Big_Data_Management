from pyspark import SparkContext
import sys
import csv


if __name__=='__main__':
    
    sc = SparkContext()

    input = sys.argv[1]
    output_folder = sys.argv[2]

    cpl = sc.textFile(input, use_unicode=True).cache()

    def mapper(partitionId, records):
        if partitionId == 0:
            next(records)
            reader = csv.reader(records,delimiter=',',quotechar='"')
            for r in reader:
                if len(r)>7 and type(r[0])==str and len(r[0])==10 and r[0][0]==('2'or'1'):
                    yield r[0][0:4], \
                        r[1].lower().replace('"', "'") if ',' in r[1] else r[1].lower(), \
                        r[7].lower()
                    

    cpl.mapPartitionsWithIndex(mapper) \
        .map(lambda x: ((x[1],x[0],x[2]),1)) \
        .reduceByKey(lambda x,y: x+y) \
        .map(lambda x: ((x[0][0],x[0][1]),(x[1],1,x[1]))) \
        .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], max(x[2],y[2]))) \
        .mapValues(lambda x: (x[0],x[1], round(x[2]/x[0]*100))) \
        .sortByKey() \
        .saveAsTextFile(output_folder)
