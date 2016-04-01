from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.storagelevel import StorageLevel
from time import time
from datetime import datetime, timedelta
import sys, logging
import itertools
import bisect
import functools
import argparse
#from numpy import array
#from pyspark.mllib import util

global args

#hides non-spark & non-errornous logging messages
def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )



def createRDD(sc, input_file):
  try:
    rdd = sc.textFile(input_file)
    return rdd
  except Exception, e:
    logging.error("Exception in creation of RDD")
    return sc.emptyRDD()
#The clusterisation using spark MLlib
def cluster():
  from pyspark.mllib.clustering import KMeans, KMeansModel
  from pyspark.mllib.linalg import Vectors
  import numpy as np

  app_name = 'wikicurves_cluster'
  conf = SparkConf()
  conf.setAppName(app_name)
  conf.set("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sc = SparkContext(conf=conf)

  parsed_data = sc.pickleFile("/user/lsde10/resamplingset/*")
  parsed_data = parsed_data.map(lambda x: np.array(x[1][1]))
  print 'Starting k-means'
  #use k=4 and 200 iterations
  clusters = KMeans.train(parsed_data, 4, 200)
  print clusters.centers
  #take 10% of our data to visualise
  sample = parsed_data.mapValues(lambda x: x[1]).sample(False, 0.1, 917)
  sample.map(lambda val: '{},{},{},{}'.format(clusters.predict(np.array(val[1])),val[0][0], val[0][1], ','.join(map(str, val[1]))))
  sample.saveAsTextFile("/user/lsde10/sample_out")


def rotate_month(path, i):
  rdd = createRDD(sc, path)
  rdd = rdd.filter(lambda line: line != None) \
    # split to columns
    .map(lambda line: line.split(" ")) \
    # filter possible corrupted lines missing columns
    .filter(lambda lst: len(lst) == 4) \
    # sanity check
    .filter(lambda lst: not (None in lst)) \
    # create key pair with key: (project, page) and value: count
    .map(lambda lst: ((lst[0],lst[1]), int(lst[2])))
  # group into (project, page) and values: [count1, count2, ...]
  grp = rdd.groupByKey(100).mapValues(lambda val: list(val))
  grp.saveAsPickleFile("/user/lsde10/"+str(i))

#main loop to rotate dataset (takes about 20 hours if all goes well!)
def rotate_data( ):
  conf = SparkConf()
  conf.set("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sc = SparkContext(conf=conf)
  global args
  if args.silent:
    quiet_logs(sc)
  #path = args.directory

  #doing [1-9] since month in the path is 01,02,03 etc
  for i in range (1,10):
    path = "/user/hannesm/lsde/wikistats/2014/2014-0"+str(i)+"/pagecounts-2014*"
    rotate_month(path, i)
  #do the rest of the months as well
  path = "/user/hannesm/lsde/wikistats/2014/2014-10/pagecounts-2014*"
  rotate_month(path, 10)
  path = "/user/hannesm/lsde/wikistats/2014/2014-11/pagecounts-2014*"
  rotate_month(path, 11)
  path = "/user/hannesm/lsde/wikistats/2014/2014-12/pagecounts-2014*"
  rotate_month(path, 12)

  rdd_list = [
    createRDD(sc, "/user/lsde10/"+str(i) +"/*")
    for i in range(1,13)
  ]
  #aggregate final dataset
  un = sc.union(rdd_list)
  fin = un.reduceByKey(lambda x,y: x+y)
  fin = fin.coalesce(200)
  fin.saveAsPickleFile("/user/lsde10/fin")

  #aggregate by month
  rdd_list = [
    createRDD(sc, "/user/lsde10/"+str(i) +"/*").mapValues(lambda lst: [sum(lst)])
    for i in range(1,13)
  ]
  un = sc.union(rdd_list)
  fin = un.reduceByKey(lambda x,y: x+y)
  fin = fin.coalesce(200)
  fin.saveAsPickleFile("/user/lsde10/aggrbymon")

  #create working dataset. Filter out small be throwing out small length vectors
  #Working with ~60% of the dataset.
  rdata = sc.pickleFile("/user/lsde10/fin/*")
  rdata = rdata.filter(lambda val: len(val[1]) > 4500)
  rdata = rdata.filter(lambda val: not (".mw" in val[0][0]))
  rdata.coalesce(100).saveAsPickleFile("/user/lsde10/workingset")

  #create the resampling set by aggregating the monthly set with the working set
  fin = sc.pickleFile("/user/lsde10/workingset/*")
  bymon = sc.pickleFile("/user/lsde10/aggrbymon/*")
  bymon = bymon.filter(lambda val: sum(val[1]) > 30 and len(val[1]) == 12)
  grp = fin.join(bymon, 100).cache()
  grp.saveAsPickleFile("/user/lsde10/resamplingset")

  cluster()

def main( args, loglevel ):
  logging.basicConfig(format="%(levelname)s %(message)s", level=loglevel)
  logging.info("Starting the script")
  rotate_data()


#std entry point
if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description = "Parses wikipedia data and clusterizes topics with \
      similar attention curves",
    epilog = "As an alternative to the commandline, params can be placed in \
      a file, one per line, and specified on the commandline like '%(prog)s \
      @params.conf'.",
    fromfile_prefix_chars = '@' )

  # Parameters specification
  parser.add_argument(
    "directory",
    help = "the path of the directory which holds the wiki dumps",
    metavar = "dir")
  parser.add_argument(
    "-v",
    "--verbose",
    help="increase output verbosity",
    action="store_true")
  parser.add_argument(
    "-s",
    "--silent",
    help="supress most logging messages",
    action="store_true")

  global args
  args = parser.parse_args()

  # Setup logging
  if args.verbose:
    loglevel = logging.DEBUG
  else:
    loglevel = logging.INFO

  main(args, loglevel)

