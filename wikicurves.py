from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.storagelevel import StorageLevel
#from pyspark.mllib import util
from time import time
from datetime import datetime, timedelta
import sys, logging
import itertools
import bisect
import functools
#import argparse
#from numpy import array

global args

#hides non-spark & non-errornous logging messages
def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )


def init_combiner(x):
  l = list()
  l.append(x)
  return l

def merge_vals(v,c):
  try:
    v.append(c)
    return v
  except Exception, e:
    print (v,c)
    raise e

def merge_combiners(v,c):
  try:
    return v+c
  except Exception, e:
    print (v,c)
    raise e

def get_partitioner(sample_rdd_kv, numPartitions, keyfunc=lambda x: x):
    # print('get_partitioner: sample_rdd_kv:', sample_rdd_kv.take(1))
    rddSize = sample_rdd_kv.count()
    print('sortByKey: rddSize: ', rddSize)

    maxSampleSize = numPartitions * 200.0  # constant from Spark's RangePartitioner
    print('sortByKey: maxSampleSize', maxSampleSize)

    fraction = min(maxSampleSize / max(rddSize, 1), 1.0)
    print('sortByKey: fraction', fraction)

    samples = sample_rdd_kv.sample(False, fraction, 1).map(lambda kv: kv[0]).collect()
    samples = sorted(samples, key=keyfunc)
    print('sortByKey: samples[0:10]', samples[0:10])

    bounds = [samples[int(len(samples) * (i + 1) / numPartitions)]
              for i in range(0, numPartitions - 1)]
    print('sortByKey: bounds', bounds)

    def rangePartitioner(k):
        p = bisect.bisect_left(bounds, keyfunc(k))
        return p

    return rangePartitioner

#main loop to rotate dataset
def process_data( ):
  conf = SparkConf()
  conf.set("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sc = SparkContext(conf=conf)
  global args
  if args.silent:
    quiet_logs(sc)
  path = args.directory

  #path = "/user/hannesm/lsde/wikistats/2014/2014-01/"
  cur = datetime(2014, 1, 1, 00, 00)
  end = datetime(2014, 1, 1, 23, 00)
  onehour = timedelta(hours=1)

  #create file names
  file_list = []
  while cur < end:
    fname = path + "pagecounts-2014" + cur.strftime("%m") + cur.strftime("%d") \
            + "-" + cur.strftime("%H") + "*.gz"
    logging.info("Adding " + fname)
    file_list.append(fname)
    cur+= onehour

  partition_num = len(file_list) * 20


  rdd_list = [
    sc.textFile(input_file)
    for input_file in file_list
  ]


  rdd_list = [
    # filter possible empty lines
    rdd.filter(lambda line: line != None)
    # split to columns
    .map(lambda line: line.split(" "))
    # filter possible corrupted lines missing columns
    .filter(lambda lst: len(lst) == 4)
    # sanity check
    .filter(lambda lst: not (None in lst))
    # create key pairs of ("key", "val1 val2")
    .map(lambda lst: ((lst[0].lower(),lst[1].lower()), int(lst[2])))
    # aggregate the same pages
    #.reduceByKey(lambda x,y: x+y)
    #.persist()
    for rdd in rdd_list
  ]

  sample_rdd = rdd_list[0]
  sample_rdd.persist(StorageLevel.MEMORY_AND_DISK)

  range_partitioner = get_partitioner(
      sample_rdd.map(lambda lst: lst[0]),  # key by proj, page
      partition_num,
  )

def init_combiner(x):
  l = list()
  l.append(x)
  return l

def merge_vals(v,c):
  try:
    v.append(c)
    return v
  except Exception, e:
    print (v,c)
    raise e

def merge_combiners(v,c):
  try:
    return v+c
  except Exception, e:
    print (v,c)
    raise e


  un_rdd = sc.union(rdd_list).partitionBy(partition_num, range_partitioner)
  grp = un_rdd.combineByKey(
              init_combiner,
              merge_vals,
              merge_combiners,
              partition_num,
              range_partitioner
                          ).mapValues(lambda val: list(val)).persist()

  #args = tuple(rdd_list[1:])
  #grp = rdd_list[0].groupWith(*args).mapValues(lambda val: [i for e in val for i in e])

  out = grp.take(400)
  for e in out:
    print (e)



def main( args, loglevel ):
  logging.basicConfig(format="%(levelname)s %(message)s", level=loglevel)
  logging.info("Starting the script")
  process_data()

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
    "-l",
    "--list",
    help="union & use list",
    action="store_true")
  parser.add_argument(
    "-t",
    "--string",
    help="union & use string concat",
    action="store_true")
  parser.add_argument(
    "-g",
    "--group",
    help="union & use group by",
    action="store_true")
  parser.add_argument(
    "-j",
    "--join",
    help="use full outer join",
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
