from pyspark import SparkContext
from pyspark.conf import SparkConf
#from pyspark.mllib import util
from time import time
from datetime import datetime, timedelta
import sys, argparse, logging
#from numpy import array

global args

#hides non-spark & non-errornous logging messages
def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

#main loop to do things for everyfile
def process_data( ):
  sc = SparkContext("local", "wikicurves")
  if args.silent:
    quiet_logs(sc)
  global args
  path = args.directory
  cur = datetime(2014, 1, 1, 00, 00)
  end = datetime(2014, 1, 5, 23, 00)
  onehour = timedelta(hours=1)
  while cur < end:
    fname = path + "pagecounts-2014" + cur.strftime("%m") + cur.strftime("%d") \
            + "-" + cur.strftime("%H") + "*.gz"
    logging.info("Working on " + fname)
    #raw_data = sc.textFile(fname)
    #sep = raw_data.map(lambda line: line.split(" "))
    #sep = sep.map(lambda vals: (vals[1], vals[2]))
    #sep = sep.filter(lambda val: val[0][0] != '%')
    #car = sep.cartesian(sep).filter(lambda pair: pair[0] != pair[1])
    #print(car.take(10))
    return #debug
    cur+= onehour


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


def test_case():
  conf = SparkConf()
  #speeds up the decompression a bit
  conf.set("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec")
  sc = SparkContext(conf=conf)

  path = "/user/hannesm/lsde/wikistats/2014/2014-01/"
  fname1 = path + "pagecounts-20140101-000000.gz" #~180mb
  fname2 = path + "pagecounts-20140101-010000.gz" #~180mb
  rdata1 = sc.textFile(fname1)
  rdata2 = sc.textFile(fname2)

  global args

  use_lists = args.list # ~ 8 minutes
  use_str = args.string # ~ 7 minutes
  use_grp = args.group # ~ 6 minutes
  use_join = args.join # ~ 20 minutes

  print use_lists , use_str, use_grp, use_join

  # filter possible empty lines
  mp1 = rdata1.filter(lambda line: line != None)
  # split to columns
  mp1 = mp1.map(lambda line: line.split(" "))
  # filter possible corrupted lines missing columns
  mp1 = mp1.filter(lambda lst: len(lst) == 4)
  # sanity check
  mp1 = mp1.filter(lambda lst: not (None in lst))

  if use_lists:
    # create tuple: (key, val) with
    # "project_name page_name" as key and count as val
    mp1 = mp1.map(lambda lst: (lst[0]+ " " + lst[1], int(lst[2])))
  else:
    mp1 = mp1.map(lambda lst: (lst[0]+ " " + lst[1], (lst[2])))


  # same exact code as above
  mp2 = rdata2.filter(lambda line: line != None)
  mp2 = mp2.map(lambda line: line.split(" "))
  mp2 = mp2.filter(lambda lst: len(lst) == 4)
  mp2 = mp2.filter(lambda lst: not (None in lst))
  if use_lists:
    mp2 = mp2.map(lambda lst: (lst[0]+ " " + lst[1], int(lst[2])))
  else:
    mp2 = mp2.map(lambda lst: (lst[0]+ " " + lst[1], (lst[2])))

  grp = None
  if use_join:
    #create key pairs of ("key", (val1, val2)) also keeping track of interpolation
    grp = mp1.fullOuterJoin(mp2, 4)
  else:
    # create a union of the two rdds
    grp = mp1.union(mp2)
    if use_lists:
      #create key pairs of ("key", list[val1,val2])
      grp = grp.combineByKey( init_combiner, merge_vals, merge_combiners, 4)
    elif use_str:
      #create key pairs of ("key", "val1 val2")
      grp = grp.reduceByKey(lambda x,y: x+" "+y, 4)
    elif use_grp:
      #create key pairs of ("key", iteratable)
      grp = grp.groupByKey(4)

  out = grp.take(150)
  for e in out:
    print (e)


def main( args, loglevel ):
  logging.basicConfig(format="%(levelname)s %(message)s", level=loglevel)
  logging.info("Starting the script")
  test_case()

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
  #parser.add_argument(
  #  "directory",
  #  help = "the path of the directory which holds the wiki dumps",
  #  metavar = "dir")
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


