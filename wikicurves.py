from pyspark import SparkContext
from pyspark.mllib import util
from time import time
from datetime import datetime, timedelta
import sys, argparse, logging
from numpy import array

global args

#hides non-spark & non-errornous logging messages
def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )


def create_context( ):
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
    raw_data = sc.textFile(fname)
    sep = raw_data.map(lambda line: line.split(" "))
    sep = sep.map(lambda vals: (vals[1], vals[2]))
    sep = sep.filter(lambda val: val[0][0] != '%')
    car = sep.cartesian(sep).filter(lambda pair: pair[0] != pair[1])
    print(car.take(10))
    return #debug

    cur+= onehour

  #basic RDD data manipulation here, Note unpacking gz takes long time!
  #ref: https://spark.apache.org/docs/0.9.0/api/pyspark/index.html
  #http://spark.apache.org/docs/latest/programming-guide.html



def main( args, loglevel ):
  logging.basicConfig(format="%(levelname)s %(message)s", level=loglevel)
  logging.info("Starting the script")
  create_context()

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


