from pyspark import SparkContext
from time import time
import sys, argparse, logging

global args

#hides non-spark & non-errornous logging messages
def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )


def create_context( ):
  global args
  path = args.directory
  # should iterate through directories here (os or special hdfs walk?)
  wikidump =  path + "projectcounts-20140131-230000"
  sc = SparkContext("local", "wikicurves")
  if args.silent:
    quiet_logs(sc)
  data = sc.textFile(wikidump).cache()
  # extract the second column of the whole file
  request_count = data.map(lambda x: x.split(" ")[2])

  #basic RDD data manipulation here
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
