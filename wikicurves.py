from pyspark import SparkContext
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

  #basic RDD data manipulation here
  #ref: https://spark.apache.org/docs/0.9.0/api/pyspark/index.html
  numAs = data.filter(lambda s: 'a' in s).count()
  numBs = data.filter(lambda s: 'b' in s).count()

  print("Lines with a: %i, lines with b: %i" % (numAs, numBs))


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
