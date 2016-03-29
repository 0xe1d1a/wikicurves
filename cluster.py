from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.linalg import Vectors

def quiet_logs(sc):
    log = sc._jvm.org.apache.log4j
    log.LogManager.getLogger('org').setLevel(log.Level.ERROR)
    log.LogManager.getLogger('akka').setLevel(log.Level.ERROR)

app_name = 'wikicurves'
conf = SparkConf().setAppName(app_name)
sc = SparkContext(conf=conf)

quiet_logs(sc)
log = sc._jvm.org.apache.log4j.Logger.getLogger(__name__)

filename = '/user/lsde10/fin/part-00000'

data = sc.pickleFile(filename)
parsed_data = data.map(lambda x: Vectors.dense(x[1]))

log.info('Starting k-means')
clusters = KMeans.train(parsed_data, 3)

clusters.save(sc, '/user/lsde10/part-00000.model')
