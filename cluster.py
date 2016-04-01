import numpy as np
from fastdtw import fastdtw
from pyspark import SparkContext, SparkConf
from scipy.spatial.distance import euclidean


class KMeans(object):
    def __init__(self, rdd, k, max_iterations=100):
        self.rdd = rdd.mapValues(lambda val: np.array(val))
        self.k = k
        self.max_iterations = max_iterations

    def cluster(self):
        old_centroids = None
        centroids = self._initialise_centroids()
        iteration = 0
        while not self._should_stop(old_centroids, centroids, iteration):
            old_centroids = centroids
            iteration += 1
            self._add_labels(centroids)
            centroids = self._get_centroids(centroids)
        return centroids

    def _initialise_centroids(self):
        return self.rdd.take(self.k)

    def _should_stop(self, old_centroids, centroids, iteration):
        convergence = (old_centroids == centroids)
        duration = (iteration >= self.max_iterations)
        return convergence or duration

    def _add_labels(self, centroids):
        def min_dist(x):
            min_dist = 100000000  # don't judge my large number
            centroid = 0
            for c in range(self.k):
                dst = self.distance(x, centroids[c])
                if dst < min_dist:
                    min_dist = dst
                    centroid = c
            x = (centroid, x)  # assign centroid to instance

        self.rdd.foreach(min_dist)

    def _get_centroids(self):
        def remove_label(x):
            x = x[1]

        centroids = self.rdd.reduceByKey(
            lambda x, y: (x + y) / 2).cache().collect()
        self.rdd.foreach(remove_label)
        centroids.foreach(remove_label)
        return centroids.take(self.k)

    def distance(self, v1, v2):
        distance, path = fastdtw(v1, v2, dist=euclidean)
        return distance


def quiet_logs(sc):
    log = sc._jvm.org.apache.log4j
    log.LogManager.getLogger('org').setLevel(log.Level.ERROR)
    log.LogManager.getLogger('akka').setLevel(log.Level.ERROR)


app_name = 'wikicurves'
conf = SparkConf().setAppName(app_name)
sc = SparkContext(conf=conf)

quiet_logs(sc)
log = sc._jvm.org.apache.log4j.Logger.getLogger(__name__)

FILENAME = '/user/lsde10/fin3/*'

data = sc.textFile(FILENAME)
parsed_data = data.map(lambda x: x.split(',')[2:])

model = KMeans(parsed_data, 3)
centroids = model.cluster()

log.info(centroids)
