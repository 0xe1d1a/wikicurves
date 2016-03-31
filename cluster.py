from random import randint

from pyspark import SparkContext, SparkConf


class KMeans(object):
    def __init__(self, sc, rdd, k, max_iterations=100):
        self.sc = sc
        self.rdd = rdd
        self.k = k
        self.max_iterations = max_iterations

    def cluster(self):
        old_centroids = None
        centroids = self._initialise_centroids(self.rdd)
        iteration = 0
        while not self._should_stop(old_centroids, centroids, iteration):
            old_centroids = centroids
            iteration += 1
            self._add_labels(centroids)
            centroids = self._update_centroids(centroids)
            centroids = self.sc.broadcast(centroids)
        return centroids

    def _initialise_centroids(self):
        return self.rdd.take(self.k)

    def _should_stop(self, old_centroids, centroids, iteration):
        convergence = (old_centroids == centroids)
        duration = (iteration >= self.max_iterations)
        return convergence or duration

    def _add_labels(self, centroids):
        def min_dist(x):
            min_dist = 10000
            centroid = 0
            for c in range(self.k):
                dst = self.distance(x, centroids[c])
                if dst < min_dist:
                    min_dist = dst
                    centroid = c
            x.insert(0, centroid)  # assign centroid to instance

        self.rdd.foreach(min_dist)

    def _update_centroids(self):
        def remove_label(x):
            x.pop(0)

        self.rdd.foreach(remove_label)
        return self.rdd.take(self.k)

    def distance(self, v1, v2):
        return randint(1, 10000)


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

centroids = KMeans(sc, parsed_data, 3)

log.info(centroids)
