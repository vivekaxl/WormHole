from pyspark import SparkContext
sc = SparkContext("local","simple app")

from numpy import array
from math import sqrt

from pyspark.mllib.clustering import KMeans, KMeansModel

def process_line(line):
    t_line = line.strip()[1:-1]
    # Remove file position
    t_line = t_line.split(', \"__fileposition__\":')[0]
    tt_line = [t_l.split(':')[-1].strip().replace('"', '').replace('?', '1') for t_l in t_line.split(',')]
    values = map(float, tt_line)
    return values

# Load and parse the data
data = sc.textFile("file:///home/osboxes/GIT/mount_pnt2/fuse_testing/clustering_medium_kegg")
parsedData = data.map(lambda line: process_line(line))

# Build the model (cluster the data)
clusters = KMeans.train(parsedData, 2, maxIterations=10, initializationMode="random")

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))

