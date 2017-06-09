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

def g(x): print x
parsedData.foreach(g)