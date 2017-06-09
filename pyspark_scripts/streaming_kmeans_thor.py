from pyspark import SparkContext
from pyspark.streaming import StreamingContext
sc = SparkContext("local","simple app")
ssc = StreamingContext(sc, 1)

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans

def process_line(line):
    t_line = line.strip()[1:-1]
    # Remove file position
    t_line = t_line.split(', \"__fileposition__\":')[0]
    tt_line = [t_l.split(':')[-1].strip().replace('"', '').replace('?', '1') for t_l in t_line.split(',')]
    values = map(float, tt_line)
    return values

# we make an input stream of vectors for training,
# as well as a stream of vectors for testing
def parse(lp):
    label = lp.split(',')[0]
    vec = Vectors.dense(lp.split(',')[1:])

    return LabeledPoint(label, vec)

trainingData = sc.textFile("file:///home/osboxes/GIT/mount_pnt2/fuse_testing/clustering_medium_kegg")\
.map(lambda line: Vectors.dense(process_line(line)))
# trainingData = sc.textFile("file:///home/osboxes/pyspark_scripts/Data/clustering_medium_kegg_small_train.csv")\
#     .map(lambda line: Vectors.dense([float(x) for x in line.strip().split(',')]))

testingData = sc.textFile("file:///home/osboxes/pyspark_scripts/Data/clustering_medium_kegg_big.csv").map(parse)



trainingQueue = [trainingData]
testingQueue = [testingData]

trainingStream = ssc.queueStream(trainingQueue)
testingStream = ssc.queueStream(testingQueue)

# We create a model with random clusters and specify the number of clusters to find
model = StreamingKMeans(k=5, decayFactor=1.0).setRandomCenters(28, 1.0, 0)

# Now register the streams for training and testing and start the job,
# printing the predicted cluster assignments on new data points as they arrive.
model.trainOn(trainingStream)

result = model.predictOnValues(testingStream.map(lambda lp: (lp.label, lp.features)))
result.pprint()

ssc.start()
ssc.stop(stopSparkContext=True, stopGraceFully=True)

print("Final centers: " + str(model.latestModel().centers))

# def g(x): print x
# trainingData.foreach(g)