from pyspark import SparkContext
sc = SparkContext("local","simple app")

from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint

# Load and parse the data
def process_line(line):
    t_line = line.strip()[1:-1]
    # Remove file position
    t_line = t_line.split(', \"__fileposition__\":')[0]
    tt_line = [t_l.split(':')[-1].strip().replace('"', '').replace('?', '1') for t_l in t_line.split(',')]
    values = map(float, tt_line)
    return LabeledPoint(values[-1], values[:-1])

data = sc.textFile("file:///home/osboxes/GIT/mount_pnt2/fuse_testing/classification_medium_shuttle_binomial")
parsedData = data.map(process_line)

print parsedData.take(1)
# Build the model
model = LogisticRegressionWithLBFGS.train(parsedData)

# Evaluating the model on training data
labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
print("Training Error = " + str(trainErr))