from pyspark import SparkContext
sc = SparkContext("local","simple app")

from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel

# Load and parse the data
def process_line(line):
    t_line = line.strip()[1:-1]
    # Remove file position
    t_line = t_line.split(', \"__fileposition__\":')[0]
    tt_line = [t_l.split(':')[-1].strip().replace('"', '').replace('?', '1') for t_l in t_line.split(',')]
    values = map(float, tt_line)
    return LabeledPoint(values[-1], values[:-1])

data = sc.textFile("file:///home/osboxes/GIT/mount_pnt2/fuse_testing/regression_medium_bikesharing")
parsedData = data.map(process_line)

# Build the model
model = LinearRegressionWithSGD.train(parsedData)

# Evaluate the model on training data
valuesAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
MSE = valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds.count()
print("Mean Squared Error = " + str(MSE))