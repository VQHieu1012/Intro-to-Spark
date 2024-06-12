import findspark
findspark.init(r"C:\spark")

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Min Temperature")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile(r"data\1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.reduceByKey(lambda x, y: min(x, y))
results = minTemps.collect()

for result in results:
    print(result[0] + f"\t{result[1]}F")