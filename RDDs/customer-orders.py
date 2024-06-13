import findspark
findspark.init(r"C:\spark")

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Customer Orders")
sc = SparkContext(conf=conf)

def parseLine(line):
    line = line.split(",")
    return (int(line[0]), float(line[2]))

customers = sc.textFile(r"data\customer-orders.csv")
customers = customers.map(parseLine)
customerOrders = customers.reduceByKey(lambda x, y: x + y)
customerOrdersSorted = customerOrders.map(lambda x: (x[1], x[0])).sortByKey()

results = customerOrdersSorted.collect()

for result in results:
    print(f"{result[1]} \t {result[0]}")