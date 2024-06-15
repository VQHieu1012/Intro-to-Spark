import findspark
findspark.init(r"C:\spark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

spark = SparkSession.builder.appName('Min Temperature').getOrCreate()

schema = StructType([\
                    StructField("stationID", StringType(), True), \
                    StructField("date", IntegerType(), True), \
                    StructField("measure_type", StringType(), True), \
                    StructField("temperature", FloatType(), True)])

df = spark.read.schema(schema).csv(r"data\1800.csv")
df.printSchema()

minTemps = df.filter(df.measure_type == "TMIN")
stationTemps = minTemps.select("stationID", "temperature")
minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation.show()

minTempsByStationF = minTempsByStation.withColumn("temperature",
                                                  func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 323.0, 2))\
                                                  .select("stationID", "temperature").sort("temperature")

result = minTempsByStationF.collect()

for re in result:
    print(f"{re[0]} \t {re[1]}")

spark.stop()