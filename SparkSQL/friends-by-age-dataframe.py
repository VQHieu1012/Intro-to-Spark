import findspark
findspark.init(r'C:\spark')

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option('header', 'true').option('inferSchema', 'true')\
                .csv(r'data\fakefriends-header.csv')

print("Here is our inferred schema:")
people.printSchema()

print("Average friends:")
friendsByAge = people.select("age", "friends")
friendsByAge.groupBy("age").avg("friends").show()
friendsByAge.groupBy("age").avg("friends").sort("age").show()
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)
            .alias("friends_avg")).sort("age").show()

spark.stop()

spark.stop()



spark.stop()