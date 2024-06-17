import findspark
findspark.init(r"C:\spark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Popular Hero").getOrCreate()

schema = StructType([\
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
    ])

names = spark.read.schema(schema).option("sep", " ").csv(r"data\Marvel Names.txt")
lines = spark.read.text(r"data\Marvel Graph.txt")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
                    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) \
                    .groupBy("id").agg(func.sum("connections").alias("connections"))

mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearance.")
spark.stop()

               