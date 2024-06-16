import findspark
findspark.init(r"C:\spark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Obsecure Hero").getOrCreate()

schema = StructType([\
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
    ])

names = spark.read.schema(schema).option("sep", " ").csv(r"data\Marvel Names.txt")
lines = spark.read.text(r"data\Marvel Graph.txt")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
                    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) \
                    .groupBy("id").agg(func.sum("connections").alias("connections"))

mostObsecure = connections.sort(func.col("connections").asc()).first()

mostObsecureName = names.filter(func.col("id") == mostObsecure[0]).select("name").first()

print(mostObsecureName[0] + " is the most popular superhero with " + str(mostObsecure[1]) + " co-appearance.")
spark.stop()

               