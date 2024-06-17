import findspark
findspark.init(r"C:\spark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName('PopularMovies').getOrCreate()

schema = StructType([ \
    StructField("userID", IntegerType(), True), \
    StructField("movieID", IntegerType(), True), \
    StructField("rating", IntegerType(), True), \
    StructField("timestamp", LongType(), True)
    ])

movieDF = spark.read.option("sep", "\t").schema(schema).csv(r"data\ml-100k\u.data")

topMovieIDs = movieDF.groupBy("movieID").count().sort(func.col("count").desc())
topMovieIDs.show(10)

spark.stop()