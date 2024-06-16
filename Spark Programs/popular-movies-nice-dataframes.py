import findspark
findspark.init(r"C:\spark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, LongType
import codecs

def loadMovieNames():
    movieNames = {}
    with codecs.open(r"data\ml-100k\u.ITEM", "r", encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
        return movieNames
    
spark = SparkSession.builder.appName('PopularMovies').getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames())

schema = StructType([ \
    StructField("userID", IntegerType(), True), \
    StructField("movieID", IntegerType(), True), \
    StructField("rating", IntegerType(), True), \
    StructField("timestamp", LongType(), True)
    ])

moviesDF = spark.read.option("sep", "\t").schema(schema).csv(r"data\ml-100k\u.data")

movieCounts = moviesDF.groupBy("movieID").count()

def lookupName(movieID):
    return nameDict.value[movieID]

lookupNameUDF = func.udf(lookupName)

movieWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

sortedMoviesWithNames = movieWithNames.orderBy(func.desc("count"))

sortedMoviesWithNames.show(10, False)

spark.stop()