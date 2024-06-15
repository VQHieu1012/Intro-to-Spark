import findspark
findspark.init(r"C:\spark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

spark = SparkSession.builder.appName("Customer Orders").getOrCreate()

schema = StructType([
    StructField("Cust_ID", IntegerType(), True),
    StructField("Prod_ID", IntegerType(), True),
    StructField("Spent_amount", FloatType(), True)
])

df = spark.read.schema(schema).csv(r"data\customer-orders.csv")
df.printSchema()

result = df.groupBy("Cust_ID") \
            .agg(func.round(func.sum("Spent_amount"), 2) \
            .alias("Sum_amount")).sort("Sum_amount")
                           
result.show()
spark.stop()