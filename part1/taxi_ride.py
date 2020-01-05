from pyspark.sql.session import SparkSession
from sys import argv

spark = SparkSession.builder.appName("NYC Taxi Rides").master("local").getOrCreate()
df = spark.read.option("header", True).csv(argv[1])
print(f"Number of records = {df.count():,.0f}")
