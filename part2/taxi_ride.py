from pyspark.sql.session import SparkSession, DataFrame

from time import time
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType

from pyspark.sql.functions import lit, max, min, col, input_file_name, split, reverse, concat_ws, year, month, \
    format_string


def timer(func):
    def wrapper(*args, **kwargs):
        before = time()
        result = func(*args, **kwargs)
        print(f"Time taken by '{func.__name__}' function: {(time() - before):.2f} seconds\n")
        return result

    return wrapper


yellow_taxi_schema = StructType(
    [StructField("VendorID", IntegerType(), False),
     StructField("tpep_pickup_datetime", TimestampType(), False),
     StructField("tpep_dropoff_datetime", TimestampType(), False),
     StructField("passenger_count", IntegerType(), False),
     StructField("trip_distance", FloatType(), False),
     StructField("RatecodeID", IntegerType(), False),
     StructField("store_and_fwd_flag", StringType(), False),
     StructField("PULocationID", IntegerType(), False),
     StructField("DOLocationID", IntegerType(), False),
     StructField("payment_type", IntegerType(), False),
     StructField("fare_amount", FloatType(), False),
     StructField("extra", FloatType(), False),
     StructField("mta_tax", FloatType(), False),
     StructField("tip_amount", FloatType(), False),
     StructField("tolls_amount", FloatType(), False),
     StructField("improvement_surcharge", FloatType(), False),
     StructField("total_amount", FloatType(), False),

     ])

green_taxi_schema = StructType(
    [StructField("VendorID", IntegerType(), False),
     StructField("lpep_pickup_datetime", TimestampType(), False),
     StructField("lpep_dropoff_datetime", TimestampType(), False),
     StructField("store_and_fwd_flag", StringType(), False),
     StructField("RatecodeID", IntegerType(), False),
     StructField("PULocationID", IntegerType(), False),
     StructField("DOLocationID", IntegerType(), False),
     StructField("passenger_count", IntegerType(), False),
     StructField("trip_distance", FloatType(), False),
     StructField("fare_amount", FloatType(), False),
     StructField("extra", FloatType(), False),
     StructField("mta_tax", FloatType(), False),
     StructField("tip_amount", FloatType(), False),
     StructField("tolls_amount", FloatType(), False),
     StructField("ehail_fee", FloatType(), False),
     StructField("improvement_surcharge", FloatType(), False),
     StructField("total_amount", FloatType(), False),
     StructField("payment_type", IntegerType(), False),
     StructField("trip_type", IntegerType(), False)])


def transform_raw_data(df: DataFrame) -> DataFrame:
    return df \
        .withColumn("split_file_name",
                    split(reverse(split(reverse("file_name"), "/")[0]), "_")) \
        .withColumn("taxi_type", col("split_file_name")[0]) \
        .withColumn("file_year_and_month",
                    split(col("split_file_name")[2], "\.")[0]) \
        .withColumn("data_year_month",
                    concat_ws("-", year("pickup_datetime"),
                              format_string("%02d", month("pickup_datetime")))) \
        .drop("file_name", "split_file_name")


@timer
def execute():
    spark = SparkSession.builder.appName("NYC Taxi Rides").master("local[3]").getOrCreate()
    yellow_df = spark.read.option("header", True) \
        .schema(yellow_taxi_schema) \
        .csv("/Users/hasif/Downloads/TestData/NYC_TaxiRide/2018/yellow_tripdata_2018-04.csv") \
        .withColumn("file_name", input_file_name()) \
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
        .withColumn("tripType", lit("N/A")) \
        .withColumn("ehail_fee", lit(0.0))

    green_df = spark.read.option("header", True) \
        .schema(green_taxi_schema) \
        .csv("/Users/hasif/Downloads/TestData/NYC_TaxiRide/2018/green_tripdata_2018-04.csv") \
        .withColumn("file_name", input_file_name()) \
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

    transformed_yellow_df = transform_raw_data(yellow_df)
    transformed_green_df = transform_raw_data(green_df)

    valid_taxi_df = transformed_yellow_df.where('data_year_month == file_year_and_month').union(
        transformed_green_df.where('data_year_month == file_year_and_month'))
    invalid_taxi_df = transformed_yellow_df.where('data_year_month != file_year_and_month').union(
        transformed_green_df.where('data_year_month != file_year_and_month'))

    print(f"Number of invalid record : {invalid_taxi_df.count()}")
    print(f"Number of valid record : {valid_taxi_df.count()}")


if __name__ == "__main__":
    execute()
