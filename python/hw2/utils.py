import argparse
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import StructType, StringType,IntegerType,FloatType

# parse command line arguments
# see https://docs.python.org/3/howto/argparse.html#argparse-tutorial
def parse_commandline() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", help="path for input data files")
    parser.add_argument("--output_path", help="path for result files", default='na')
    args = parser.parse_args()
    return args

taxicab_schema = StructType() \
     .add("VendorID", StringType(), True) \
     .add("lpep_pickup_datetime", StringType(), True) \
     .add("lpep_dropoff_datetime", StringType(), True) \
     .add("store_and_fwd_flag", StringType(), True) \
     .add("RatecodeID", StringType(), True) \
     .add("PULocationID", StringType(), True) \
     .add("DOLocationID", StringType(), True) \
     .add("passenger_count", IntegerType(), True) \
     .add("trip_distance", FloatType(), True) \
     .add("fare_amount", FloatType(), True) \
     .add("extra", FloatType(), True) \
     .add("mta_tax", FloatType(), True) \
     .add("tip_amount", FloatType(), True) \
     .add("tolls_amount", FloatType(), True) \
     .add("ehail_fee", FloatType(), True) \
     .add("improvement_surcharge", FloatType(), True) \
     .add("total_amount", FloatType(), True) \
     .add("payment_type", StringType(), True) \
     .add("trip_type", StringType(), True) \
     .add("congestion_surcharge", FloatType(), True)

def read_input_csv(spark:SparkSession, input_path:str, **kvargs) -> SparkDataFrame:
    lines = spark \
        .read \
        .format("csv") \
        .option("header", True) \
        .option("path",input_path) \
        .schema(taxicab_schema)
    result = lines.load()
    return result

def read_input_csv_stream(spark:SparkSession, input_path:str, **kvargs) -> SparkDataFrame:
    lines = spark \
        .readStream \
        .format("csv") \
        .option("header", True) \
        .schema(taxicab_schema)
    result = lines.load(input_path)
    return result

def write_output_csv(output_df:SparkDataFrame, output_path:str) -> None:
  if output_path:
    output_df.write.format('csv').option('header','true').mode("overwrite").save(output_path)
  else:
    output_df.show(10, False)

def write_output_csv_stream(output_df:SparkDataFrame, output_path:str = '') -> None:
  if output_path != '':
    query = output_df \
      .writeStream \
      .format('csv') \
      .trigger(processingTime="10 seconds") \
      .option('header','true') \
      .option("path",output_path) \
      .outputMode("append") \
      .option("checkpointLocation", "checkpoint/") \
      .start()
  else:
    query = output_df \
      .writeStream \
      .format('console') \
      .trigger(processingTime="30 seconds") \
      .outputMode("append") \
      .option("checkpointLocation", "checkpoint/") \
      .start()
  query.awaitTermination()
    