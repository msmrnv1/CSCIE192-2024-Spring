from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
import pyspark.sql.functions as f
from utils import parse_commandline, read_input_csv, write_output_csv


# data transformation function
def transform_data(rows_df: SparkDataFrame, **kvargs) -> SparkDataFrame:
    result_df = rows_df.groupBy('VendorID').max('Fare_amount', 'Tolls_amount') #.agg(max('Fare_amount'))
    return result_df

# spark job pipeline
def run_pipeline(input_path:str, output_path:str) -> None:
    # 1. create spark session
    spark = SparkSession.builder \
        .appName("BatchTransformTaxi") \
        .getOrCreate()
 #       .config('spark.local.dir', '/Users/ESumitra/tmp/spark-localdir') \

    try:
        input_df = read_input_csv(spark, input_path) # 2. read input DataFrame
        result_df = transform_data(input_df)         # 3. transform data
        write_output_csv(result_df, output_path)     # 4. write output from DataFrame
    except Exception as e:
        print(f'error running pipeline\n{e}')
    finally:
        spark.stop()

# main entry point
if __name__ == "__main__":
    """
        Usage: 
        ./bin/spark-submit $SCRIPT_PATH/batch_transform_taxi.py --input_path $INPUT_PATH/taxi_tripdata_sample.csv --output_path $OUTPUT_PATH
    """
    args = parse_commandline()
    run_pipeline(args.input_path, args.output_path)


