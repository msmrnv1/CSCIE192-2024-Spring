from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
import pyspark.sql.functions as f
from utils import parse_commandline, read_input_csv_stream, write_output_csv_stream


# data transformation function
def transform_data(rows_df: SparkDataFrame, **kvargs) -> SparkDataFrame:
    with_ts_df = rows_df.withColumn("event_time", f.to_timestamp('lpep_dropoff_datetime'))
    window_by_60m_df = with_ts_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        f.window('event_time', '60 minutes')
    ) \
    .agg(f.count('VendorID').alias('trip_counts')) \
    .select("window.start", "window.end", "trip_counts")

    result_df = window_by_60m_df
    return result_df

# spark job pipeline
def run_pipeline(input_path:str, output_path:str) -> None:
    # 1. create spark session
    spark = SparkSession.builder \
                .appName("SimpleStreamingApp") \
                .getOrCreate() 
    try:
        input_df = read_input_csv_stream(spark, input_path) # 2. read input DataFrame
        result_df = transform_data(input_df)                # 3. transform data
        write_output_csv_stream(result_df, output_path)     # 4. write output from DataFrame
    except Exception as e:
        print(f'error running pipeline\n{e}')
    finally:
        spark.stop()

# main entry point
if __name__ == "__main__":
    """
        Usage: 
        ./bin/spark-submit $SCRIPT_PATH/stream_transform_taxi.py --input_path $INPUT_PATH --output_path $OUTPUT_PATH
    """
    args = parse_commandline()
    run_pipeline(args.input_path, args.output_path)


