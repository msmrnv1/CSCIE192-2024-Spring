import time
import pandas as pd


def calculate_summary_metrics(input_file, summary_output_file_path, relative_output_file_path):
    try:
        # Read input CSV file
        df = pd.read_csv(input_file, skip_blank_lines=True, low_memory=False, na_values=['', 'NA', 'N/A', 'null', 'NaN'])

        df.dropna(subset=['passenger_count','fare_amount'], inplace = True)

        # Calculate summary metrics
        total_trips = len(df)

        # Calculate relative metrics
        mean_fare_by_payment_type = df.groupby('payment_type')['fare_amount'].mean()

        # Create summary output dataframe
        summary_df = pd.DataFrame({
            'total number of trips': [total_trips]
        })

        # Create relative output dataframe
        relative_df = pd.DataFrame({
            'payment_type': mean_fare_by_payment_type.index,
            'mean fare per trip': mean_fare_by_payment_type.values
        })



        # Write results to output CSV files
        summary_df.to_csv(summary_output_file_path, index=False)
        print("Summary metrics calculated and written to", summary_output_file_path)

        relative_df.to_csv(relative_output_file_path, index=False)
        print("Relative metrics calculated and written to", relative_output_file_path)

    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")


# Command-line execution
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python hw2p2.py input_file_path summary_output_file_path relative_output_file_path")
    else:
        input_file_path = sys.argv[1]
        summary_output_file_path = sys.argv[2]
        relative_output_file_path = sys.argv[3]
        # Measure the execution time of the function
        start_time = time.time()  # Record the current time

        calculate_summary_metrics(input_file_path, summary_output_file_path, relative_output_file_path )

        end_time = time.time()  # Record the current time again
        # Calculate the elapsed time
        elapsed_time = end_time - start_time
        print(f"Execution time: {elapsed_time} seconds")