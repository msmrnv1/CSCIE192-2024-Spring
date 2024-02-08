import time
from collections import Counter

#def sorted_word_frequency(lines: list[str]) -> dict[str, int]:

#    return sorted_frequency

def print_top_words(file_path: str) -> None:
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()

            # Call sorted_word_frequency function to get word frequencies
            # word_frequency = sorted_word_frequency(lines)

            num_lines = len(lines)
            print(f"Number of lines: {num_lines}")
            # Call sorted_word_frequency function to get word frequencies

            # Print the top 10 words by frequency

    except FileNotFoundError:
        print(f"File '{file_path}' not found.")


# Command-line execution
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python hw2p1.py input_file_path")
    else:
        file_path = sys.argv[1]
        # Measure the execution time of the function
        start_time = time.time()  # Record the current time

        print_top_words(file_path)

        end_time = time.time()  # Record the current time again
        # Calculate the elapsed time
        elapsed_time = end_time - start_time
        print(f"Execution time: {elapsed_time} seconds")