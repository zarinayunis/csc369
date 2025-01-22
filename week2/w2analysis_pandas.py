import pandas as pd
from time import perf_counter_ns
from datetime import datetime

file_path = "/Users/zzyun/Documents/College/Cal Poly/Year 4/Winter Quarter/CSC 369/2022_place_canvas_history.csv"

def most_placed_color_and_pixel():
    start_hour_str = input("What is the starting time in YYYY-MM-DD HH format? ")
    end_hour_str = input("What is the ending time in YYYY-MM-DD HH format? ")

    start_hour = datetime.strptime(start_hour_str, "%Y-%m-%d %H")
    end_hour = datetime.strptime(end_hour_str, "%Y-%m-%d %H")

    pandas_df = pd.read_csv(file_path)

    pandas_df["timestamp"] = pd.to_datetime(pandas_df["timestamp"], errors="coerce", utc=True)
    pandas_df["timestamp"] = pandas_df["timestamp"].dt.tz_convert(None)
    
    start = perf_counter_ns()
    
    filtered_pandas = pandas_df[(pandas_df["timestamp"] >= start_hour) & (pandas_df["timestamp"] <= end_hour)]

    end = perf_counter_ns()

    milliseconds = (end - start) / 1000000 

    return milliseconds, filtered_pandas["pixel_color"].mode(), filtered_pandas["coordinate"].mode()

def main():
    color, pixel, milliseconds = most_placed_color_and_pixel()
    print(color, pixel, milliseconds)

if __name__ == "__main__":
    main()


