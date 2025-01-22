import polars as pl
from time import perf_counter_ns
from datetime import datetime

file_path = "/Users/zzyun/Documents/College/Cal Poly/Year 4/Winter Quarter/CSC 369/2022_place_canvas_history.csv"

def most_placed_color_and_pixel():
    start_hour_str = input("What is the starting time in YYYY-MM-DD HH format? ")
    end_hour_str = input("What is the ending time in YYYY-MM-DD HH format? ")

    start_hour = datetime.strptime(start_hour_str, "%Y-%m-%d %H")
    end_hour = datetime.strptime(end_hour_str, "%Y-%m-%d %H")

    polars_df = pl.read_csv(file_path)

    # print("Before conversion:", polars_df.dtypes)

    polars_df = polars_df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f %Z", strict=False))

    # print("After conversion:", polars_df.dtypes)
    
    start = perf_counter_ns()
    
    filtered_polars = polars_df.filter((pl.col("timestamp") >= pl.lit(start_hour)) & (pl.col("timestamp") <= pl.lit(end_hour)))

    most_freq_color = (filtered_polars.select(pl.col("pixel_color")).group_by("pixel_color").count().sort(by="count", descending=True).head(1))
    most_freq_pixel = (filtered_polars.select(pl.col("coordinate")).group_by("coordinate").count().sort(by="count", descending=True).head(1))

    end = perf_counter_ns()

    milliseconds = (end - start) / 1000000 

    return milliseconds, most_freq_color, most_freq_pixel

def main():
    milliseconds, color, pixel = most_placed_color_and_pixel()
    print(milliseconds, color, pixel)

if __name__ == "__main__":
    main()