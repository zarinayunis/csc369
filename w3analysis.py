from time import perf_counter_ns
from datetime import datetime
import polars as pl

parquet_file_path = "/Users/zzyun/Documents/GitHub/csc369/week3/2022pyarrow.parquet"
polars_df = pl.scan_parquet(parquet_file_path)

def color_pixel_analysis():
    start_hour_str = input("What is the starting time in YYYY-MM-DD HH format? ")
    end_hour_str = input("What is the ending time in YYYY-MM-DD HH format? ")

    start_hour = datetime.strptime(start_hour_str, "%Y-%m-%d %H")
    end_hour = datetime.strptime(end_hour_str, "%Y-%m-%d %H")

    lazy_polars_df = polars_df.with_columns(
        pl.col("timestamp").cast(pl.Utf8)  
    ).with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f %Z", strict=False)
    )

    filtered_polars = lazy_polars_df.filter(
        (pl.col("timestamp") >= pl.lit(start_hour)) & 
        (pl.col("timestamp") <= pl.lit(end_hour))
    )


    filtered_result = filtered_polars.collect()


    colors_unique = filtered_result.group_by("pixel_color").agg(
        pl.col("user_id").n_unique().alias("count")
    ).sort(by="count", descending=True)


    pixels = filtered_result.group_by("user_id").agg(
        pl.len().alias("pixel_count")
    )


    pixels_summary = pixels.describe(percentiles=[0.5, 0.75, 0.9, 0.99])


    first_time_users = (
        filtered_result.group_by("user_id")
        .agg(pl.col("x").count().alias("user_count"))
        .filter(pl.col("user_count") == 1)
        .height
    )

    user_session_length = filtered_result.sort(["user_id", "timestamp"])

    user_session_length = user_session_length.with_columns(
    (pl.col("timestamp").diff()).alias("time_diff") 
    )

    user_session_length = user_session_length.with_columns(
        (pl.col("time_diff").cast(pl.Int64) / 1000000).alias("time_diff_in_seconds") 
    )

    user_session_length = user_session_length.with_columns(
        (pl.col("time_diff_in_seconds") > 900).alias("time_diff_greater_than_15")
    )

    user_session_length = user_session_length.with_columns(
        pl.col("time_diff_greater_than_15").cum_sum().alias("session_id")
    )
    session_lengths = user_session_length.group_by(["user_id", "session_id"]).agg(
        (pl.col("timestamp").max() - pl.col("timestamp").min()).alias("session_length")
    )

    user_pixel_counts = user_session_length.group_by("user_id").agg(pl.count().alias("pixel_count"))

    valid_users = user_pixel_counts.filter(pl.col("pixel_count") > 1)

    valid_session_lengths = session_lengths.filter(
        pl.col("user_id").is_in(valid_users["user_id"])
    )

    avg_session_length = valid_session_lengths.select(
        pl.col("session_length").mean()
    )


    return colors_unique, pixels_summary, first_time_users, avg_session_length


def main():
    start = perf_counter_ns()
    colors, pixels, first_time_users, avg_session_length = color_pixel_analysis()
    print("Number of unique colors:\n", colors)
    print("Pixels summary statistics:\n", pixels)
    print("Number of first-time users:\n", first_time_users)
    print("Average session length:", avg_session_length)
    end = perf_counter_ns()
    milliseconds = (end - start) / 1000000
    print("Milliseconds", milliseconds)

if __name__ == "__main__":
    main()
