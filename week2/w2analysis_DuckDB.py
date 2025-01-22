from time import perf_counter_ns
from datetime import datetime
import duckdb

file_path = "/Users/zzyun/Documents/College/Cal Poly/Year 4/Winter Quarter/CSC 369/2022_place_canvas_history.csv"

def most_placed_color_and_pixel():
    start_hour_str = input("What is the starting time in YYYY-MM-DD HH format? ")
    end_hour_str = input("What is the ending time in YYYY-MM-DD HH format? ")

    start_hour = datetime.strptime(start_hour_str, "%Y-%m-%d %H")
    end_hour = datetime.strptime(end_hour_str, "%Y-%m-%d %H")

    conn = duckdb.connect(database=':memory:')

    conn.execute(f"CREATE TABLE duckdb_df AS SELECT * FROM read_csv_auto('{file_path}', timestampformat='%Y-%m-%d %H:%M:%S.%f %Z');")

    start = perf_counter_ns()

    conn.execute(f"CREATE TABLE filtered_query_df AS SELECT * FROM duckdb_df WHERE timestamp >= '{start_hour}' AND timestamp <= '{end_hour}'")

    # filtered_query = conn.execute(f"SELECT * FROM duckdb_df WHERE timestamp >= '{start_hour}' AND timestamp <= '{end_hour}'")
    # filtered_query_df = conn.execute(filtered_query).df()

    max_pixel_query = """
        SELECT pixel_color, COUNT(*) AS frequency
        FROM filtered_query_df
        GROUP BY pixel_color
        ORDER BY frequency DESC
        LIMIT 1
    """
    max_color_query = """
        SELECT coordinate, COUNT(*) AS frequency
        FROM filtered_query_df
        GROUP BY coordinate
        ORDER BY frequency DESC
        LIMIT 1
    """

    max_pixel = conn.execute(max_pixel_query).fetchall()
    max_color = conn.execute(max_color_query).fetchall()

    end = perf_counter_ns()
    milliseconds = (end - start) / 1000000

    return milliseconds, max_pixel, max_color

def main():
    milliseconds, max_pixel, max_color = most_placed_color_and_pixel()
    most_placed_color = max_pixel[0] if max_pixel else ("N/A", 0)
    most_frequent_pixel = max_color[0] if max_color else ("N/A", 0)
    print(f"Execution Time: {milliseconds:.2f}")
    print(f"Most Placed Color: {most_placed_color[0]}")
    print(f"Most Frequent Pixel Location: {most_frequent_pixel[0]}")

if __name__ == "__main__":
    main()
