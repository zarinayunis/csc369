import csv
from time import perf_counter_ns
from collections import Counter
from datetime import datetime

file_path = "/Users/zzyun/Documents/College/Cal Poly/Year 4/Winter Quarter/CSC 369/2022_place_canvas_history.csv"


def most_placed_color_and_pixel():
    start_hour_str = input("What is the starting time in YYYY-MM-DD HH format? ")
    end_hour_str = input("What is the ending time in YYYY-MM-DD HH format? ")

    start_hour = datetime.strptime(start_hour_str, "%Y-%m-%d %H")
    end_hour = datetime.strptime(end_hour_str, "%Y-%m-%d %H")

    hex_codes_counter = Counter()
    pixel_locations_counter = Counter()

    start, end = None, None

    start = perf_counter_ns()

    with open(file_path, 'r') as file:
        reader = csv.reader(file, delimiter=',')
        next(reader)


        for row in reader:
            time = row[0]
            try:
                row_time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S.%f %Z")

                if start_hour <= row_time <= end_hour:
                    hex_code = row[2]
                    pixel_location = row[3]
                    hex_codes_counter[hex_code] += 1
                    pixel_locations_counter[pixel_location] += 1

            except ValueError:
                pass 

    end = perf_counter_ns()

    milliseconds = (end - start) / 1000000 

    return hex_codes_counter.most_common(1), pixel_locations_counter.most_common(1), milliseconds



def main():
    color, pixel, milliseconds = most_placed_color_and_pixel()
    print(color, pixel, milliseconds)

if __name__ == "__main__":
    main()