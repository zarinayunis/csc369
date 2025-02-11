import os
os.environ['JAVA_HOME'] = '/Users/zzyun/Documents/College/Cal Poly/Year 4/Winter Quarter/CSC 369/jdk-23.0.2.jdk/Contents/Home'
os.environ['PATH'] = '/Users/zzyun/Documents/College/Cal Poly/Year 4/Winter Quarter/CSC 369/jdk-23.0.2.jdk/Contents/Home/bin'

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("CSV Operations") \
    .master("local[*]") \
    .getOrCreate()


pixels_df = spark.read.csv('/Users/zzyun/Documents/College/Cal Poly/Year 4/Winter Quarter/CSC 369/2022_place_canvas_history.csv', header=True)
pixels_df.show(5, 0)