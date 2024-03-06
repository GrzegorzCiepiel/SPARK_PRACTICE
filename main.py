from pyspark.sql import SparkSession

# Create a new SparkSession
spark = SparkSession.builder.getOrCreate()

# Sample clickstream counts
sample_clickstream_counts = [
    ["other-search", "Hanging_Gardens_of_Babylon", "external", 47000],
    ["other-empty", "Hanging_Gardens_of_Babylon", "external", 34600],
    ["Wonders_of_the_World", "Hanging_Gardens_of_Babylon", "link", 14000],
    ["Babylon", "Hanging_Gardens_of_Babylon", "link", 2500]
]

clickstream_counts_rdd = spark.sparkContext.parallelize(sample_clickstream_counts)

# RDD stands for resilient distributed dataset

# Create a DataFrame from the RDD of sample clickstream counts
clickstream_sample_df = clickstream_counts_rdd.toDF(['source_page', 'target_page', 'link_category', 'link_count'])

