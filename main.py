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

clickstream_sample_df.show()

# Read the target directory (`./cleaned/clickstream/`) into a DataFrame (`clickstream`)
clickstream = spark.read.option('delimiter', '\t').option('header', True).option('inferSchema', True).csv('./cleaned/clickstream/')

# Display the DataFrame to the notebook
clickstream.show()


# Drop target columns
clickstream = clickstream.drop('language_code')

# Display the first few rows of the DataFrame
clickstream.show(3)
# Display the new schema in the notebook
clickstream.printSchema()


# Rename `referrer` and `resource` to `source_page` and `target_page`
clickstream = clickstream.withColumnRenamed('referrer', 'source_page')
clickstream = clickstream.withColumnRenamed('resource', 'target_page')
  
# Display the first few rows of the DataFrame
clickstream.show(3)
# Display the new schema in the notebook
clickstream.printSchema()


# Filter and sort the DataFrame using PySpark DataFrame method
clickstream.select([ 'source_page', 'target_page','link_category', 'click_count']).filter('target_page == "Hanging_Gardens_of_Babylon"').orderBy('click_count').show()


# Filter and sort the DataFrame using SQL
q = '''SELECT * FROM clickstream WHERE target_page = "Hanging_Gardens_of_Babylon"
ORDER BY click_count;'''

spark.sql(q).show()



