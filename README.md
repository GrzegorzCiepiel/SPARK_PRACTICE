# Analyzing Wikipedia Clickstream Data
SPARK practice and comparing python and sql query data methods


### Import Libraries


```python
from pyspark.sql import SparkSession
```

## Task Group 1 - Introduction to Clickstream Data

### Task 1
Create a new `SparkSession` and assign it to a variable named `spark`.


```python
# Create a new SparkSession
spark = SparkSession.builder.getOrCreate()
```

### Task 2

Create an RDD from a list of sample clickstream counts and save it as `clickstream_counts_rdd`.


```python
# Sample clickstream counts
sample_clickstream_counts = [
    ["other-search", "Hanging_Gardens_of_Babylon", "external", 47000],
    ["other-empty", "Hanging_Gardens_of_Babylon", "external", 34600],
    ["Wonders_of_the_World", "Hanging_Gardens_of_Babylon", "link", 14000],
    ["Babylon", "Hanging_Gardens_of_Babylon", "link", 2500]
]

# Create RDD from sample data
clickstream_counts_rdd = spark.sparkContext.parallelize(sample_clickstream_counts)
```

### Task 3

Using the RDD from the previous step, create a DataFrame named `clickstream_sample_df`


```python
# Create a DataFrame from the RDD of sample clickstream counts
clickstream_sample_df = clickstream_counts_rdd.toDF(['source_page', 'target_page', 'link_category', 'link_count'])

# Display the DataFrame to the notebook
clickstream_sample_df.show()
```

    +--------------------+--------------------+-------------+----------+
    |         source_page|         target_page|link_category|link_count|
    +--------------------+--------------------+-------------+----------+
    |        other-search|Hanging_Gardens_o...|     external|     47000|
    |         other-empty|Hanging_Gardens_o...|     external|     34600|
    |Wonders_of_the_World|Hanging_Gardens_o...|         link|     14000|
    |             Babylon|Hanging_Gardens_o...|         link|      2500|
    +--------------------+--------------------+-------------+----------+
    


## Task Group 2 - Inspecting Clickstream Data

### Task 4

Read the files in `./cleaned/clickstream/` into a new Spark DataFrame named `clickstream` and display the first few rows of the DataFrame in the notebook


```python
# Read the target directory (`./cleaned/clickstream/`) into a DataFrame (`clickstream`)
clickstream = spark.read.option('delimiter', '\t').option('header', True).option('inferSchema', True).csv('./cleaned/clickstream/')

# Display the DataFrame to the notebook
clickstream.show()
```

    +-------------------+--------------------+-------------+-------------+-----------+
    |           referrer|            resource|link_category|language_code|click_count|
    +-------------------+--------------------+-------------+-------------+-----------+
    |   Daniel_Day-Lewis|      Phantom_Thread|         link|           en|      43190|
    |     other-internal|      Phantom_Thread|     external|           en|      21683|
    |        other-empty|      Phantom_Thread|     external|           en|     169532|
    |90th_Academy_Awards|      Phantom_Thread|         link|           en|      40449|
    |       other-search|      Phantom_Thread|     external|           en|     536940|
    |       other-search|Tara_Grinstead_mu...|     external|           en|      30041|
    |       other-search|      Yossi_Benayoun|     external|           en|      11045|
    |        other-empty|       Parthiv_Patel|     external|           en|      11481|
    |       other-search|       Parthiv_Patel|     external|           en|      34953|
    |        other-empty|   Cosimo_de'_Medici|     external|           en|      16418|
    |       other-search|   Cosimo_de'_Medici|     external|           en|      22190|
    |       other-search|University_of_Geo...|     external|           en|      29963|
    |        other-empty|University_of_Geo...|     external|           en|      17325|
    |       other-search|Carbon_monoxide_d...|     external|           en|      13617|
    |        other-empty|      Marissa_Ribisi|     external|           en|      18979|
    |             Shinee|Kim_Jong-hyun_(si...|         link|           en|      24433|
    |       other-search|Kim_Jong-hyun_(si...|     external|           en|     162466|
    |        other-empty|Kim_Jong-hyun_(si...|     external|           en|      60193|
    |        other-empty|         Hello_Kitty|     external|           en|      10674|
    |       other-search|         Hello_Kitty|     external|           en|      23726|
    +-------------------+--------------------+-------------+-------------+-----------+
    only showing top 20 rows
    


### Task 5

Print the schema of the DataFrame in the notebook.


```python
# Display the schema of the `clickstream` DataFrame to the notebook
clickstream.printSchema()
```

    root
     |-- referrer: string (nullable = true)
     |-- resource: string (nullable = true)
     |-- link_category: string (nullable = true)
     |-- language_code: string (nullable = true)
     |-- click_count: integer (nullable = true)
    


### Task 6

Drop the `language_code` column from the DataFrame and display the new schema in the notebook.


```python
# Drop target columns
clickstream = clickstream.drop('language_code')

# Display the first few rows of the DataFrame
clickstream.show(3)
# Display the new schema in the notebook
clickstream.printSchema()
```

    +----------------+--------------+-------------+-----------+
    |        referrer|      resource|link_category|click_count|
    +----------------+--------------+-------------+-----------+
    |Daniel_Day-Lewis|Phantom_Thread|         link|      43190|
    |  other-internal|Phantom_Thread|     external|      21683|
    |     other-empty|Phantom_Thread|     external|     169532|
    +----------------+--------------+-------------+-----------+
    only showing top 3 rows
    
    root
     |-- referrer: string (nullable = true)
     |-- resource: string (nullable = true)
     |-- link_category: string (nullable = true)
     |-- click_count: integer (nullable = true)
    


### Task 7

Rename `referrer` and `resource` to `source_page` and `target_page`, respectively,


```python
# Rename `referrer` and `resource` to `source_page` and `target_page`
clickstream = clickstream.withColumnRenamed('referrer', 'source_page')
clickstream = clickstream.withColumnRenamed('resource', 'target_page')
  
# Display the first few rows of the DataFrame
clickstream.show(3)
# Display the new schema in the notebook
clickstream.printSchema()
```

    +----------------+--------------+-------------+-----------+
    |     source_page|   target_page|link_category|click_count|
    +----------------+--------------+-------------+-----------+
    |Daniel_Day-Lewis|Phantom_Thread|         link|      43190|
    |  other-internal|Phantom_Thread|     external|      21683|
    |     other-empty|Phantom_Thread|     external|     169532|
    +----------------+--------------+-------------+-----------+
    only showing top 3 rows
    
    root
     |-- source_page: string (nullable = true)
     |-- target_page: string (nullable = true)
     |-- link_category: string (nullable = true)
     |-- click_count: integer (nullable = true)
    


## Task Group 3 - Querying Clickstream Data

### Task 8

Add the `clickstream` DataFrame as a temporary view named `clickstream` to make the data queryable with `sparkSession.sql()`


```python
# Create a temporary view in the metadata for this `SparkSession` 
clickstream.createOrReplaceTempView('clickstream')
```

### Task 9

Filter the dataset to entries with `Hanging_Gardens_of_Babylon` as the `target_page` and order the result by `click_count` using PySpark DataFrame methods.


```python
# Filter and sort the DataFrame using PySpark DataFrame method
clickstream.select([ 'source_page', 'target_page','link_category', 'click_count']).filter('target_page == "Hanging_Gardens_of_Babylon"').orderBy('click_count').show()
```

    +--------------------+--------------------+-------------+-----------+
    |         source_page|         target_page|link_category|click_count|
    +--------------------+--------------------+-------------+-----------+
    |Seven_Wonders_of_...|Hanging_Gardens_o...|         link|      12296|
    |Wonders_of_the_World|Hanging_Gardens_o...|         link|      14668|
    |         other-empty|Hanging_Gardens_o...|     external|      34619|
    |        other-search|Hanging_Gardens_o...|     external|      47088|
    +--------------------+--------------------+-------------+-----------+
    


### Task 10

Perform the same analysis as the previous exercise using a SQL query. 


```python
# Filter and sort the DataFrame using SQL
q = '''SELECT * FROM clickstream WHERE target_page = "Hanging_Gardens_of_Babylon"
ORDER BY click_count;'''

spark.sql(q).show()
```

    +--------------------+--------------------+-------------+-----------+
    |         source_page|         target_page|link_category|click_count|
    +--------------------+--------------------+-------------+-----------+
    |Seven_Wonders_of_...|Hanging_Gardens_o...|         link|      12296|
    |Wonders_of_the_World|Hanging_Gardens_o...|         link|      14668|
    |         other-empty|Hanging_Gardens_o...|     external|      34619|
    |        other-search|Hanging_Gardens_o...|     external|      47088|
    +--------------------+--------------------+-------------+-----------+
    


### Task 11

Calculate the sum of `click_count` grouped by `link_category` using PySpark DataFrame methods.


```python
# Aggregate the DataFrame using PySpark DataFrame Methods 
clickstream.select(['link_category', 'click_count']).groupBy('link_category').sum().show()
```

    +-------------+----------------+
    |link_category|sum(click_count)|
    +-------------+----------------+
    |         link|        97805811|
    |        other|         9338172|
    |     external|      3248677856|
    +-------------+----------------+
    


### Task 12

Perform the same analysis as the previous exercise using a SQL query.


```python
# Aggregate the DataFrame using SQL
q ='''SELECT link_category, SUM(click_count)
FROM clickstream
GROUP BY link_category;'''
spark.sql(q).show()
```

    +-------------+----------------+
    |link_category|sum(click_count)|
    +-------------+----------------+
    |         link|        97805811|
    |        other|         9338172|
    |     external|      3248677856|
    +-------------+----------------+
    

