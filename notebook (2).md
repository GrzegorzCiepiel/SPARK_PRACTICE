# PySpark and Big Data Project

## Task Group 1 - Analyzing Common Crawl Data with RDDs

### Task 1

Initialize a new Spark Context and read in the domain graph as an RDD.


```python
# Import required modules
from pyspark.sql import SparkSession

# Create a new SparkSession
spark = SparkSession \
    .builder \
    .getOrCreate()

# Get SparkContext
sc = spark.sparkContext
```


```python
# Read Domains CSV File into an RDD
common_crawl_domain_counts = sc.textFile('./crawl/cc-main-limited-domains.csv')

# Display first few domains from the RDD
common_crawl_domain_counts.take(10)
```




    ['367855\t172-in-addr\tarpa\t1',
     '367856\taddr\tarpa\t1',
     '367857\tamphic\tarpa\t1',
     '367858\tbeta\tarpa\t1',
     '367859\tcallic\tarpa\t1',
     '367860\tch\tarpa\t1',
     '367861\td\tarpa\t1',
     '367862\thome\tarpa\t7',
     '367863\tiana\tarpa\t1',
     '367907\tlocal\tarpa\t1']



### Task 2

Apply `fmt_domain_graph_entry` over `common_crawl_domain_counts` and save the result as a new RDD named `formatted_host_counts`.


```python
def fmt_domain_graph_entry(entry):
    """
    Formats a Common Crawl domain graph entry. Extracts the site_id, 
    top-level domain (tld), domain name, and subdomain count as seperate items.
    """

    # Split the entry on delimiter ('\t') into site_id, domain, tld, and num_subdomains
    site_id, domain, tld, num_subdomains = entry.split('\t')        
    return int(site_id), domain, tld, int(num_subdomains)
```


```python
# Apply `fmt_domain_graph_entry` to the raw data RDD
formatted_host_counts = common_crawl_domain_counts.map(lambda x: fmt_domain_graph_entry(x))

# Display the first few entries of the new RDD
formatted_host_counts.take(10)
```




    [(367855, '172-in-addr', 'arpa', 1),
     (367856, 'addr', 'arpa', 1),
     (367857, 'amphic', 'arpa', 1),
     (367858, 'beta', 'arpa', 1),
     (367859, 'callic', 'arpa', 1),
     (367860, 'ch', 'arpa', 1),
     (367861, 'd', 'arpa', 1),
     (367862, 'home', 'arpa', 7),
     (367863, 'iana', 'arpa', 1),
     (367907, 'local', 'arpa', 1)]



### Task 3

Apply `extract_subdomain_counts` over `common_crawl_domain_counts` and save the result as a new RDD named `host_counts`.


```python
def extract_subdomain_counts(entry):
    """
    Extract the subdomain count from a Common Crawl domain graph entry.
    """
    
    # Split the entry on delimiter ('\t') into site_id, domain, tld, and num_subdomains
    site_id, domain, tld, num_subdomains = entry.split('\t')
    
    # return ONLY the num_subdomains
    return int(num_subdomains)


# Apply `extract_subdomain_counts` to the raw data RDD
host_counts = common_crawl_domain_counts.map(lambda x: extract_subdomain_counts(x))

# Display the first few entries
host_counts.take(10)
```




    [1, 1, 1, 1, 1, 1, 1, 7, 1, 1]



### Task 4

Using `host_counts`, calculate the total number of subdomains across all domains in the dataset, save the result to a variable named `total_host_counts`.


```python
# Reduce the RDD to a single value, the sum of subdomains, with a lambda function
# as the reduce function
total_host_counts = host_counts.reduce(lambda x,y : x+y)

# Display result count
print(total_host_counts)
```

    595466


### Task 5

Stop the current `SparkSession` and `sparkContext` before moving on to analyze the data with SparkSQL


```python
# Stop the sparkContext and the SparkSession
spark.stop()
```

## Task Group 2 - Exploring Domain Counts with PySpark DataFrames and SQL

### Task 6

Create a new `SparkSession` and assign it to a variable named `spark`.


```python
from pyspark.sql import SparkSession

# Create a new SparkSession
spark = SparkSession.builder.getOrCreate()
```

### Task 7

Read `./crawl/cc-main-limited-domains.csv` into a new Spark DataFrame named `common_crawl`.


```python
# Read the target file into a DataFrame
common_crawl = spark.read.option('delimiter', '\t').option('inferSchema', True).csv('./crawl/cc-main-limited-domains.csv')


# Display the DataFrame to the notebook
common_crawl.show(10)
```

    +------+-----------+----+---+
    |   _c0|        _c1| _c2|_c3|
    +------+-----------+----+---+
    |367855|172-in-addr|arpa|  1|
    |367856|       addr|arpa|  1|
    |367857|     amphic|arpa|  1|
    |367858|       beta|arpa|  1|
    |367859|     callic|arpa|  1|
    |367860|         ch|arpa|  1|
    |367861|          d|arpa|  1|
    |367862|       home|arpa|  7|
    |367863|       iana|arpa|  1|
    |367907|      local|arpa|  1|
    +------+-----------+----+---+
    only showing top 10 rows
    


### Task 8

Rename the DataFrame's columns to the following: 

- site_id
- domain
- top_level_domain
- num_subdomains



```python
# Rename the DataFrame's columns with `withColumnRenamed()`
common_crawl = common_crawl.withColumnRenamed('_c0', 'site_id')
common_crawl = common_crawl.withColumnRenamed('_c1', 'domain')
common_crawl = common_crawl.withColumnRenamed('_c2', 'top_level_domain')
common_crawl = common_crawl.withColumnRenamed('_c3', 'num_subdomains')  
# Display the first few rows of the DataFrame and the new schema
common_crawl.show(10)

```

    +-------+-----------+----------------+--------------+
    |site_id|     domain|top_level_domain|num_subdomains|
    +-------+-----------+----------------+--------------+
    | 367855|172-in-addr|            arpa|             1|
    | 367856|       addr|            arpa|             1|
    | 367857|     amphic|            arpa|             1|
    | 367858|       beta|            arpa|             1|
    | 367859|     callic|            arpa|             1|
    | 367860|         ch|            arpa|             1|
    | 367861|          d|            arpa|             1|
    | 367862|       home|            arpa|             7|
    | 367863|       iana|            arpa|             1|
    | 367907|      local|            arpa|             1|
    +-------+-----------+----------------+--------------+
    only showing top 10 rows
    


## Task Group 3 - Reading and Writing Datasets to Disk

### Task 9

Save the `common_crawl` DataFrame as parquet files in a directory called `./results/common_crawl/`.


```python
# Save the `common_crawl` DataFrame to a series of parquet files
common_crawl.write.parquet('./results/common_crawl/', mode='overwrite')

```

### Task 10

Read `./results/common_crawl/` into a new DataFrame to confirm our DataFrame was saved properly.


```python
# Read from parquet directory
common_crawl_domains = spark.read.parquet('./results/common_crawl/')

# Display the first few rows of the DataFrame and the schema

common_crawl.show(10)
```

    +-------+-----------+----------------+--------------+
    |site_id|     domain|top_level_domain|num_subdomains|
    +-------+-----------+----------------+--------------+
    | 367855|172-in-addr|            arpa|             1|
    | 367856|       addr|            arpa|             1|
    | 367857|     amphic|            arpa|             1|
    | 367858|       beta|            arpa|             1|
    | 367859|     callic|            arpa|             1|
    | 367860|         ch|            arpa|             1|
    | 367861|          d|            arpa|             1|
    | 367862|       home|            arpa|             7|
    | 367863|       iana|            arpa|             1|
    | 367907|      local|            arpa|             1|
    +-------+-----------+----------------+--------------+
    only showing top 10 rows
    


## Task Group 4 - Querying Domain Counts with PySpark DataFrames and SQL

### Task 11

Create a local temporary view from `common_crawl_domains`


```python
# Create a temporary view in the metadata for this `SparkSession`
common_crawl_domains.createOrReplaceTempView('crawl')
```

### Task 12

Calculate the total number of domains for each top-level domain in the dataset.


```python
# Aggregate the DataFrame using DataFrame methods
common_crawl_domains.groupBy('top_level_domain').count().orderBy('count', ascending=False).show()

```

    +----------------+-----+
    |top_level_domain|count|
    +----------------+-----+
    |             edu|18547|
    |             gov|15007|
    |          travel| 6313|
    |            coop| 5319|
    |            jobs| 3893|
    |            post|  117|
    |             map|   34|
    |            arpa|   11|
    +----------------+-----+
    



```python
# Aggregate the DataFrame using SQL
spark.sql('''
    SELECT top_level_domain, COUNT(domain) as count
    FROM crawl
    GROUP BY 1
    ORDER BY 2 DESC;''').show()

```

    +----------------+-----+
    |top_level_domain|count|
    +----------------+-----+
    |             edu|18547|
    |             gov|15007|
    |          travel| 6313|
    |            coop| 5319|
    |            jobs| 3893|
    |            post|  117|
    |             map|   34|
    |            arpa|   11|
    +----------------+-----+
    


### Task 13

Calculate the total number of subdomains for each top-level domain in the dataset.


```python
# Aggregate the DataFrame using DataFrame methods
common_crawl_domains.groupBy('top_level_domain').sum('num_subdomains').orderBy('sum(num_subdomains)', ascending=False).show()

```

    +----------------+-------------------+
    |top_level_domain|sum(num_subdomains)|
    +----------------+-------------------+
    |             edu|             484438|
    |             gov|              85354|
    |          travel|              10768|
    |            coop|               8683|
    |            jobs|               6023|
    |            post|                143|
    |             map|                 40|
    |            arpa|                 17|
    +----------------+-------------------+
    



```python
# Aggregate the DataFrame using SQL
spark.sql('''
    SELECT top_level_domain, SUM(num_subdomains)
    FROM crawl
    GROUP BY 1
    ORDER BY 2 DESC''').show()

```

    +----------------+-------------------+
    |top_level_domain|sum(num_subdomains)|
    +----------------+-------------------+
    |             edu|             484438|
    |             gov|              85354|
    |          travel|              10768|
    |            coop|               8683|
    |            jobs|               6023|
    |            post|                143|
    |             map|                 40|
    |            arpa|                 17|
    +----------------+-------------------+
    


### Task 14

How many sub-domains does `nps.gov` have? Filter the dataset to that website's entry, display the columns `top_level_domain`, `domain`, and `num_subdomains` in your result.


```python
# Filter the DataFrame using DataFrame Methods
common_crawl_domains.select(['top_level_domain', 'domain', 'num_subdomains']).filter(common_crawl_domains.domain == 'nps').filter(common_crawl_domains.top_level_domain =='gov').show()

```

    +----------------+------+--------------+
    |top_level_domain|domain|num_subdomains|
    +----------------+------+--------------+
    |             gov|   nps|           178|
    +----------------+------+--------------+
    



```python
# Filter the DataFrame using SQL
spark.sql('''
    SELECT top_level_domain, domain, num_subdomains
    FROM crawl
    WHERE domain = 'nps'
    AND top_level_domain = "gov" ''').show()

```

    +----------------+------+--------------+
    |top_level_domain|domain|num_subdomains|
    +----------------+------+--------------+
    |             gov|   nps|           178|
    +----------------+------+--------------+
    


### Task 15

Close the `SparkSession` and underlying `sparkContext`.


```python
# Stop the notebook's `SparkSession` and `sparkContext`
spark.stop()
```


```python

```
