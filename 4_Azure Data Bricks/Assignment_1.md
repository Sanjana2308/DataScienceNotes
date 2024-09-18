# Assignment

## Introduction to Databricks
**Task: Creating a Databricks Notebook**
- Create a new Databricks notebook.
- Use Python code to demonstrate basic operations (such as reading data and performing calculations).

**-- Cell 1 --**
```python
a=10
b=5
print(a+b)

data = [(1, "John", 28), (2, "Jane", 32), (3, "Jake", 24)]
df = spark.createDataFrame(data, ["ID", "Name", "Age"])
df.show()
```

## Setting Up Azure Databricks Workspace and Configuring Clusters

**Task: Configuring Clusters**
- Set up an Azure Databricks cluster.
- Configure the cluster with appropriate node sizes and autoscaling.
- Attach your notebook to the cluster and run basic Python code to verify configuration.
```
Setup done
```

## Real-Time Data Processing with Databricks

**Task: Implementing Databricks for Real-Time Data Processing**
- Create a notebook to process real-time data using streaming APIs.
- Use a dataset with 1 million records, including fields such as event_time , event_type , user_id , and amount , to simulate a streaming data scenario.
- Perform real-time data aggregation (e.g., summing up the amount per minute or
event type).

**-- Cell 2 --**
```python

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Task 3: Streaming Dataframe
streaming_df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()

# Adding more data to df
enriched_df = streaming_df.withColumn("event_type", expr("CASE WHEN rand() < 0.25 THEN 'purchase' WHEN rand() < 0.5 THEN 'click' WHEN rand() <  0.75 THEN 'view' ELSE 'add_to_cart' END")) \
    .withColumn("user_id", (rand() * 10000).cast("int")) \
    .withColumn("amount", (rand() * 1000).cast("double"))

# 1. Sum of amount per minute
amount_per_minute = enriched_df.withWatermark("timestamp", "1 minute").groupBy(window("timestamp", "1 minute")).agg(sum("amount").alias("total_amount"))

# 2. Sum of amount per event type
amount_per_event_type = enriched_df.groupBy("event_type").agg(sum("amount").alias("total_amount"))


# Start the streaming queries
query1 = amount_per_minute.writeStream.outputMode("complete").format("memory").queryName("amount_per_minute_table").trigger(processingTime="10 seconds").start()

query2 = amount_per_event_type.writeStream.outputMode("complete").format("memory").queryName("amount_per_event_type_table").trigger(processingTime="10 seconds") .start()


# Wait for the streams to finish
# query1.awaitTermination()
# query2.awaitTermination()
```

**-- Cell 3 --**
```python
query1.stop()
query2.stop()
display(spark.sql("SELECT * FROM amount_per_minute_table"))
display(spark.sql("SELECT * FROM amount_per_event_type_table"))
```

## Data Exploration and Visualization in Databricks
**Task: Visualizing Data in Databricks**
- Use a large dataset of at least 10,000 rows, such as sales or transaction data, for exploratory data analysis (EDA).
- Create multiple visualizations, including bar charts and scatter plots, to explore relationships between variables.

**-- Cell 4 --**

```python
import pandas as pd
import matplotlib.pyplot as plt

dbutils.fs.cp("file:/Workspace/Shared/large_sales_data.csv","dbfs:/FileStore/large_sales_data.csv")

large_sales_df = spark.read.csv("dbfs:/FileStore/large_sales_data.csv", header=True, inferSchema=True).toPandas()

# Scatter plot of Total Revenue vs. Total Profit
plt.figure(figsize=(10, 6))
plt.scatter(large_sales_df["Total Revenue"],large_sales_df["Total Profit"], alpha=0.5)
plt.title("Total Revenue vs. Total Profit")
plt.xlabel("Total Revenue")
plt.ylabel("Total Profit")
plt.grid(True)
plt.show()
```

**-- Cell 5 --**
```python
# Aggregate data by Region
revenue_by_region = large_sales_df.groupby("Region")["Total Revenue"].sum().reset_index()

# Bar chart of Total Revenue by Region
plt.figure(figsize=(12, 8))
plt.bar(revenue_by_region["Region"], revenue_by_region["Total Revenue"], color='skyblue')
plt.title("Total Revenue by Region")
plt.xlabel("Region")
plt.ylabel("Total Revenue")
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()
```

**-- Cell 6 --**
```python
# Histogram of Units Sold
plt.figure(figsize=(10, 6))
plt.hist(large_sales_df["Units Sold"], bins=30, color='lightgreen', edgecolor='black')
plt.title("Distribution of Units Sold")
plt.xlabel("Units Sold")
plt.ylabel("Frequency")
plt.grid(True)
plt.show()
```

## Reading and Writing Data in Databricks
**Task: Reading and Writing Data in Various Formats**
- Read data from different formats: CSV, JSON, Parquet, and Delta.
- Use a large dataset with at least 100,000 rows containing customer and transaction data.
- Write the data to Delta format and other formats like Parquet and JSON.

**-- Cell 7 --**

```python
# Reading and writing data in various formats from CSV, JSON, and Parquet, Delta
dbutils.fs.cp("file:/Workspace/Shared/sales1_data.csv", "dbfs:/FileStore/sales1_data.csv")
dbutils.fs.cp("file:/Workspace/Shared/customer_data.json", "dbfs:/FileStore/customer_data.json")

# CSV
df_sales = spark.read.format("csv").option("header","true").load("dbfs:/FileStore/sales1_data.csv")

# JSON
df_customers = spark.read.option("multiline", "true").json("dbfs:/FileStore/customer_data.json")

# Parquet (Read and Write)
df_sales.write.format("parquet").mode("overwrite").save("dbfs:/FileStore/sales_parquet")
df_parquet = spark.read.format("parquet").load("dbfs:/FileStore/sales_parquet")

# Delta (Read and Write)
df_sales.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("dbfs:/FileStore/sales_delta")
sales_df = spark.read.format("delta").load("dbfs:/FileStore/sales_delta")
```

## Analyzing and Visualizing Streaming Data with Databricks
**Task: Analyzing Streaming Data**
- Use a dataset with at least 500,000 records for streaming data analysis.
- Visualize real-time data changes over time using Databricks' built-in visualization tools.

**-- Cell 8 --**

```python
# Task 7
from pyspark.sql.functions import *
from pyspark.sql.types import *

streaming_df = spark.readStream.format("rate").option("rowsPerSecond", 100).load()

# adding additional columns like event_type, user_id, and amount
streaming_data = streaming_df.withColumn("event_type", expr("CASE WHEN rand() < 0.5 THEN 'purchase' ELSE 'view' END")).withColumn("user_id", (rand() * 10000).cast("int")) \
    .withColumn("amount", (rand() * 1000).cast("double"))

# Sum of amounts per event type
aggregated_data = streaming_data.groupBy("event_type").agg(sum("amount").alias("total_amount"))

# streaming data to memory
query = aggregated_data.writeStream.outputMode("complete").format("memory").queryName("aggregated_streaming_data").start()

# Query the in-memory streaming table to visualize
display(spark.sql("SELECT * FROM aggregated_streaming_data"))

# Grouping data by a time window every 1 minute and event_type
windowed_data = streaming_data.withWatermark("timestamp", "1 minute").groupBy(window("timestamp", "1 minute"), "event_type").agg(sum("amount").alias("total_amount"))

# Write the windowed data to memory for visualization
query_windowed = windowed_data.writeStream.outputMode("complete").format("memory").queryName("windowed_streaming_data").start()

# Query the windowed data for visualization
display(spark.sql("SELECT * FROM windowed_streaming_data"))
```

**-- Cell 9 --**
```python
display(spark.sql("SELECT * FROM aggregated_streaming_data"))

display(spark.sql("SELECT * FROM windowed_streaming_data"))
```

**-- Cell 10 --**
```python
# Stop streaming queries
query.stop()
query_windowed.stop()
```

## Introduction to Databricks Delta Lake
**Task: Using Delta Lake for Data Versioning**
- Create a Delta table from a dataset of at least 500,000 rows.
- Update the table over time and demonstrate the ability to time travel between versions of the table.
- Optimize the Delta table and perform operations like compaction and vacuuming.

**-- Cell 11 --**

```python
dbutils.fs.cp("file:/Workspace/Shared/largest_sales_data.csv","dbfs:/FileStore/largest_sales_data.csv")

df_largest_sales = spark.read.csv("dbfs:/FileStore/largest_sales_data.csv", header=True, inferSchema=True)

# Creating a delta table
df_largest_sales.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("dbfs:/FileStore/largest_sales_data")

# Registering delta table
spark.sql("CREATE TABLE IF NOT EXISTS largest_sales_data USING DELTA LOCATION 'dbfs:/FileStore/largest_sales_data'")

# Updating
spark.sql("UPDATE largest_sales_data SET `TotalProfit` = `TotalProfit` * 1.5")

# Time Travel
spark.sql("SELECT * FROM largest_sales_data VERSION AS OF 0 LIMIT 10").show()
```

**-- Cell 12 --**
```python
# Optimize
spark.sql("OPTIMIZE largest_sales_data")

# Vacuum
spark.sql("VACUUM largest_sales_data RETAIN 168 HOURS")
```

## Managed and Unmanaged Tables
**Task: Creating Managed and Unmanaged Tables**
- Create a managed table with at least 100,000 rows of sales or transaction data.
- Create an unmanaged table with similar data, specifying an external location for the data.
- Perform basic operations like selecting, updating, and deleting records from both tables.

**-- Cell 13 --**

```python
# Managed and unmanaged table
dbutils.fs.cp("file:/Workspace/Shared/large_sales_data.csv","dbfs:/FileStore/large_sales_data.csv")

large_sale_df = spark.read.csv("dbfs:/FileStore/large_sales_data.csv", header=True, inferSchema=True)

large_sale_df.createOrReplaceTempView("large_sales_data_view")

# Managed Table
spark.sql("CREATE TABLE IF NOT EXISTS managed_sales_table USING DELTA AS SELECT * FROM large_sales_data_view")


spark.sql("SELECT * from managed_sales_table LIMIT 10").show()

spark.sql("UPDATE managed_sales_table SET Total_Revenue = Total_Revenue * 1.1")
```

**-- Cell 14 --**

```python
# Unmanaged Table
spark.sql("CREATE TABLE IF NOT EXISTS unmanage_sales_table USING DELTA LOCATION 'dbfs:/FileStore/unmanage_sales_table' AS SELECT * FROM large_sales_data_view")

spark.sql("SELECT * FROM unmanage_sales_table LIMIT 10").show()

spark.sql("UPDATE unmanage_sales_table SET Total_Revenue = Total_Revenue * 1.1")
```

## Views and Temporary Views
**Task: Working with Views in Databricks**
- Create a view, temporary view, and global temporary view from a dataset
containing at least 100,000 rows.
- Perform queries on these views and analyze their behavior across different
sessions.
- For datasets, you can use publicly available large datasets or generate synthetic data to meet the size requirements.

**-- Cell 15 --**

```python
# Temporary and Global View
sales_view_df = spark.read.csv("dbfs:/FileStore/large_sales_data.csv", header=True, inferSchema=True)

# Global View
sales_view_df.createOrReplaceGlobalTempView("global_sales_view")

# Temporary View
sales_view_df.createOrReplaceTempView("temp_sales_data_view")

spark.sql("SELECT * FROM temp_sales_data_view LIMIT 10").show()
spark.sql("SELECT * FROM global_temp.global_sales_view LIMIT 10").show()
```