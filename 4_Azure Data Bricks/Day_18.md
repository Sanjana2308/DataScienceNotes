# Day 18

## Structured Streaming
**sales_data.csv**
```csv
OrderID,OrderDate,CustomerID,Product,Quantity,Price
1001,2024-01-15,C001,Widget A,10,25.50
1002,2024-01-16,C002,Widget B,5,15.75
1003,2024-01-16,C001,Widget C,8,22.50
1004,2024-01-17,C003,Widget A,15,25.50
1005,2024-01-18,C004,Widget D,7,30.00
1006,2024-01-19,C002,Widget B,9,15.75
1007,2024-01-20,C005,Widget C,12,22.50
1008,2024-01-21,C003,Widget A,10,25.50
```

**customer_data.json**
```json
[
    {
      "CustomerID": "C001",
      "CustomerName": "John Doe",
      "Region": "North",
      "SignupDate": "2022-07-01"
    },
    {
      "CustomerID": "C002",
      "CustomerName": "Jane Smith",
      "Region": "South",
      "SignupDate": "2023-02-15"
    },
    {
      "CustomerID": "C003",
      "CustomerName": "Emily Johnson",
      "Region": "East",
      "SignupDate": "2021-11-20"
    },
    {
      "CustomerID": "C004",
      "CustomerName": "Michael Brown",
      "Region": "West",
      "SignupDate": "2022-12-05"
    },
    {
      "CustomerID": "C005",
      "CustomerName": "Linda Davis",
      "Region": "North",
      "SignupDate": "2023-03-10"
    }
  ]
```

**Importing File**
```python
dbutils.fs.cp("file:/Workspace/Shared/sales_data.csv", "dbfs:/FileStore/streaming/input/sales_data.csv")
dbutils.fs.cp("file:/Workspace/Shared/customer_data.json", "dbfs:/FileStore/streaming/input/customer_data.json")
```

**Reading Streaming data**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list

# Initialize SparkSession
spark = SparkSession.builder\
        .appName("StructuredStreamingExample")\
        .getOrCreate()

# Define the schema for the CSV data
sales_schema = "OrderID INT, OrderDate STRING, CustomerID STRING, Product STRING, Quantity INT, Price DOUBLE"

# Read streaming data from CSV files
df_sales_stream = spark.readStream\
    .format("csv")\
    .option("header", "true")\
    .schema(sales_schema)\
    .load("dbfs:/FileStore/streaming/input/")

# Define the schema for JSON data
customer_schema = "CustomerID STRING, CustomerName STRING, Region STRING, SignupDate STRING"

# Read streaming data from JSON files
df_customer_stream = spark.readStream\
    .format("json")\
    .schema(customer_schema)\
    .load("dbfs:/FileStore/streaming/input/")

df_customer_stream.printSchema()
df_sales_stream.printSchema()
```

**Transforming streaming data**: As soon as data is entered processing is done.

`Watermark` is used because without it we cannot read the data.

`readstream`: To read streaming data
`writestream` but it is called sink in technical terms

```python
from pyspark.sql.functions import current_date, datediff, to_timestamp
from pyspark.sql.functions import col

# Transform the sales data: Add a new column for the total amount
df_sales_transformed = df_sales_stream.select(
    col("OrderID"),
    to_timestamp(col("OrderDate"), "yyyy-MM-dd HH:mm:ss").alias("OrderDate"), # Convert OrderDate to TIMESTAMP
    col("Product"),
    col("Quantity"),
    col("Price"),
    (col("Quantity") * col("Price")).alias("TotalAmount")
)

print("Applied transformations to the sales data...")

# Add watermark to handle late data and perform an aggregation
df_sales_aggregated = df_sales_transformed\
                    .withWatermark("OrderDate", "1 day")\
                    .groupBy("Product")\
                    .agg({"TotalAmount": "sum"})

print("Aggregated sales data by product...")

# Transform the customer data: Add a new column for the number of years since signup
df_customers_transformed = df_customer_stream.withColumn(
    "YearsSinceSignup",
    datediff(current_date(), to_timestamp("SignupDate", "yyyy-MM-dd")).cast("int")/365
)

print("Applied transformations on customer data...")

```

**Writing the streaming data**
```python
# Write the aggregated sales data to a console sink for debugging
sales_query = df_sales_aggregated.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

print("Started streaming query to write aggregated sales data to console...")

# Wrie the transformed customer data to a console sink for debugging
customers_query = df_customers_transformed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

print("Started streaming query to write customer data to console...")
```
