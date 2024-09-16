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

### Jobs
Gets trigerred at a particular time everyday and then it points to a notebook. <br>
When we need continuously triggered data we create jobs.

```python
# Create sample sales data
data = {
    "OrderID": [1, 2, 3, 4],
    "OrderDate":["2024-01-01 10:00:00", "2024-01-02 11:00:00", "2024-01-03 12:00:00", "2024-01-04 13:00:00"],
    "CustomerID": ["C001", "C002", "C003", "C004"],
    "Product": ["ProductA", "ProductB", "ProductC", "ProductD"],
    "Quantity": [10, 20, 15, 5],
    "Price": [100.0, 200.0, 150.0, 50.0]
}

# Convert to DataFrame
df = pd.DataFrame(data)

# Save to CSV
csv_path = "dbfs/FileStore/sales_data.csv"
df.to_csv(csv_path, index=False)

print(f"Sample data saved to {csv_path}")
```

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# Initialize SparkSession
spark = SparkSession.builder.appName("StructuredStreamingExample").getOrCreate()

# Load data from CSV
df = spark.read.format("csv").option("header", "true").load("/FileStore/sales_data.csv")

print("Data Loaded successfully")

# Transform the data: Add a new column for total amount
df_transformed = df.withColumn("TotalAmount", col("Quantity").cast("int") * col("Price").cast("double"))

print("Data Transformed successfully")

# Write transformed data to a Delta table
df_transformed.write.format("delta").mode("overwrite").save("/delta/sales_data")

print("Transformed Data Written to Delta table successfully")
```

## Delta Live Table

![alt text](<../Images/Azure DataBricks/18_1.png>)

**-- Cell 1 --**
```python
import pandas as pd
```

**-- Cell 2 --**
```python
# Create sample sales data
sales_data = {
    "OrderID": [1, 2, 3, 4],
    "OrderDate": ["2024-01-01 10:00:00", "2024-01-02 11:00:00", "2024-01-03 12:00:00", "2024-01-04 13:00:00"],
    "CustomerID": ["C001", "C002", "C003", "C004"],
    "Product": ["ProductA", "ProductB", "ProductC", "ProductD"],
    "Quantity": [10, 20, 15, 5],
    "Price": [100.0, 200.0, 150.0, 50.0]
}

# Convert to DataFrame
df_sales = pd.DataFrame(sales_data)

# Save as CSV
csv_path = "dbfs/FileStore/sales_data_2.csv"
df_sales.to_csv(csv_path, index=False)

# Save as a Parquet
parquet_path = "/dbfs/FileStore/sales_data.parquet"
df_sales.to_parquet(parquet_path, index=False)
print(f"Sample data saved to {csv_path} and {parquet_path}")
```

**-- Cell 3 --**
```python
# Initialize SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder.appName("DeltaExample").getOrCreate()

# Load data from CSV
df_sales = spark.read.format("csv").option("header", "true").load("/FileStore/sales_data.csv")

# Transform the data: Add a new column for total amount
df_transformed = df_sales.withColumn("TotalAmount", col("Quantity").cast("int") * col("Price").cast("double"))

# Write transformed data to Delta table
delta_table_path = "/delta/sales_data"
df_transformed.write.format("delta").mode("overwrite").save(delta_table_path)

print("Delta table created and data written successfully")
```

**-- Cell 4 --**
```python
import dlt
```

**-- Cell 5 --**
```python
@dlt.table
def sales_data():
    df = spark.read.format("delta").load(delta_table_path)
    return df.select(
        col("OrderID"),
        col("OrderDate"),
        col("CustomerID"),
        col("Product"),
        col("Quantity"),
        col("Price"),
        (col("Quantity").cast("int") * col("Price").cast("double")).alias("TotalAmount")
    )

print("Delta table created and data written successfully")
```