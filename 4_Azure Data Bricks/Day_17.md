# Day 17

## Working with Deltas

**-- Cell 1 --**<br>
**Move the file from Workspace to DBFS**
```python
dbutils.fs.cp("file:/Workspace/Shared/employee_updates.csv", "dbfs:/FileStore/employee_updates.csv")
```

**-- Cell 2 --**<br>
**Convert employees CSV data to Delta format**
```python
df_employee = spark.read.format("csv").option("header", "true").load("/FileStore/employee_data.csv")
df_employee.write.format("delta").mode("overwrite").save("/delta/employee_data")
```

**Convert employee updates CSV data to Delta format**
```python
df_employee_updates = spark.read.format("csv").option("header", "true").load("/FileStore/employee_updates.csv")

df_employee_updates.write.format("delta").mode("overwrite").save("/delta/employee_updates")
```

**-- Cell 3 --**<br>
**Load delta tables**
```python
df_employee = spark.read.format("delta").load("/delta/employee_data")
df_employee_updates = spark.read.format("delta").load("/delta/employee_updates")
```

**Create temporary views for SQL operations**
```python
df_employee.createOrReplaceTempView("delta_employee")
df_employee_updates.createOrReplaceTempView("employee_updates")
```

**-- Cell 4 --**<br>
```python
spark.sql("""
  MERGE INTO delta_employee AS target
  USING employee_updates AS source
  ON target.EmployeeID = source.EmployeeID
  WHEN MATCHED THEN UPDATE SET target.Salary = source.Salary, target.Department = source.Department
  WHEN NOT MATCHED THEN INSERT (EmployeeID, Name, Department, JoiningDate, Salary)
  VALUES (source.EmployeeID, source.Name, source.Department, source.JoiningDate, source.Salary)
""")
```

**-- Cell 5 --**<br>
**Query the Delta table to check if the data was updated or inserted correctly**
```python
spark.sql("SELECT * FROM delta_employee").show()
```

### Fragmentation and Defragmentation

![alt text](<../Images/Azure DataBricks/17_1.jpg>)

### Optimization
**-- Cell 1 --**
**Write the employee DataFrame to a Delta table**
```python
df_employee.write.format("delta").mode("overwrite").save("/delta/employee_data")
```

**-- Cell 2 --**
**Register with Delta table**
```python
spark.sql("CREATE TABLE IF NOT EXISTS delta_employee_table USING DELTA LOCATION '/delta/employee_data'")
```

**-- Cell 3 --**
**Optimize the Delta table**
```python
spark.sql("OPTIMIZE delta_employee_table")
```

### Gives history of Delta table
```python
spark.sql("DESCRIBE HISTORY delta_employee_table").show(truncate=False)
```

### ZOrder
Connects the data points together and optimizes the Delta Lake.

**-- Cell 1 --**
**Load CSV data into a DataFrame**
```python
df_employee = spark.read.format("csv").option("header", "true").load("/FileStore/employee_data.csv")
```

**Write DataFrame to a Delta table**
```python
df_employee.write.format("delta").mode("overwrite").save("/delta/employee_data")
```

**-- Cell 2 --**
```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS delta_employee_table
    USING DELTA
    LOCATION '/delta/employee_data'
""")

spark.sql("""
    OPTIMIZE delta_employee_table ZORDER BY EmployeeId
""")
```

### Vacuum
```python
spark.sql("""
    VACUUM delta_employee_table RETAIN 168 HOURS
""")
```

# Assignment

## Sales Data (sales_data.csv)
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

## Customer Data (customer_data.json)
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

## New Sales Data (new_sales_data.csv)
```csv
OrderID,OrderDate,CustomerID,Product,Quantity,Price
1009,2024-01-22,C006,Widget E,14,20.00
1010,2024-01-23,C007,Widget F,6,35.00
1002,2024-01-16,C002,Widget B,10,15.75
```

## Tasks
### Create Delta Tables Using 3 Methods
**Setting up the environment**
```python
dbutils.fs.cp("file:/Workspace/Shared/sales_data1.csv", "dbfs:/FileStore/sales_data1.csv")

dbutils.fs.cp("file:/Workspace/Shared/customer_data.json", "dbfs:/FileStore/customer_data.json")

dbutils.fs.cp("file:/Workspace/Shared/new_sales_data.csv", "dbfs:/FileStore/new_sales_data.csv")
```

**1. Load the sales_data.csv file into a DataFrame.**
```python
# Load sales_data.csv into DataFrame
sales_df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/sales_data1.csv")
```

**2. Write the DataFrame as a Delta Table.**
```python
sales_df.write.format("delta").mode("overwrite").save("/delta/sales_data")
```

**3. Load the customer_data.json file into a DataFrame.**
```python
from pyspark.sql.types import StructType, StructField, StringType, DateType

# Define schema for customer_data.json
customer_schema = StructType([
    StructField("CustomerID", StringType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("SignupDate", DateType(), True)
])

# Load the JSON data with the defined schema
customer_df = spark.read.format("json").schema(customer_schema).load("dbfs:/FileStore/customer_data.json")
customer_df.show()
```

**4. Write the DataFrame as a Delta Table.**
```python
customer_df.write.format("delta").mode("overwrite").save("/delta/customer_data")
```

**5. Convert an existing Parquet file into a Delta Table (For demonstration, use a Parquet file available in your workspace).**


### Data Management

**1. Load the new_sales_data.csv file into a DataFrame.**
```python
# Load new_sales_data.csv into DataFrame
new_sales_df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/new_sales_data.csv")
```

**2. Write the new DataFrame as a Delta Table.**
```python
new_sales_df.write.format("delta").mode("overwrite").save("/delta/new_sales_data")
```

**3. Perform a MERGE INTO operation to update and insert records into the existing Delta table.**
```python
from delta.tables import *

# Load existing Delta table
delta_table =spark.read.format("delta").load("/delta/sales_data")
delta_new_sales=spark.read.format("delta").load("/delta/new_sales_data")

# Create temporary views for SQL operations
delta_table.createOrReplaceTempView("delta_sales_data")
delta_new_sales.createOrReplaceTempView("new_sales_data")

# Merge new sales data into existing Delta Table
spark.sql("""
          MERGE INTO delta_sales_data AS target
          USING new_sales_data AS source
          ON target.OrderID = source.OrderID
          WHEN MATCHED THEN UPDATE SET target.OrderDate=source.OrderDate, target.CustomerID = source.CustomerID, target.Product=source.Product,
          target.Quantity=source.Quantity, target.Price=source.Price
          WHEN NOT MATCHED THEN INSERT (OrderID,OrderDate,CustomerID,Product,Quantity,Price)
          VALUES (source.OrderID, source.OrderDate, source.CustomerID, source.Product, source.Quantity, source.Price)
""")

spark.sql("SELECT * FROM delta_sales_data").show()

# Register delta table
spark.sql("CREATE TABLE IF NOT EXISTS delta_sales_table USING DELTA LOCATION '/delta/sales_data'")

# Check the schema of the Delta table
spark.read.format("delta").load("/delta/sales_data").printSchema()

```

### Optimize Delta Table
**1. Apply the OPTIMIZE command on the Delta Table and use Z-Ordering on an appropriate column.**
```python
spark.sql(" OPTIMIZE delta_sales_table ")

spark.sql("ALTER TABLE delta_sales_table ADD COLUMN CustomerID STRING")

spark.sql("OPTIMIZE delta_sales_table ZORDER BY (CustomerID)")
```

### Advanced Features

**1. Use DESCRIBE HISTORY to inspect the history of changes for a Delta Table.**
```python
spark.sql("DESCRIBE HISTORY delta_sales_table").show(truncate=False)
```

**2. Use VACUUM to remove old files from the Delta Table.**
```python
spark.sql("""
         VACUUM delta_sales_table RETAIN 168 HOURS
          """)
```

### Hands-on Exercises
**1. Using Delta Lake for Data Versioning:**
- Query historical versions of the Delta Table using Time Travel.
```python
historical_df = spark.read.format("delta").option("versionAsOf", 1).load("/delta/sales_data")
historical_df.show()
```
**2. Building a Reliable Data Lake with Delta Lake:**
- Implement schema enforcement and handle data updates with Delta Lake.
- Optimize data layout and perform vacuum operations to maintain storage
efficiency.
```python
new_sales_df.write.format("delta").mode("append").option("mergeSchema", "true").save("/path/to/delta/sales_data")
spark.sql("VACUUM delta_sales_table")
```

### Managed and Unmanaged Table

![alt text](<../Images/Azure DataBricks/17_2.png>)

**First change the cell language to SQL**
```sql
%sql
CREATE TABLE managed_table(
    id INT,
    name STRING
);
```

```sql
%sql
CREATE EXTERNAL TABLE unmanaged_table(
    id INT,
    name STRING
)
LOCATION '/user/data/external_data/';
```


