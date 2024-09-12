# Day 16

## Using Matplotlib
For data.csv
```python
# Move the file from Workspace to DBFS
dbutils.fs.cp("file:/Workspace/Shared/data.csv", "dbfs:/FileStore/data.csv")

# Load the file from DBFS
df = spark.read.format("csv").option("header", "true").load("/FileStore/data.csv")
df.show()
```

**Plotting the graph**
```python
import matplotlib.pyplot as plt

# Convert to Pandas
df_pandas = df.groupBy("CARRIER").agg({"ARR_DELAY": "avg"}).toPandas()

# Plot using Matplotlib
df_pandas.plot(kind='bar',x="CARRIER", y="avg(ARR_DELAY)", legend=False)
plt.title("Average Arrival Delay by Airline")
plt.xlabel("Airline")
plt.ylabel("Average Delay (minutes)")
plt.show()
```

**Output:**
![Output](<../Images/Azure DataBricks/15_1.png>)

### Sales data
```csv
Date,Region,Product,Quantity,Price
2024-09-01,North,Widget A,10,25.50
2024-09-01,South,Widget B,5,15.75
2024-09-02,North,Widget A,12,25.50
2024-09-02,East,Widget C,8,22.50
2024-09-03,West,Widget A,15,25.50
2024-09-03,South,Widget B,20,15.75
2024-09-03,East,Widget C,10,22.50
2024-09-04,North,Widget D,7,30.00
2024-09-04,West,Widget B,9,15.75
```

**Loading the file:**
```python
# Move the file from Workspace to DBFS
dbutils.fs.cp("file:/Workspace/Shared/sales_data.csv", "dbfs:/FileStore/sales_data.csv")

# Load the file from DBFS
df_csv = spark.read.format("csv").option("header", "true").load("/FileStore/sales_data.csv")
df_csv.show()
```

**Converting the DataFrame to SQL Table and reading it**
```python
# Create a SQL Table from DataFrame
df_csv.write.saveAsTable("sales_tables")

# Read the table using Spark SQL
df_table = spark.read.table("sales_tables")

df_table.show()
```

**Converting the DataFrame to SQL Table and reading it**
We are converting the DataFrame into Delta table because of the following two reasons:
1. We are making a table in the Delta Lake
2. This form of table works better with the Parquet form file.
```python
# Writing data to a Delta table
df_csv.write.format("delta").mode("overwrite").save("/Workspace/Shared/sales_delta")

# Reading data from a Delta table
df_delta = spark.read.format("delta").load("/Workspace/Shared/sales_delta")
df_delta.show()

# Query Delta Table with SQL
df_query = spark.sql("SELECT * FROM sales_delta WHERE Quantity > 10")
df_query.show()
```

## Assignments
**Sample CSV Data (employee_data.csv)**:
```csv
EmployeeID,Name,Department,JoiningDate,Salary
1001,John Doe,HR,2021-01-15,55000
1002,Jane Smith,IT,2020-03-10,62000
1003,Emily Johnson,Finance,2019-07-01,70000
1004,Michael Brown,HR,2018-12-22,54000
1005,David Wilson,IT,2021-06-25,58000
1006,Linda Davis,Finance,2020-11-15,67000
1007,James Miller,IT,2019-08-14,65000
1008,Barbara Moore,HR,2021-03-29,53000
```

**Sample JSON Data (product_data.json):**
```json
[
    {
        "ProductID": 101,
        "ProductName": "Laptop",
        "Category": "Electronics",
        "Price": 1200,
        "Stock": 35
    },
    {
        "ProductID": 102,
        "ProductName": "Smartphone",
        "Category": "Electronics",
        "Price": 800,
        "Stock": 80
    },
    {
        "ProductID": 103,
        "ProductName": "Desk Chair",
        "Category": "Furniture",
        "Price": 150,
        "Stock": 60
    },
    {
        "ProductID": 104,
        "ProductName": "Monitor",
        "Category": "Electronics",
        "Price": 300,
        "Stock": 45
    },
    {
        "ProductID": 105,
        "ProductName": "Desk",
        "Category": "Furniture",
        "Price": 350,
        "Stock": 25
    }
]
```

## Assignment 1: Working with CSV Data (employee_data.csv)
### Tasks:
**Move the file from Workspace to DBFS**
```python
dbutils.fs.cp("file:/Workspace/Shared/employee_data.csv", "dbfs:/FileStore/employee_data.csv")

#Read the data from the csv file
df_csv=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/employee_data.csv")
df_csv.show()
```

**1. Load the CSV data:**
- Load the employee_data.csv file into a DataFrame.
- Display the first 10 rows and inspect the schema.
```python
df_employee = spark.read.csv('/FileStore/employee_data.csv', header=True, inferSchema=True)
df_employee.show(10)
df_employee.printSchema()
```

**2. Data Cleaning:**
- Remove rows where the Salary is less than 55,000.
- Filter the employees who joined after the year 2020.
```python
df_cleaned = df_employee.filter((df_employee['Salary'] >= 55000) & (df_employee['JoiningDate'] > '2020-01-01'))
df_cleaned.show()
```

**3. Data Aggregation:**
- Find the average salary by Department.
```python
df_avg_salary_by_dept = df_cleaned.groupBy('Department').agg({'Salary': 'avg'}).withColumnRenamed('avg(Salary)', 'AvgSalary')
df_avg_salary_by_dept.show()
```

- Count the number of employees in each Department.
```python
df_count_by_dept = df_cleaned.groupBy('Department').count().withColumnRenamed('count', 'EmployeeCount')
df_count_by_dept.show()
```

**4. Write the Data to CSV:**
- Save the cleaned data (from the previous steps) to a new CSV file.
```python
df_cleaned.coalesce(1).write.csv('/dbfs/FileStore/cleaned_employee_data.csv', header=True)
```

## Assignment 2: Working with JSON Data (product_data.json)
### Tasks:
```python
dbutils.fs.cp("file:/Workspace/Shared/product_data.json", "dbfs:/FileStore/product_data.json")
```

**1. Load the JSON data:**
- Load the product_data.json file into a DataFrame.
- Display the first 10 rows and inspect the schema.
```python
df = spark.read.option("multiline", "true").json("/FileStore/product_data.json")
df.show(10)
df.printSchema()
```

**2. Data Cleaning:**
- Remove rows where Stock is less than 30.
- Filter the products that belong to the "Electronics" category.
```python
df_cleaned_product = df.filter((df['Stock'] >= 30) & (df['Category'] == 'Electronics'))
df_cleaned_product.show()
```

**3. Data Aggregation:**
- Calculate the total stock for products in the "Furniture" category.
```python
df_total_furniture_stock = df.filter(df['Category'] == 'Furniture').groupBy('Category').agg({'Stock': 'sum'}).withColumnRenamed('sum(Stock)', 'TotalStock')
df_total_furniture_stock.show()
```

- Find the average price of all products in the dataset.
```python
df_avg_price = df.groupBy('Category').agg({'Price': 'avg'}).withColumnRenamed('avg(Price)', 'AvgPrice')
df_avg_price.show()
```

**4. Write the Data to JSON:**
- Save the cleaned and aggregated data into a new JSON file.
```python
df_cleaned_product.coalesce(1).write.json('/FileStore/cleaned_product_data.json')
```

## Assignment 3: Working with Delta Tables
### Tasks:

**Load employee.csv file data**
```python
df_employee = spark.read.csv('/FileStore/employee_data.csv', header=True, inferSchema=True).cache()
df_employee.show()
df_employee.printSchema()
```

**Load product_data.json file**
```python
df = spark.read.option("multiline", "true").json("/FileStore/product_data.json")
df.show(10)
df.printSchema()
```

**1. Convert CSV and JSON Data to Delta Format**
- Convert the employee_data.csv and product_data.json into Delta Tables.
- Save the Delta tables to a specified location.
df_employee.write.format("delta").mode("overwrite").save("/dbfs/FileStore/delta/employee_data")
df.write.format("delta").mode("overwrite").save("/dbfs/FileStore/delta/product_data")

**2. Register Delta Tables as SQL Tables**
- Register both the employee and product Delta tables as SQL tables.
```python
spark.sql("CREATE TABLE IF NOT EXISTS employee_delta USING DELTA LOCATION '/dbfs/FileStore/delta/employee_data'")
spark.sql("CREATE TABLE IF NOT EXISTS product_delta USING DELTA LOCATION '/dbfs/FileStore/delta/product_data'")
```

**3. Data Modifications with Delta Tables**
- Perform an update operation on the employee Delta table: Increase the
salary by 5% for all employees in the IT department.
```python
spark.sql("UPDATE employee_delta SET Salary = Salary * 1.05 WHERE Department = 'IT'")
```

- Perform a delete operation on the product Delta table: Delete products
where the stock is less than 40.
```python
spark.sql("DELETE FROM product_delta WHERE Stock < 40")
```

**4. Time Travel with Delta Tables:**
- Query the product Delta table to show its state before the delete operation (use time travel).
```python
df_product_version_before_delete = spark.sql("SELECT * FROM product_delta VERSION AS OF 0")
df_product_version_before_delete.show()
```

- Retrieve the version of the employee Delta table before the salary update.
```python
df_employee_version_before_update = spark.sql("SELECT * FROM employee_delta VERSION AS OF 0")
df_employee_version_before_update.show()
```

**5. Query Delta Tables:**
- Query the employee Delta table to find the employees in the Finance department.
```python
df_finance_employees = spark.sql("SELECT * FROM employee_delta WHERE Department = 'Finance'")
df_finance_employees.show()
```

- Query the product Delta table to find all products in the Electronics category with a price greater than 500.
```python
df_expensive_electronics = spark.sql("SELECT * FROM product_delta WHERE Category = 'Electronics' AND Price > 500")
df_expensive_electronics.show()
```

### Sample Code Snippets
**1. Loading and Writing CSV Data**
```python
# Load CSV data
df_csv = spark.read.format("csv").option("header",
"true").load("/path_to_file/employee_data.csv")
df_csv.show()

# Filter and clean data
df_cleaned = df_csv.filter(df_csv['Salary'] > 55000)

# Write back to CSV
df_cleaned.write.format("csv").option("header",
"true").save("/path_to_output/cleaned_employee_data.csv")
```

**2. Loading and Writing JSON Data**
```python
# Load JSON data
df_json = spark.read.format("json").load("/path_to_file/product_data.json")
df_json.show()

# Filter and clean data
df_filtered = df_json.filter(df_json['Stock'] > 30)

# Write back to JSON
df_filtered.write.format("json").save("/path_to_output/filtered_product_data.json")
```

**3. Delta Table Operations**
```python
# Write DataFrame to Delta Table
df_csv.write.format("delta").mode("overwrite").save("/path_to_delta/employee_delta")

# Read Delta Table
df_delta = spark.read.format("delta").load("/path_to_delta/employee_delta")
df_delta.show()

# Update Delta Table
df_delta.createOrReplaceTempView("employee_delta")
spark.sql("""
UPDATE employee_delta
SET Salary = Salary * 1.05
WHERE Department = 'IT'
""")

# Time Travel - Query a Previous Version
df_version = spark.read.format("delta").option("versionAsOf",
1).load("/path_to_delta/employee_delta")
df_version.show()
```

#### Deliverables
- CSV data with cleaned and filtered employee information.
- JSON data with filtered and aggregated product information.
- Delta tables for both employee and product data with updated, deleted, and time-traveled versions.
- SQL queries and results for Delta tables.

## Database vs Data Warehouse vs Data Lake vs Delta Lake
![alt text](<../Images/Azure DataBricks/15_2.png>)

## 
**employee_data.csv**:
```csv
EmployeeID,Name,Department,JoiningDate,Salary
1001,John Doe,HR,2021-01-15,55000
1002,Jane Smith,IT,2020-03-10,62000
1003,Emily Johnson,Finance,2019-07-01,70000
1004,Michael Brown,HR,2018-12-22,54000
1005,David Wilson,IT,2021-06-25,58000
1006,Linda Davis,Finance,2020-11-15,67000
1007,James Miller,IT,2019-08-14,65000
1008,Barbara Moore,HR,2021-03-29,53000
```

**product_data.json**:
```json
[
  {
    "ProductID": 101,
    "ProductName": "Laptop",
    "Category": "Electronics",
    "Price": 1200,
    "Stock": 35
  },
  {
    "ProductID": 102,
    "ProductName": "Smartphone",
    "Category": "Electronics",
    "Price": 800,
    "Stock": 80
  },
  {
    "ProductID": 103,
    "ProductName": "Desk Chair",
    "Category": "Furniture",
    "Price": 150,
    "Stock": 60
  },
  {
    "ProductID": 104,
    "ProductName": "Monitor",
    "Category": "Electronics",
    "Price": 300,
    "Stock": 45
  },
  {
    "ProductID": 105,
    "ProductName": "Desk",
    "Category": "Furniture",
    "Price": 350,
    "Stock": 25
  }
]
```

### CSV file to DELTA
```python
# Move the file from Workspace to DBFS
dbutils.fs.cp("file:/Workspace/Shared/employee_data.csv", "dbfs:/FileStore/employee_data.csv")

# Load CSV data into a DataFrame
df_employee = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/employee_data.csv")

# Write DataFrame to Delta format
df_employee.write.format("delta").save("/FileStore/delta/employee_data")
```

### JSON file to DELTA
**Cell - 1**
```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Define schema for JSON File
schema = StructType([
    StructField("ProductID", IntegerType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Stock", IntegerType(), True),
])
```

**Cell - 2**
```python
# MOve the file from Workspace to DBFS
dbutils.fs.cp("file:/Workspace/Shared/product_data.json", "dbfs:/FileStore/product_data.json")
```

**Cell - 3**
```python
# Load JSON data with schema
df_product = spark.read.format("json").schema(schema).load("dbfs:/FileStore/product_data.json")
df_product.show()
```

**Cell - 4**
```python
# Create a temp view for SQL Operations
df_product.createOrReplaceTempView("product_view")

# Create a Delta table from the view
spark.sql("""
    CREATE TABLE delta_product_table
    USING DELTA
    AS SELECT * FROM product_view    
""")
```

```csv
EmployeeID,Name,Department,JoiningDate,Salary
1001,John Doe,HR,2021-01-15,58000
1009,Sarah Adams,Marketing,2021-09-01,60000
1010,Robert King,IT,2022-01-10,62000
```

![alt text](<../Images/Azure DataBricks/15_3.png>)


