# Coding Challenge

## Data Ingestion (1)

### Task 1: Vehicle Maintenance Data Ingestion
- Use the following CSV data representing vehicle maintenance records:
```csv
VehicleID,Date,ServiceType,ServiceCost,Mileage
V001,2024-04-01,Oil Change,50.00,15000
V002,2024-04-05,Tire Replacement,400.00,30000
V003,2024-04-10,Battery Replacement,120.00,25000
V004,2024-04-15,Brake Inspection,200.00,40000
V005,2024-04-20,Oil Change,50.00,18000
```

```python
dbutils.fs.cp(“file:/Workspace/Shared/vehicle_records.csv”,”dbfs:/FileStore/vehicle_records.csv”)
```

- Ingest this CSV data into a Delta table in Databricks.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import logging

spark = SparkSession.builder \
.appName("Vehicle Maintenance Data Ingestion") \
.getOrCreate()

logging.basicConfig(level=logging.INFO, filename="/dbfs/logs/vehicle_data_ingestion.log")

file_path = "dbfs:/FileStore/vehicle_records.csv"

try:
    df = spark.read.option("header", True).csv(file_path)

    df_clean = df.withColumn("ServiceCost", col("ServiceCost").cast("double")) \
    .withColumn("Mileage", col("Mileage").cast("int")) \
    .filter((col("ServiceCost").isNotNull()) & (col("Mileage") >= 0))

    df_invalid = df.subtract(df_clean)
    if df_invalid.count() > 0:
        logging.error(f"Invalid data found: {df_invalid.show(truncate=False)}")

    df_clean.write.format("delta").mode("overwrite").save("/delta/vehicle_records")
```

- Add error handling for cases where the file is missing or contains incorrect data, and log any such issues.

```python
except FileNotFoundError as e:
    logging.error(f"File not found: {e}")

except Exception as e:
    logging.error(f"Error during data ingestion: {e}")
```

### Task 2: Data Cleaning
- Clean the vehicle maintenance data:
  - Remove any duplicate records based on VehicleID and Date .
  - Save the cleaned data to a new Delta table.
```python
from pyspark.sql import functions as F

df = spark.read.format("delta").load("/delta/vehicle_maintenance")
```

- - Ensure that the ServiceCost and Mileage columns contain valid positive values.
```python
df_clean = df.filter((F.col("ServiceCost") > 0) & (F.col("Mileage") > 0))
```

- - Remove any duplicate records based on VehicleID and Date .
```python
df_clean = df_clean.dropDuplicates(["VehicleID", "Date"])
```

- - Save the cleaned data to a new Delta table.
```python
df_clean.write.format("delta").mode("overwrite").save("/delta/cleaned_vehicle_maintenance")
df_clean.show()
```

### Task 3: Vehicle Maintenance Analysis
- Create a notebook to analyze the vehicle maintenance data:
```python
df_cleaned = spark.read.format("delta").load("/delta/cleaned_vehicle_maintenance")
```

- - Calculate the total maintenance cost for each vehicle.
```python
df_total_cost = df_cleaned.groupBy("VehicleID") \
.agg(F.sum("ServiceCost").alias("TotalMaintenanceCost"))
df_total_cost.show()
```

- - Identify vehicles that have exceeded a certain mileage threshold (e.g., 30,000 miles) and might need additional services.
```python
mileage_threshold = 30000
df_high_mileage = df_cleaned.filter(F.col("Mileage") > mileage_threshold)
df_high_mileage.show()
```

- - Save the analysis results to a Delta table.
```python
df_total_cost.write.format("delta").mode("overwrite").save("/delta/total_maintenance_cost")
df_high_mileage.write.format("delta").mode("overwrite").save("/delta/high_mileage_vehicles")
```


### Task 5: Data Governance with Delta Lake
- Enable Delta Lake's data governance features:
```python
delta_table_path = "/delta/cleaned_vehicle_maintenance"
```

- - Use VACUUM to clean up old data from the Delta table.
```python
spark.sql(f"VACUUM '{delta_table_path}' RETAIN 168 HOURS")
```

- - Use DESCRIBE HISTORY to check the history of updates to the maintenance records.
```python
history_df = spark.sql(f"DESCRIBE HISTORY '{delta_table_path}'")
history_df.show(truncate=False)
```

---
---

## Data Ingestion (2)

### Task 1: Movie Ratings Data Ingestion
- Use the following CSV data to represent movie ratings by users:
```csv
UserID,MovieID,Rating,Timestamp
U001,M001,4,2024-05-01 14:30:00
U002,M002,5,2024-05-01 16:00:00
U003,M001,3,2024-05-02 10:15:00
U001,M003,2,2024-05-02 13:45:00
U004,M002,4,2024-05-03 18:30:00
```
```python
dbutils.fs.cp(“file:/Workspace/Shared/movie_ratings.csv”,”dbfs:/FileStore/movie_ratings.csv”)
```

- Ingest this CSV data into a Delta table in Databricks.
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import logging

spark = SparkSession.builder \
.appName("Movie Ratings Ingestion") \
.getOrCreate()

schema = StructType([
    StructField("UserID", StringType(), True),
    StructField("MovieID", StringType(), True),
    StructField("Rating", IntegerType(), True),
    StructField("Timestamp", StringType(), True)
])

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

try:
    movie_ratings_df = spark.read.csv("dbfs:/FileStore/movie_ratings.csv", schema=schema, header=True)
    
    movie_ratings_df = movie_ratings_df.withColumn("Timestamp", to_timestamp(col("Timestamp"),
    "yyyy-MM-dd HH:mm:ss"))

    cleaned_df = movie_ratings_df.dropna()
    cleaned_df.write.format("delta").mode("overwrite").save("/delta/movie_ratings")
    logger.info("Movie ratings data ingested successfully.")
```

- Ensure proper error handling for missing or inconsistent data, and log errors accordingly.
```python
except Exception as e:
logger.error(f"Error during data ingestion: {e}")
```


### Task 2: Data Cleaning

- Clean the movie ratings data:
- - Ensure that the Rating column contains values between 1 and 5.
- - Remove any duplicate entries (same UserID and MovieID ).
```python
cleaned_ratings_df = movie_ratings_df \
.filter((col("Rating") >= 1) & (col("Rating") <= 5)) \
.dropDuplicates(["UserID", "MovieID"])
```
- - Save the cleaned data to a new Delta table.
```python
cleaned_ratings_df.write.format("delta").mode("overwrite").save("/delta/cleaned_movie_ratings")
```

### Task 3: Movie Rating Analysis
- Create a notebook to analyze the movie ratings:
- - Calculate the average rating for each movie.
```python
avg_ratings_df = cleaned_ratings_df.groupBy("MovieID")\
    .agg({"Rating": "avg"})\
    .withColumnRenamed("avg(Rating)", "AverageRating")
```

- - Identify the movies with the highest and lowest average ratings.
```python
highest_rated = avg_ratings_df.orderBy(col("AverageRating").desc()).limit(1)
lowest_rated = avg_ratings_df.orderBy(col("AverageRating").asc()).limit(1)
```

- - Save the analysis results to a Delta table.
```python
avg_ratings_df.write.format("delta").mode("overwrite").save("/delta/movie_rating_analysis")
```

### Task 4: Time Travel and Delta Lake History
- Implement Delta Lake's time travel feature:
- - Perform an update to the movie ratings data (e.g., change a few ratings).
```python
cleaned_ratings_df = cleaned_ratings_df\
    .withColumn("Rating", when(col("MovieID") == "M001", 5)\
    .otherwise(col("Rating")))
cleaned_ratings_df.write.format("delta").mode("overwrite")\
.save("/delta/cleaned_movie_ratings")
```

- - Roll back to a previous version of the Delta table to retrieve the original ratings.
```python
rolled_back_df = spark.read.format("delta").option("versionAsOf", 1).load("/delta/cleaned_movie_ratings")
```

- - Use DESCRIBE HISTORY to view the history of changes to the Delta table.
```python
spark.sql("DESCRIBE HISTORY delta.` /delta/cleaned_movie_ratings`").show()
```

### Task 5: Optimize Delta Table
- Apply optimizations to the Delta table:
- - Implement Z-ordering on the MovieID column to improve query performance.
- - Use the OPTIMIZE command to compact the data and improve performance.
```python
spark.sql("OPTIMIZE delta.`/delta/cleaned_movie_ratings` ZORDER BY (MovieID)")
```
- - Use VACUUM to clean up older versions of the table.
```python
spark.sql("VACUUM delta.`/path/to/delta/cleaned_movie_ratings` RETAIN 168 HOURS")
```

## Data Ingestion (3)

### Task 1: Data Ingestion - Reading Data from Various Formats
1. **Ingest data from different formats** (CSV, JSON, Parquet, Delta table):
- **CSV Data**: Use the following CSV data to represent student information:
```csv
StudentID,Name,Class,Score
S001,Anil Kumar,10,85
S002,Neha Sharma,12,92
S003,Rajesh Gupta,11,78
```
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

student_schema = StructType([
    StructField("StudentID", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Class", StringType(), True),
    StructField("Score", IntegerType(), True)
])

data = [("S001", "Anil Kumar", "10", 85),
("S002", "Neha Sharma", "12", 92),
("S003", "Rajesh Gupta", "11", 78)
]
columns = ["StudentID", "Name", "Class", "Score"]

student_df = spark.createDataFrame(data, schema=student_schema)
student_df.show()
```

- **JSON Data**: Use the following JSON data to represent city information:
```json
[
{"CityID": "C001", "CityName": "Mumbai", "Population": 20411000},
{"CityID": "C002", "CityName": "Delhi", "Population": 16787941},
{"CityID": "C003", "CityName": "Bangalore", "Population": 8443675}
]
```
```python
city_rdd = spark.sparkContext.parallelize([city_json_data])

city_df = spark.read.json(city_rdd)
city_df.show()
```

- **Parquet Data**: Use a dataset containing data about hospitals stored in Parquet format. Write code to load this data into a DataFrame.
```python
dbutils.fs.cp(“file:/Workspace/hospital_data.parquet”,”dbfs:/FileStore/hospital_data.parquet”)

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
hospital_schema = StructType([
    StructField("HospitalID", StringType(), True),
    StructField("HospitalName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Beds", IntegerType(), True),
    StructField("Specialty", StringType(), True)
])

data =spark.read.parquet(“dbfs:/FileStore/hospital_data.parquet”).option(“header”,”true”)

parquet_file_path = "dbfs:/FileStore/hospital_data.parquet"
hospital_df = spark.read.parquet(parquet_file_path)
hospital_df.show()
```

- **Delta Table**: Load a Delta table containing hospital records, ensuring you include proper error handling in case the table does not exist.
```python
from delta.tables import DeltaTable
delta_table_path = "/delta/hospital_records"

try:
    if DeltaTable.isDeltaTable(spark, delta_table_path):
        hospital_records_df = spark.read.format("delta").load(delta_table_path)
        hospital_records_df.show()

    else:
        raise Exception("Delta table does not exist at the specified path.")

except Exception as e:
    print(f"Error: {str(e)}")
```

### Task 2: Writing Data to Various Formats
1. **Write data from the following DataFrames to different formats:**

- **CSV**: Write the student data (from Task 1) to a CSV file.
```python
csv_output_path = "dbfs:/FileStore/students.csv"
student_df.write.mode("overwrite").option("header", "true").csv(csv_output_path)
print(f"Student data written to: {csv_output_path}")
```

- **JSON**: Write the city data (from Task 1) to a JSON file.
```python
json_output_path = "dbfs:/FileStore/cities.json"
city_df.write.mode("overwrite").json(json_output_path)
print(f"City data written to: {json_output_path}")
```

- **Parquet**: Write the hospital data (from Task 1) to a Parquet file.
```python
parquet_output_path = "dbfs:/FileStore/hospitals_output.parquet"
hospital_df.write.mode("overwrite").parquet(parquet_output_path)
print(f"Hospital data written to: {parquet_output_path}")
```

- **Delta Table**: Write the hospital data to a Delta table.
```python
delta_table_path = "/delta/hospitals_delta"
hospital_df.write.format("delta").mode("overwrite").save(delta_table_path)
print(f"Hospital data written to Delta table at: {delta_table_path}")
```

### Task 3: Running One Notebook from Another
1. **Create two notebooks:**

- Notebook A: Ingest data from a CSV file, clean the data (remove duplicates, handle missing values), and save it as a Delta table.
```python
csv_input_path = "dbfs:/FileStore/students.csv"
student_df = spark.read.option("header", "true").csv(csv_input_path)

cleaned_student_df = student_df.dropDuplicates().na.drop()

delta_table_path = " /delta/students_delta"
cleaned_student_df.write.format("delta").mode("overwrite").save(delta_table_path)

print(f"Cleaned student data saved to Delta table at: {delta_table_path}")

dbutils.notebook.run("file:/Workspace/Users/Notebook B", 60)
```

- Notebook B: Perform analysis on the Delta table created in Notebook A (e.g., calculate the average score of students) and write the results to a new Delta table.
```python
delta_table_path = "/delta/students_delta"
students_delta_df = spark.read.format("delta").load(delta_table_path)
average_score_df = students_delta_df.groupBy().avg("Score")\
    .withColumnRenamed("avg(Score)", "Average_Score")

result_delta_table_path = "/delta/students_analysis_delta"
average_score_df.write.format("delta").mode("overwrite").save(result_delta_table_path)

print(f"Analysis results saved to Delta table at: {result_delta_table_path}")
```

2. **Run Notebook B from Notebook A:**
- Implement the logic to call and run Notebook B from within Notebook A.

### Task 4: Databricks Ingestion
1. **Read data from the following sources**:
- CSV file from Azure Data Lake.
```python
csv_file_path = "dbfs:/FileStore/students.csv"
csv_df = spark.read.option("header", "true").csv(csv_file_path)
```

- JSON file stored on Databricks FileStore.
```python
json_file_path = "dbfs:/FileStore/cities.json"
json_df = spark.read.json(json_file_path)
```

- Parquet file from an external data source (e.g., AWS S3).
```python
parquet_file_path = "s3:/FileStore/hospitals.parquet"
parquet_df = spark.read.parquet(parquet_file_path)
```

- Delta table stored in a Databricks-managed database.
```python
delta_table_path = "/delta/hospitals_delta"
delta_df = spark.read.format("delta").load(delta_table_path)
```


2. **Write the cleaned data** to each of the formats listed above (CSV, JSON, Parquet, and Delta) after performing some basic transformations (e.g., filtering rows, calculating totals).
```python
filtered_csv_df = csv_df.filter(csv_df["Score"] > 80)
filtered_csv_df.write.mode("overwrite").option("header", "true")\
        .csv("dbfs:/FileStore/filtered_students.csv")

json_df.write.mode("overwrite").json("dbfs:/FileStore/cleaned_cities.json")

parquet_df.write.mode("overwrite").parquet("dbfs:/FileStore/cleaned_hospitals.parquet")

delta_df.write.format("delta").mode("overwrite").save("/delta/cleaned_hospitals_delta")

print("Data written to CSV, JSON, Parquet, and Delta formats.")
```

### Additional Tasks:
- Optimization Task: Once the data is written to a Delta table, optimize it using Delta Lake's OPTIMIZE command.
```python
delta_table_path = "/delta/hospitals_delta"
spark.sql(f"OPTIMIZE delta.`{delta_table_path}`")
print(f"Delta table at {delta_table_path} optimized.")
```

- Z-ordering Task: Apply Z-ordering on the CityName or Class columns for faster querying.
```python
spark.sql(f"OPTIMIZE delta.`{delta_table_path}` ZORDER BY (CityName)")
print(f"Z-ordering applied on 'CityName' column for faster querying.")
```

- Vacuum Task: Use the VACUUM command to clean up old versions of the Delta table.
```python
spark.sql(f"VACUUM delta.`{delta_table_path}` RETAIN 168 HOURS")
print(f"Vacuum operation completed for Delta table at {delta_table_path}.")
```

---

## Exercise 1: Creating a Complete ETL Pipeline using Delta Live Tables (DLT)
**Objective:**
Learn how to create an end-to-end ETL pipeline using Delta Live Tables.

### Tasks:
1. **Create Delta Live Table (DLT) Pipeline:**
- Set up a DLT pipeline for processing transactional data. Use sample data representing daily customer transactions.

```csv
TransactionID,TransactionDate,CustomerID,Product,Quantity,Price
1,2024-09-01,C001,Laptop,1,1200
2,2024-09-02,C002,Tablet,2,300
3,2024-09-03,C001,Headphones,5,50
4,2024-09-04,C003,Smartphone,1,800
5,2024-09-05,C004,Smartwatch,3,200
```

- Define the pipeline steps:
- - Step 1: Ingest raw data from CSV files.
- - Step 2: Apply transformations (e.g., calculate total transaction amount).
- - Step 3: Write the final data into a Delta table.

```python
dbutils.fs.cp(“file:/Workspace/Shared/transaction_data.csv”,”dbfs:/FileStore/transaction_data.csv”)
```

**Creating a Delta Live Table (DLT) Pipeline:**
- Create a cluster
- Explore the source data
- Ingest the raw data
- Prepare the raw data
- Query the transformed data
- Create a job to run the pipeline 


2. **Write DLT in Python:**
- Implement the pipeline using DLT in Python. Define the following tables:
```python
import dlt
from pyspark.sql.functions import col
```

- - Raw Transactions Table: Read data from the CSV file.
```python
@dlt.table(
    comment="Raw transactions ingested from CSV"
)
def raw_transactions():
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("dbfs:/FileStore/transaction_data.csv")
```

- - Transformed Transactions Table: Apply transformations (e.g., calculate total amount: Quantity * Price ).
```python
def transformed_transactions():
    return (
        dlt.read("raw_transactions")
        .withColumn("TotalAmount", col("Quantity") * col("Price"))
        .filter(col("Quantity") > 0)
    )
```

3. **Write DLT in SQL:**
- Implement the same pipeline using DLT in SQL. Use SQL syntax to define tables, transformations, and outputs.
```sql
CREATE OR REPLACE TABLE raw_transactions
COMMENT 'Raw transactions ingested from CSV'
AS SELECT *
FROM csv.`dbfs:/FileStore/transactions.csv`;

CREATE OR REPLACE TABLE transformed_transactions
COMMENT 'Transformed transactions with total amount'
AS
SELECT *,
    Quantity * Price AS TotalAmount
FROM raw_transactions
WHERE Quantity > 0;
```

4. **Monitor the Pipeline:**
- Use Databricks' DLT UI to monitor the pipeline and check the status of each step.
```
i. Open the Databricks Delta Live Tables, go to Pipelines tab.
ii. Select the pipeline
iii. Monitor the status of the pipeline
```

---

## Exercise 2: Delta Lake Operations - Read, Write, Update, Delete, Merge
**Objective:**
Work with Delta Lake to perform read, write, update, delete, and merge operations using both PySpark and SQL.

### Tasks:
1. **Read Data from Delta Lake:**
- Read the transactional data from the Delta table you created in the first exercise using PySpark and SQL.
- Verify the contents of the table by displaying the first 5 rows.

**Python**
```python
df = spark.read.format("delta").load("/delta/transaction_data ")
df.show()
```

**SQL**
~~~sql
SELECT * FROM delta.`/delta/transaction_data `
LIMIT 5;
~~~

2. **Write Data to Delta Lake:**
- Append new transactions to the Delta table using PySpark.
- Example new transactions:
```csv
6,2024-09-06,C005,Keyboard,4,100
7,2024-09-07,C006,Mouse,10,20
```
```python
new_transactions = [
    (6, "2024-09-06", "C005", "Keyboard", 4, 100),
    (7, "2024-09-07", "C006", "Mouse", 10, 20)
]
new_df = spark.createDataFrame(new_transactions, ["TransactionID","TransactionDate", "CustomerID", "Product", "Quantity", "Price"])

new_df.write.format("delta").mode("append").save("/delta/transaction_data")
```

3. **Update Data in Delta Lake:**
- Update the Price of Product = 'Laptop' to 1300 .
- Use PySpark or SQL to perform the update and verify the results.
```python
from delta.tables import *
delta_table = DeltaTable.forPath(spark, "/delta/transaction_data")
delta_table.update(
condition="Product = 'Laptop'",
set={"Price": "1300"}
)
```

4. **Delete Data from Delta Lake:**
- Delete all transactions where the Quantity is less than 3.
- Use both PySpark and SQL to perform this deletion.
**Python**
```python
delta_table.delete(condition="Quantity < 3")
```

**SQL**
```sql
DELETE FROM delta.`/delta/transaction_data`
WHERE Quantity < 3;
```

5. **Merge Data into Delta Lake:**
- Create a new set of data representing updates to the existing transactions. Merge the following new data into the Delta table:
```csv
TransactionID,TransactionDate,CustomerID,Product,Quantity,Price
1,2024-09-01,C001,Laptop,1,1250 -- Updated Price
8,2024-09-08,C007,Charger,2,30 -- New Transaction
```

- Use the Delta Lake merge operation to insert the new data and update the
existing records.
```python
updates = [
    (1, "2024-09-01", "C001", "Laptop", 1, 1250),
    (8, "2024-09-08", "C007", "Charger", 2, 30)
]
updates_df = spark.createDataFrame(updates, ["TransactionID", "TransactionDate", "CustomerID", "Product", "Quantity", "Price"])

delta_table.alias("target").merge(updates_df.alias("source"),
    "target.TransactionID = source.TransactionID")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll().execute()
```

---

## Exercise 3: Delta Lake - History, Time Travel, and Vacuum
**Objective:**
Understand how to use Delta Lake features such as versioning, time travel, and data cleanup with vacuum.

### Tasks:
1. **View Delta Table History:**
- Query the history of the Delta table to see all changes (inserts, updates, deletes) made in the previous exercises.
- Use both PySpark and SQL to view the history.
**PySpark:**
```python
from delta.tables import *
delta_table = DeltaTable.forPath(spark, "/path/to/delta/table")
history_df = delta_table.history()
history_df.show(truncate=False)
```

**SQL:**
~~~sql
DESCRIBE HISTORY delta.`/delta/transaction_data`;
~~~

2. **Perform Time Travel:**
- Retrieve the state of the Delta table as it was 5 versions ago.
```python
df_time_travel = spark.read.format("delta").option("versionAsOf",5).load("/delta/transaction_data")
df_time_travel.show()
```

- Verify that the table reflects the data before some of the updates and deletions made earlier.
```python
df_time_travel = spark.read.format("delta").option("versionAsOf", 5).load("/delta/transaction_data")
df_time_travel.show()
```

- Perform a query to get the transactions from a specific timestamp (e.g., just before an update).
```python
df_time_travel = spark.read.format("delta").option("timestampAsOf", "2024-09-22T14:00:00Z").load("/delta/transaction_data")
df_time_travel.show()
```

3. **Vacuum the Delta Table:**
- Clean up old data using the VACUUM command.
- Set a retention period of 7 days and vacuum the Delta table.
- Verify that old versions are removed, but the current table state is intact.
```python
delta_table.vacuum(retentionHours=168)
```

4. **Converting Parquet Files to Delta Files:**
- Create a new Parquet-based table from the raw transactions CSV file.
```python
parquet_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/transaction_data.csv")

parquet_df.write.format("parquet").save("/FileStore/parquet/transaction_table")
```

- Convert this Parquet table to a Delta table using Delta Lake functionality.
```python
spark.read.format("parquet").load("/FileStore/parquet/transaction_table").write.format("delta").save("/delta/parquet_converted_table")
```

---

## Exercise 4: Implementing Incremental Load Pattern using Delta Lake
**Objective:**
Learn how to implement incremental data loading with Delta Lake to avoid reprocessing
old data.

### Tasks:
1. **Set Up Initial Data:**
- Use the same transactions data from previous exercises, but load only transactions from the first three days ( 2024-09-01 to 2024-09-03 ) into
the Delta table.
```python
initial_transactions = [
    (1, "2024-09-01", "C001", "Laptop", 1, 1200),
    (2, "2024-09-02", "C002", "Tablet", 2, 300),
    (3, "2024-09-03", "C001", "Headphones", 5, 50)
]
initial_df = spark.createDataFrame(initial_transactions, ["TransactionID", "TransactionDate", "CustomerID", "Product", "Quantity", "Price"])

initial_df.write.format("delta").mode("overwrite").save("/delta/transaction_data")
```

2. **Set Up Incremental Data:**
- Add a new set of transactions representing the next four days ( 2024-09-04 to 2024-09-07 ).
- Ensure that these transactions are loaded incrementally into the Delta table.
```python
incremental_transactions = [
    (4, "2024-09-04", "C003", "Smartphone", 1, 800),
    (5, "2024-09-05", "C004", "Smartwatch", 3, 200),
    (6, "2024-09-06", "C005", "Keyboard", 4, 100),
    (7, "2024-09-07", "C006", "Mouse", 10, 20)
]
incremental_df = spark.createDataFrame(incremental_transactions, ["TransactionID", "TransactionDate", "CustomerID", "Product", "Quantity", "Price"])
```

3. **Implement Incremental Load:**
- Create a pipeline that reads new transactions only (transactions after 2024-09-03 ) and appends them to the Delta table without overwriting existing data.

- Verify that the incremental load only processes new data and does not duplicate or overwrite existing records.
```python
delta_table = DeltaTable.forPath(spark, "/delta/transaction_data")

max_date = delta_table.toDF().agg({"TransactionDate": "max"}).collect()[0][0]

new_transactions = incremental_df.filter(incremental_df.TransactionDate > max_date)

new_transactions.write.format("delta").mode("append").save("/delta/transaction_data")
```

4. **Monitor Incremental Load:**
- Check the Delta Lake version history to ensure only the new transactions are added, and no old records are reprocessed.
```python
history_df = delta_table.history()
history_df.show(truncate=False)
```
---