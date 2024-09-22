# Assignment 2

## Mini Project: Data Governance Using Unity Catalog - Advanced Capabilities

### Objective
Participants will:
1. Create multiple schemas and tables using Unity Catalog.
2. Set up data governance features like Data Discovery, Data Audit, Data Lineage,
and Access Control.
3. Build a secure environment with fine-grained control over data access and
visibility.
---


### Task 1: Set Up Unity Catalog Objects with Multiple Schemas
**1. Create a Catalog:**
- Create a catalog named finance_data_catalog for storing financial data.
~~~sql
CREATE CATALOG finance_data_catalog;
~~~

**2. Create Multiple Schemas:**

Create two schemas inside the catalog:
- transaction_data
- customer_data
~~~sql
CREATE SCHEMA finance_data_catalog.transaction_data;
CREATE SCHEMA finance_data_catalog.customer_data;
~~~

**3. Create Tables in Each Schema:**
- For transaction_data , create a table with columns: TransactionID ,
CustomerID , TransactionAmount , TransactionDate .
~~~sql
CREATE TABLE finance_data_catalog.transaction_data.transactions (
TransactionID STRING,
CustomerID STRING,
TransactionAmount DECIMAL(10, 2),
TransactionDate DATE
);
~~~

- For customer_data , create a table with columns: CustomerID ,
CustomerName , Email , Country.
~~~sql
CREATE TABLE finance_data_catalog.customer_data.customers (
CustomerID STRING,
CustomerName STRING,
Email STRING,
Country STRING
);
~~~
---

### Task 2: Data Discovery Across Schemas
**1. Explore Metadata:**

- Search for tables across both schemas and retrieve metadata using SQL commands.
~~~sql
SHOW TABLES IN finance_data_catalog.transaction_data;
SHOW TABLES IN finance_data_catalog.customer_data;
~~~

**2. Data Profiling:**

Run SQL queries to perform data profiling on both tables, discovering trends in transaction amounts and customer locations.
-  Profiling TransactionAmount in transaction_data.transactions
~~~sql
SELECT AVG(TransactionAmount) AS AvgTransactionAmount, 
MAX(TransactionAmount) AS MaxTransactionAmount, 
MIN(TransactionAmount) AS MinTransactionAmount
FROM finance_data_catalog.transaction_data.transactions;
~~~

-  To find Transaction counts over time
~~~sql
SELECT TransactionDate, COUNT(*) AS TotalTransactions
FROM finance_data_catalog.transaction_data.transactions
GROUP BY TransactionDate
ORDER BY TransactionDate;
~~~

-  Profiling Country in customer_data.customers
~~~sql
SELECT Country, COUNT(*) AS TotalCustomers
FROM finance_data_catalog.customer_data.customers
GROUP BY Country
ORDER BY TotalCustomers DESC;
~~~


**3. Tagging Sensitive Data:**
- Apply tags to sensitive columns such as Email and TransactionAmount for better governance tracking.
~~~sql
ALTER TABLE finance_data_catalog.customer_data.customers
ADD TAG (sensitive='true') FOR COLUMN Email;
~~~

~~~sql
ALTER TABLE finance_data_catalog.transaction_data.transactions
ADD TAG (sensitive='true') FOR COLUMN TransactionAmount;
~~~
---

### Task 3: Implement Data Lineage and Auditing

**1. Track Data Lineage:**

- Merge data from both schemas ( transaction_data and customer_data ) to generate a comprehensive view.
~~~sql
SELECT t.TransactionID, t.CustomerID, c.CustomerName, c.Email, c.Country, 
t.TransactionAmount, t.TransactionDate
FROM finance_data_catalog.transaction_data.transactions t
JOIN finance_data_catalog.customer_data.customers c
ON t.CustomerID = c.CustomerID;
~~~
- Use Unity Catalog to trace the data lineage and track changes between these two tables.
```
To view data lineage, navigate to data explorer in databricks. In unity catalog we can view
lineage and track changes.
```

**2. Audit User Actions:**
- Enable audit logs for operations performed on the tables and track who
accessed or modified the data.
```
a. Navigate to admin console in databricks
b. Go to audit logs tab and enable audit logs
- track who accessed or modified the data.
```
```
Once audit logging is enabled, you can monitor user actions such as:
a. Who queried or accessed the tables.
b. Who performed modifications (e.g., inserts, updates, deletes) on the tables
```
---

### Task 4: Access Control and Permissions
**1. Set Up Roles and Groups:**

- Create two groups: DataEngineers and DataAnalysts.
~~~sql
CREATE GROUP DataEngineers;
CREATE GROUP DataAnalysts;
~~~

- Assign appropriate roles:
  - DataEngineers should have full access to both schemas and tables.
~~~sql
GRANT ALL PRIVILEGES ON SCHEMA finance_data_catalog.transaction_data TO `DataEngineers`;
    
GRANT ALL PRIVILEGES ON SCHEMA finance_data_catalog.customer_data TO `DataEngineers`;
    
GRANT ALL PRIVILEGES ON TABLE finance_data_catalog.transaction_data.transactions TO `DataEngineers`;
    
GRANT ALL PRIVILEGES ON TABLE finance_data_catalog.customer_data.customers TO `DataEngineers`;
~~~
  - DataAnalysts should have read-only access to the customer_data
schema and restricted access to the transaction_data schema.
~~~sql
GRANT SELECT ON SCHEMA finance_data_catalog.customer_data TO `DataAnalysts`;

GRANT SELECT ON TABLE finance_data_catalog.customer_data.customers TO `DataAnalysts`;

GRANT SELECT ON TABLE finance_data_catalog.transaction_data.transactions TO `DataAnalysts`;
~~~


**2. Row-Level Security:**

- Implement row-level security for the transaction_data schema, allowing only certain users to view high-value transactions.
 
  -  Create a Dynamic View for High-Value Transactions
~~~sql
CREATE OR REPLACE VIEW
finance_data_catalog.transaction_data.secure_transactions AS
SELECT * FROM finance_data_catalog.transaction_data.transactions
WHERE (TransactionAmount <= 10000)
OR
(TransactionAmount > 10000 AND CURRENT_USER() IN ('authorized_user1',
'authorized_user2'));
~~~


- -  Restrict Access to the Original Table
~~~sql
REVOKE SELECT ON TABLE finance_data_catalog.transaction_data.transactions
FROM `DataAnalysts`;
GRANT SELECT ON VIEW finance_data_catalog.transaction_data.secure_transactions
TO `DataAnalysts`;
~~~
---
### Task 5: Data Governance Best Practices
**1. Create Data Quality Rules:**
- Implement basic data quality rules to ensure that:
- - Transaction amounts are non-negative.
~~~sql
ALTER TABLE finance_data_catalog.transaction_data.transactions
ADD CONSTRAINT check_non_negative_amount CHECK (TransactionAmount >= 0);
~~~

- - Customer emails follow the correct format.
~~~sql
ALTER TABLE finance_data_catalog.customer_data.customers
ADD CONSTRAINT check_email_format
CHECK (Email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$');
~~~


**2. Validate Data Governance:**
- Validate all data governance rules by running SQL queries and checking that the lineage and audit logs capture all operations correctly.
- - Validate all data governance rules by running SQL queries
~~~sql
ALTER TABLE finance_data_catalog.customer_data.customers
ADD CONSTRAINT check_email_format
CHECK (Email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$');
~~~
- - Checking that the lineage and audit logs capture all operations correctly.

**Verify Audit Logs**
~~~sql
SELECT eventName,userIdentity, objectName, actionName, timestamp
FROM <audit_log_table>
WHERE objectName IN
('finance_data_catalog.transaction_data.transactions',
'finance_data_catalog.customer_data.customers')
AND actionName IN ('INSERT', 'UPDATE');
~~~
---

### Task 6: Data Lifecycle Management
**1. Implement Time Travel:**

- Use Unity Catalog’s Delta Time Travel feature to access historical versions of the transaction_data table and restore to a previous state.
- - Access historical versions of the table
~~~sql
SELECT * FROM finance_data_catalog.transaction_data.transactions
VERSION AS OF 1;
~~~
- - Restore the table to a Previous State
~~~sql
RESTORE TABLE finance_data_catalog.transaction_data.transactions
TO VERSION AS OF 5;
~~~

**2. Run a Vacuum Operation:**
- Run a vacuum operation on the tables to clean up old files and ensure the Delta tables are optimized.
~~~sql
VACUUM finance_data_catalog.transaction_data.transactions RETAIN 168 HOURS;
VACUUM finance_data_catalog.customer_data.customers RETAIN 168 HOURS;
~~~

---
---


## Mini Project: Advanced Data Governance and Security Using Unity Catalog
### Objective:
Participants will:
1. Create a multi-tenant data architecture using Unity Catalog.
2. Explore the advanced features of Unity Catalog, including data discovery, data
lineage, audit logs, and access control.

---

### Task 1: Set Up Multi-Tenant Data Architecture Using Unity Catalog
**1. Create a New Catalog:**

- Create a catalog named corporate_data_catalog for storing corporate-wide
data.
~~~sql
CREATE CATALOG corporate_data_catalog;
~~~

**2. Create Schemas for Each Department:**
- Create three schemas:
- - sales_data
~~~sql
CREATE SCHEMA corporate_data_catalog.sales_data;
~~~

- - hr_data
~~~sql
CREATE SCHEMA corporate_data_catalog.hr_data;
~~~

- - finance_data
~~~sql
CREATE SCHEMA corporate_data_catalog.finance_data;
~~~

**3. Create Tables in Each Schema:**
- For sales_data : Create a table with columns SalesID , CustomerID ,
SalesAmount , SalesDate .
~~~sql
CREATE TABLE corporate_data_catalog.sales_data.sales_table(
SalesID STRING,
CustomerID STRING,
SalesAmount DECIMAL(10,2),
SalesDate DATE
);
~~~

- For hr_data : Create a table with columns EmployeeID , EmployeeName ,
Department , Salary .
~~~sql
CREATE TABLE corporate_data_catalog.hr_data.hr_table(
EmployeeID STRING,
EmployeeName STRING,
Department STRING,
Salary DECIMAL(10,2)
);
~~~

- For finance_data : Create a table with columns InvoiceID , VendorID ,
InvoiceAmount , PaymentDate .
~~~sql
CREATE TABLE corporate_data_catalog.finance_data.finance_table(
InvoiceID STRING,
VendorID STRING,
InvoiceAmount DECIMAL(10,2),
PaymentDate DATE
);
~~~

---

### Task 2: Enable Data Discovery for Cross-Departmental Data
**1. Search for Tables Across Departments:**
- Use the Unity Catalog interface to search for tables across the
sales_data , hr_data , and finance_data schemas.
~~~sql
SHOW TABLES IN corporate_data_catalog.sales_data;
SHOW TABLES IN corporate_data_catalog.hr_data;
SHOW TABLES IN corporate_data_catalog.finance_data;
~~~

**2. Tag Sensitive Information:**
- Tag columns that contain sensitive data, such as Salary in the hr_data
schema and InvoiceAmount in the finance_data schema.
~~~sql
ALTER TABLE corporate_data_catalog.hr_data.hr_table
SET TAG 'sensitive' ON COLUMN Salary;

ALTER TABLE corporate_data_catalog.finance_data.finance_table
SET TAG 'sensitive' ON COLUMN InvoiceAmount;
~~~

**3. Data Profiling:**
- Perform basic data profiling on the tables to analyze trends in sales,
employee salaries, and financial transactions.
~~~sql
SELECT AVG(SalesAmount), MIN(SalesAmount), MAX(SalesAmount) 
FROM corporate_data_catalog.sales_data.sales_table;

SELECT AVG(Salary), MIN(Salary), MAX(Salary) 
FROM corporate_data_catalog.hr_data.hr_table;

SELECT AVG(InvoiceAmount), MIN(InvoiceAmount), MAX(InvoiceAmount) 
FROM corporate_data_catalog.finance_data.finance_table;
~~~
---

### Task 3: Implement Data Lineage and Data Auditing
**1. Track Data Lineage:**
- Track data lineage between the sales_data and finance_data schemas by creating a reporting table that merges the sales and finance data.
- Use Unity Catalog’s data lineage feature to visualize how data flows between these tables.
~~~sql
-- creating a reporting table that merges the sales and finance data.
CREATE TABLE corporate_data_catalog.reporting.sales_finance_report AS
SELECT s.SalesID, s.CustomerID, s.SalesAmount, s.SalesDate, f.InvoiceID,
f.InvoiceAmount
FROM corporate_data_catalog.sales_data.sales_table s
JOIN corporate_data_catalog.finance_data.finance_table f
ON s.CustomerID = f.VendorID;
~~~

**2. Enable Data Audit Logs:**
- Ensure that all operations (e.g., data reads, writes, and updates) on the hr_data and finance_data tables are captured in audit logs for regulatory compliance.
```
- Enabling audit logs for operations performed on the tables
a. Navigate to admin console in databricks
b. Go to audit logs tab and enable audit logs
```
---

### Task 4: Data Access Control and Security

**1. Set Up Roles and Permissions:**
- Create the following groups:
- - SalesTeam : Should have access to the sales_data schema only.
~~~sql
CREATE GROUP SalesTeam;
GRANT USAGE ON SCHEMA corporate_data_catalog.sales_data TO SalesTeam;
~~~

- - FinanceTeam : Should have access to both sales_data and finance_data schemas.
~~~sql
CREATE GROUP FinanceTeam;
GRANT USAGE ON SCHEMA corporate_data_catalog.sales_data TO FinanceTeam;
GRANT USAGE ON SCHEMA corporate_data_catalog.finance_data TO FinanceTeam;
~~~

- - HRTeam : Should have access to the hr_data schema with the ability to update employee records.
~~~sql
CREATE GROUP HRTeam;
GRANT USAGE ON SCHEMA corporate_data_catalog.hr_data TO HRTeam;
GRANT UPDATE ON TABLE corporate_data_catalog.hr_data.hr_table TO HRTeam;
~~~

**2. Implement Column-Level Security:**
- Restrict access to the Salary column in the hr_data schema, allowing
only HR managers to view this data.
~~~sql
GRANT SELECT ON COLUMN Salary TO HRManager;
~~~

**3. Row-Level Security:**
- Implement row-level security on the sales_data schema to ensure that each sales representative can only access their own sales records.
~~~sql
CREATE ROW ACCESS POLICY sales_rep_policy ON
corporate_data_catalog.sales_data.sales_table
FOR EACH ROW
WHEN current_user = sales_rep_id;
~~~
---

### Task 5: Data Governance Best Practices
**1. Define Data Quality Rules:**
- Set up data quality rules to ensure:
- - Sales amounts are positive in the sales_data table.
~~~sql
SELECT * FROM corporate_data_catalog.sales_data.sales_table
WHERE SalesAmount <= 0;
~~~

- - Employee salaries are greater than zero in the hr_data table.
~~~sql
SELECT * FROM corporate_data_catalog.hr_data.hr_table
WHERE Salary <= 0;
~~~

- - Invoice amounts in the finance_data table match payment records.
~~~sql
SELECT * FROM corporate_data_catalog.finance_data.finance_table
WHERE InvoiceAmount <> PaymentAmount;
~~~

**2. Apply Time Travel for Data Auditing:**
- Use Delta Time Travel to restore the finance_data table to a previous
state after an erroneous update and validate the changes using data
audit logs.
~~~sql
RESTORE TABLE corporate_data_catalog.finance_data.finance_table
TO VERSION AS OF 5;
~~~
---

### Task 6: Optimize and Clean Up Delta Tables
**1. Optimize Delta Tables:**
- Use the OPTIMIZE command to improve query performance on the
sales_data and finance_data tables.
~~~sql
OPTIMIZE corporate_data_catalog.sales_data.sales_table;
OPTIMIZE corporate_data_catalog.finance_data.finance_table;
~~~

**2. Vacuum Delta Tables:**
- Run a VACUUM operation to remove old and unnecessary data files from
the Delta tables, ensuring efficient storage.
~~~sql
VACUUM corporate_data_catalog.sales_data.sales_table RETAIN 168 HOURS;
VACUUM corporate_data_catalog.finance_data.finance_table RETAIN 168 HOURS;
~~~

---
---

## Mini Project: Building a Secure Data Platform with Unity Catalog
### Objective:
Participants will:
1. Set up a secure data platform using Unity Catalog.
2. Explore key data governance features such as Data Discovery, Data Lineage,
Access Control, and Audit Logging.

---

### Task 1: Set Up Unity Catalog for Multi-Domain Data Management
**1. Create a New Catalog:**
- Create a catalog named enterprise_data_catalog to manage data across
various domains.
~~~sql
CREATE CATALOG enterprise_data_catalog;
~~~

**2. Create Domain-Specific Schemas:**
- Create the following schemas:
- - marketing_data
~~~sql
CREATE SCHEMA enterprise_data_catalog.marketing_data;
~~~

- - operations_data
~~~sql
CREATE SCHEMA enterprise_data_catalog.operations_data;
~~~

- - it_data
~~~sql
CREATE SCHEMA enterprise_data_catalog.it_data;
~~~

**3. Create Tables in Each Schema:**
- In the marketing_data schema, create a table with columns: CampaignID ,
CampaignName , Budget , StartDate .
~~~sql
CREATE TABLE enterprise_data_catalog.marketing_data.marketing_table(
CampaignID INT,
CampaignName STRING,
Budget DECIMAL(10,2),
StartDate DATE
);
~~~

- In the operations_data schema, create a table with columns: OrderID ,
ProductID , Quantity , ShippingStatus .
~~~sql
CREATE TABLE enterprise_data_catalog.operations_data.operations_table(
OrderID INT,
ProductID INT,
Quantity INT,
ShippingStatus STRING
);
~~~

- In the it_data schema, create a table with columns: IncidentID ,
ReportedBy , IssueType , ResolutionTime .
~~~sql
CREATE TABLE enterprise_data_catalog.it_data.it_table(
IncidentID STRING,
ReportedBy STRING,
IssueType STRING,
ResolutionTime INT
);
~~~
---

### Task 2: Data Discovery and Classification
**1. Search for Data Across Schemas:**
- Use Unity Catalog’s data discovery features to list all tables in the
catalog.
- Perform a search query to retrieve tables based on data types (e.g.,
Budget , ResolutionTime ).
~~~sql
SHOW TABLES IN enterprise_data_catalog;
~~~

**2. Tag Sensitive Information:**
- Tag the Budget column in marketing_data and ResolutionTime in
it_data as sensitive for better data management and compliance.
~~~sql
ALTER TABLE enterprise_data_catalog.marketing_data.marketing_table
SET TAG 'sensitive' ON COLUMN Budget;

ALTER TABLE enterprise_data_catalog.it_data.it_table
SET TAG 'sensitive' ON COLUMN ResolutionTime;
~~~

**3. Data Profiling:**
- Perform basic data profiling to understand trends in marketing budgets
and operational shipping statuses.
~~~sql
SELECT AVG(Budget), MIN(Budget), MAX(Budget)
FROM enterprise_data_catalog.marketing_data.marketing_table;
SELECT COUNT(ShippingStatus), ShippingStatus 
FROM enterprise_data_catalog.operations_data.operations_table
GROUP BY ShippingStatus;
~~~
---

### Task 3: Data Lineage and Auditing
**1. Track Data Lineage Across Schemas:**
- Link the marketing_data with the operations_data by joining campaign
performance with product orders.

- Use Unity Catalog to track the lineage of the data from marketing
campaigns to sales.
~~~sql
CREATE TABLE enterprise_data_catalog.reporting.campaign_orders_report AS
SELECT m.CampaignID, m.CampaignName, m.Budget, o.OrderID, o.ProductID, 
o.Quantity
FROM enterprise_data_catalog.marketing_data.campaigns m
JOIN enterprise_data_catalog.operations_data.orders o
ON m.CampaignID = o.ProductID;
~~~

**2. Enable and Analyze Audit Logs:**
- Ensure audit logging is enabled to track all operations on tables within
the it_data schema. Identify who accessed or modified the data.
```
- Enabling audit logs for operations performed on the tables
a. Navigate to admin console in databricks
b. Go to audit logs tab and enable audit logs
```
---

### Task 4: Implement Fine-Grained Access Control

**1. Create User Roles and Groups:**
- Set up the following groups:
- - MarketingTeam : Access to the marketing_data schema only.
~~~sql
CREATE GROUP MarketingTeam;
GRANT USAGE ON SCHEMA enterprise_data_catalog.marketing_data TO
MarketingTeam;
GRANT USAGE ON SCHEMA enterprise_data_catalog.marketing_data TO
OperationsTeam;
~~~

- - OperationsTeam : Access to both operations_data and marketing_data schemas.
~~~sql
CREATE GROUP OperationsTeam;
GRANT USAGE ON SCHEMA enterprise_data_catalog.operations_data TO
OperationsTeam;
~~~

- - ITSupportTeam : Access to the it_data schema with permission to update issue resolution times.
~~~sql
CREATE GROUP ITSupportTeam;
GRANT USAGE ON SCHEMA enterprise_data_catalog.it_data TO ITSupportTeam;
GRANT UPDATE ON TABLE enterprise_data_catalog.it_data.it_data TO ITSupportTeam;
~~~

**2. Implement Column-Level Security:**
- Restrict access to the Budget column in the marketing_data schema,
allowing only the MarketingTeam to view it.
~~~sql
GRANT SELECT ON COLUMN Budget TO MarketingTeam;
~~~

**3. Row-Level Security:**
- Implement row-level security in the operations_data schema to ensure
that users from the OperationsTeam can only view orders relevant to
their department.
~~~sql
CREATE ROW ACCESS POLICY operations_team_policy ON
enterprise_data_catalog.operations_data.orders
FOR EACH ROW
WHEN current_user = operations_rep;
~~~
---

### Task 5: Data Governance and Quality Enforcement
**1. Set Data Quality Rules:**
- Define rules for each schema:
- - marketing_data : Ensure that the campaign budget is greater than
zero.
~~~sql
SELECT *
FROM enterprise_data_catalog.marketing_data.marketing_table
WHERE Budget <= 0;
~~~

- - operations_data : Ensure that shipping status is valid (e.g.,
'Pending', 'Shipped', 'Delivered').
~~~sql
SELECT *
FROM enterprise_data_catalog.operations_data.operations_table
WHERE ShippingStatus NOT IN ('Pending', 'Shipped', 'Delivered');
~~~

- - it_data : Ensure that issue resolution times are recorded
correctly and not negative.
~~~sql
SELECT * 
FROM enterprise_data_catalog.it_data.it_table 
WHERE ResolutionTime < 0;
~~~

**2. Apply Delta Lake Time Travel:**
- Use Delta Lake Time Travel to explore different historical states of the
operations_data schema, and revert to an earlier version if required.
~~~sql
RESTORE TABLE enterprise_data_catalog.operations_data.operations_table
TO VERSION AS OF 1;
~~~
---

### Task 6: Performance Optimization and Data Cleanup
**1. Optimize Delta Tables:**
- Apply OPTIMIZE to the operations_data and it_data schemas to enhance
performance for frequent queries.
~~~sql
OPTIMIZE enterprise_data_catalog.operations_data.operations_table;
OPTIMIZE enterprise_data_catalog.it_data.it_table;
~~~

**2. Vacuum Delta Tables:**
- Run a VACUUM operation on the Delta tables to clean up old and
unnecessary data files.
~~~sql
VACUUM enterprise_data_catalog.operations_data.operations_table 
RETAIN 168 HOURS;
VACUUM enterprise_data_catalog.it_data.it_table RETAIN 168 HOURS;
~~~

---
---

## Data Ingestion (1)

### Task 1: Raw Data Ingestion
- Create a notebook to ingest raw weather data.
- The notebook should read a CSV file containing weather data.
- Define a schema for the data and ensure that proper data types are used (e.g., City, Date, Temperature, Humidity).
- If the raw data file does not exist, handle the error and log it.
- Save the raw data to a Delta table.

**1. Read the CSV file**
```python
dbutils.fs.cp(“file:/Workspace/Shared/weather_data.csv”,”dbfs:/FileStore/weather_data.csv”)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.appName("WeatherDataIngestion").getOrCreate()

weather_schema = StructType([
StructField("City", StringType(), True),
StructField("Date", DateType(), True),
StructField("Temperature", FloatType(), True),
StructField("Humidity", FloatType(), True)
])

try:
    weather_df = spark.read.csv("dbfs:/FileStore/weather_data.csv", schema=weather_schema,
    header=True)
except AnalysisException:
    print("Error: File not found.")
```

**2. Save the data to a delta table**
```python
weather_df.write.format("delta").mode("overwrite").save("/delta/raw_weather_data")
```

---

### Task 2: Data Cleaning
- Create a notebook to clean the raw weather data.

- Load the data from the Delta table created in Task 1.
```python
weather_raw_df = spark.read.format("delta").load("/delta/raw_weather_data")
```

- Remove any rows that contain missing or null values.
```python
cleaned_weather_df = weather_raw_df.dropna()
```

- Save the cleaned data to a new Delta table.
```python
cleaned_weather_df.write.format("delta").mode("overwrite").save("/delta/weather_cleaned")
```

---

### Task 3: Data Transformation
- Create a notebook to perform data transformation.
- Load the cleaned data from the Delta table created in Task 2.
```python
weather_cleaned_df = spark.read.format("delta").load("/delta/weather_cleaned")
```

- Calculate the average temperature and humidity for each city.
```python
from pyspark.sql.functions import avg
transformed_df = weather_cleaned_df.groupBy("City").agg(
avg("Temperature").alias("Avg_Temperature"),
avg("Humidity").alias("Avg_Humidity")
)
```

- Save the transformed data to a Delta table.
```python
transformed_df.write.format("delta").mode("overwrite").save("/delta/weather_transformed")
```

---

### Task 4: Create a Pipeline to Execute Notebooks
- Create a pipeline that sequentially executes the following notebooks:
- - Raw Data Ingestion
- - Data Cleaning
- - Data Transformation
- Handle errors such as missing files or failed steps in the pipeline.
```python
import subprocess
notebooks = [
"/ delta/raw_weather_data /data_ingestion.py",
"/delta/weather_cleaned/data_cleaning.py",
"/delta/weather_transformed/data_transformation.py"
]
for notebook in notebooks:
    try:
        subprocess.run(["databricks", "workspace", "import", notebook], check=True)
        print(f"Successfully executed {notebook}")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while executing {notebook}: {e}")
```
- Ensure that log messages are generated at each step to track the progress of the pipeline.
```python
import logging
logging.basicConfig(filename='/path/to/pipeline_log.log', level=logging.INFO)
try:
    logging.info(f'Successfully executed {notebook}')
except Exception as e:
    logging.error(f'Failed to execute {notebook}: {e}')
```
---

**Bonus Task: Error Handling**
- Add error handling to the pipeline to manage scenarios like missing files or corrupted data.
```python
import os
if not os.path.exists("dbfs:/FileStore/weather_data.csv"):
    raise FileNotFoundError("Weather data file not found")
```

- Ensure the pipeline logs errors to a file or table for further analysis.
```python
try:
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        error_df = spark.createDataFrame([(str(e),)], ["Error"])
        error_df.write.format("delta").mode("append").save("/delta/error_log")
```
---
---

## Data Ingestion(2)

### Task 1: Raw Data Ingestion
- Use the following CSV data to represent daily weather conditions:
```csv
City,Date,Temperature,Humidity
New York,2024-01-01,30.5,60
Los Angeles,2024-01-01,25.0,65
Chicago,2024-01-01,-5.0,75
Houston,2024-01-01,20.0,80
Phoenix,2024-01-01,15.0,50
```
```python
dbutils.fs.cp(“file:/Workspace/Shared/weather_data.csv”,”dbfs:/FileStore/weather_data.csv”)
```

- Load the CSV data into a Delta table in Databricks.
- If the file does not exist, handle the missing file scenario and log the error.
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType

import os
import logging

spark = SparkSession.builder.appName("WeatherDataIngestion").getOrCreate()

weather_schema = StructType([
StructField("City", StringType(), True),
StructField("Date", DateType(), True),
StructField("Temperature", FloatType(), True),
StructField("Humidity", FloatType(), True)
])

file_path = "dbfs:/FileStore/weather_data.csv"
logging.basicConfig(filename='/dbfs/mnt/logs/ingestion_log.log', level=logging.INFO)

if not os.path.exists(file_path):
    logging.error(f"File not found: {file_path}")

else:
    weather_df = spark.read.csv(file_path, schema=weather_schema, header=True)
    weather_df.write.format("delta").mode("overwrite").save("/delta/weather_raw")
    logging.info(f"Weather data successfully ingested and saved to Delta at /delta/weather_raw")
```
---

### Task 2: Data Cleaning
- Create a notebook to clean the ingested weather data.
- Handle null or incorrect values in the temperature and humidity columns.
```python
from pyspark.sql.functions import col
weather_raw_df = spark.read.format("delta").load("/delta/weather_raw")
cleaned_weather_df = weather_raw_df.na.drop()
cleaned_weather_df = cleaned_weather_df.filter((col("Temperature") >= -50) & (col("Humidity") >= 0))
```

- After cleaning, save the updated data to a new Delta table.
```python
cleaned_weather_df.write.format("delta").mode("overwrite").save("/delta/weather_cleaned"
)
```
---

### Task 3: Data Transformation
- Transform the cleaned data by calculating the average temperature and humidity for each city.
```python
from pyspark.sql.functions import avg

cleaned_weather_df = spark.read.format("delta").load("/delta/weather_cleaned")
transformed_df = cleaned_weather_df.groupBy("City").agg(
avg("Temperature").alias("Avg_Temperature"),
avg("Humidity").alias("Avg_Humidity")
)
```

- Save the transformed data into a new Delta table.
```python
transformed_df.write.format("delta").mode("overwrite").save("/delta/weather_transformed")
```
---

### Task 4: Build and Run a Pipeline
- Create a Databricks pipeline that executes the following notebooks in sequence:
- - Data ingestion (from Task 1)
- - Data cleaning (from Task 2)
- - Data transformation (from Task 3)
- - Ensure each step logs its status and any errors encountered.
```python
import subprocess
import logging

logging.basicConfig(filename='/dbfs/mnt/logs/pipeline_log.log', level=logging.INFO)

notebooks = [
"/delta/weather_raw",
"/delta/weather_cleaned",
"/delta/weather_transformed"
]

for notebook in notebooks:
    try:
        subprocess.run(["databricks", "workspace", "import", notebook], check=True)
        logging.info(f"Successfully executed {notebook}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error occurred while executing {notebook}: {e}")
```
---
---

## Data Ingestion(3)

### Task 1: Customer Data Ingestion
- Use the following CSV data representing customer transactions:
```csv
CustomerID,TransactionDate,TransactionAmount,ProductCategory
C001,2024-01-15,250.75,Electronics
C002,2024-01-16,125.50,Groceries
C003,2024-01-17,90.00,Clothing
C004,2024-01-18,300.00,Electronics
C005,2024-01-19,50.00,Groceries
```
```python
dbutils.fs.cp(“file:/Workspace/Shared/customer_transactions.csv”,”dbfs:/FileStore/customer_tra
nsactions.csv”)
```

- Load the CSV data into a Delta table in Databricks.
- If the file is not present, add error handling and log an appropriate message.
```python
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

import logging

spark = SparkSession.builder.appName("Customer Data Ingestion").getOrCreate()

file_path = " dbfs:/FileStore/customer_transactions.csv "

logging.basicConfig(level=logging.INFO)
try:
    customer_df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(file_path)
    logging.info(f"File loaded successfully from {file_path}")
    customer_df.write.format("delta").mode("overwrite").save("/mnt/delta/customer_transactions_delta")
    logging.info("Data ingested and saved to Delta table.")
except Exception as e:
    logging.error(f"Error loading file: {e}")
```
---

### Task 2: Data Cleaning
- Create a notebook to clean the ingested customer data.
- Remove any duplicate transactions and handle null values in the TransactionAmount column.
```python
delta_table = DeltaTable.forPath(spark, "/mnt/delta/customer_transactions_delta")
cleaned_df = delta_table.toDF().dropDuplicates().na.drop(subset=["TransactionAmount"])
```

- Save the cleaned data into a new Delta table.
```python
cleaned_df.write.format("delta").mode("overwrite").save("/mnt/delta/cleaned_customer_transacti
ons_delta")
```
---

### Task 3: Data Aggregation
- Aggregate the cleaned data by ProductCategory to calculate the total transaction amount per category.
```python
cleaned_data_df = spark.read.format("delta").load("/mnt/delta/cleaned_customer_transactions_delta")
aggregated_df = cleaned_data_df.groupBy("ProductCategory").sum("TransactionAmount").withColumnRenamed("sum(TransactionAmount)", "TotalTransactionAmount")

```

- Save the aggregated data to a Delta table.
```python
aggregated_df.write.format("delta").mode("overwrite").save("/mnt/delta/aggregated_transactions_delta")
```
---

### Task 4: Pipeline Creation
- Build a pipeline that:
1. Ingests the raw customer data (from Task 1).
2. Cleans the data (from Task 2).
3. Performs aggregation (from Task 3).
- Ensure the pipeline handles missing files or errors during each stage and logs them properly.
```python
import logging
logging.basicConfig(level=logging.INFO)

def ingest_data():
    try:
        customer_df = spark.read.format("csv").option("header", "true")/
        .option("inferSchema", "true").load(file_path)
        
        logging.info(f"File loaded successfully from {file_path}")

        customer_df.write.format("delta").mode("overwrite")/
        .save("/mnt/delta/customer_transactions_delta")
        
    except Exception as e:
        
        logging.error(f"Error in data ingestion: {e}")
        
    def clean_data():
        try:
            delta_table = DeltaTable.forPath(spark, "/mnt/delta/customer_transactions_delta")
            cleaned_df = 
            delta_table.toDF().dropDuplicates().na.drop(subset=["TransactionAmount"])
            cleaned_df.write.format("delta").mode("overwrite")/
            .save("/mnt/delta/cleaned_customer_transactions_delta")
        except Exception as e:
            logging.error(f"Error in data cleaning: {e}")
        
    def aggregate_data():
        try:
            cleaned_data_df = spark.read.format("delta")/
            .load("/mnt/delta/cleaned_customer_transactions_delta")
            aggregated_df = cleaned_data_df.groupBy("ProductCategory")/
            .sum("TransactionAmount")/
            .withColumnRenamed("sum(TransactionAmount)", "TotalTransactionAmount")
            aggregated_df.write.format("delta").mode("overwrite")/
            .save("/mnt/delta/aggregated_transactions_delta")
    except Exception as e:
        logging.error(f"Error in data aggregation: {e}")

ingest_data()
clean_data()
aggregate_data()

```
---

### Task 5: Data Validation
- After completing the pipeline, add a data validation step to verify that the total number of transactions matches the sum of individual category transactions.
```python

def validate_data():
    try:
        cleaned_data_df = spark.read.format("delta").load("/mnt/delta/cleaned_customer_transactions_delt
        a")
        aggregated_data_df = spark.read.format("delta").load("/mnt/delta/aggregated_transactions_delta")
        
        total_transaction_amount = cleaned_data_df.groupBy().sum("TransactionAmount").collect()[0][0]
        
        total_aggregated_amount = aggregated_data_df.groupBy().sum("TotalTransactionAmount").collect()[0][0]
        
        if total_transaction_amount == total_aggregated_amount:
            logging.info("Data validation successful: totals match.")
        else:
            logging.warning("Data validation failed: totals do not match.")
    
    except Exception as e:
        logging.error(f"Error in data validation: {e}")

validate_data()
```
---
---


## Data Ingestion (4)
### Task 1: Product Inventory Data Ingestion
- Use the following CSV data to represent product inventory information:
```csv
ProductID,ProductName,StockQuantity,Price,LastRestocked
P001,Laptop,50,1500.00,2024-02-01
P002,Smartphone,200,800.00,2024-02-02
P003,Headphones,300,100.00,2024-01-29
P004,Tablet,150,600.00,2024-01-30
P005,Smartwatch,100,250.00,2024-02-03
```
```python
dbutils.fs.cp(‘file:/Workspace/Shared/product_inventory.csv’,’dbfs:/FileStore/product_inventor
y.csv’)
```

- Load this CSV data into a Delta table in Databricks.
- Handle scenarios where the file is missing or corrupted and log the error accordingly.
```python
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

import logging

spark = SparkSession.builder \
.appName("Product Inventory Ingestion") \
.getOrCreate()

file_path = ’dbfs:/FileStore/product_inventory.csv’

logging.basicConfig(level=logging.INFO)

try:
    product_df = spark.read.csv(file_path, header=True, inferSchema=True)
    product_df.write.format("delta").mode("overwrite").save("/delta/product_inventory"
    )
    logging.info("Data ingested successfully.")

except FileNotFoundError:
    logging.error("File not found: %s", file_path)

except AnalysisException:
    logging.error("Error reading the CSV file, it may be corrupted.")
```

---

### Task 2: Data Cleaning
- Clean the ingested product data:
- - Ensure no null values in StockQuantity and Price columns.
- - Remove any records with StockQuantity less than 0.
- - Save the cleaned data to a new Delta table.
```python
cleaned_product_df = spark.read.format("delta").load("/delta/product_inventory")

cleaned_product_df = cleaned_product_df.filter(
(cleaned_product_df.StockQuantity.isNotNull()) &
(cleaned_product_df.Price.isNotNull()) &
(cleaned_product_df.StockQuantity >= 0)
)

cleaned_product_df.write.format("delta").mode("overwrite")/
.save("/delta/cleaned_product_inventory")

logging.info("Data cleaned and saved to new Delta table.")
```

---

### Task 3: Inventory Analysis
- Create a notebook to analyze the inventory data:
- - Calculate the total stock value for each product ( StockQuantity * Price ).
- - Find products that need restocking (e.g., products with StockQuantity <
100 ).
- Save the analysis results to a Delta table.
```python
inventory_df = spark.read.format("delta").load("/delta/cleaned_product_inventory")

inventory_df = inventory_df.withColumn("TotalStockValue", inventory_df.StockQuantity * inventory_df.Price)

restock_products_df = inventory_df.filter(inventory_df.StockQuantity < 100)

inventory_df.write.format("delta").mode("overwrite").save("/delta/inventory_analysis")

restock_products_df.write.format("delta").mode("overwrite").save("/delta/restock_products")

logging.info("Inventory analysis completed and results saved.")
```
---

### Task 4: Build an Inventory Pipeline
- Build a Databricks pipeline that:
1. Ingests the product inventory data (from Task 1).
2. Cleans the data (from Task 2).
3. Performs inventory analysis (from Task 3).
- Ensure the pipeline logs errors if any step fails and handles unexpected issues
such as missing data.
```python
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

import logging

spark = SparkSession.builder \
.appName("Product Inventory Pipeline") \
.getOrCreate()

logging.basicConfig(level=logging.INFO)

file_path = "dbfs:/FileStore/product_inventory.csv"

def run_pipeline():
    try:
        
        logging.info("Starting Task 1: Ingesting product inventory data...")
        
        product_df = spark.read.csv(file_path, header=True, inferSchema=True)
        
        product_df.write.format("delta").mode("overwrite").save("/delta/product_inventory")
        
        logging.info("Task 1 completed: Data ingested successfully.")
        
        logging.info("Starting Task 2: Cleaning data...")
        
        cleaned_product_df = spark.read.format("delta").load("/delta/product_inventory")
        
        cleaned_product_df = cleaned_product_df.filter(
        (cleaned_product_df.StockQuantity.isNotNull()) &
        (cleaned_product_df.Price.isNotNull()) &
        (cleaned_product_df.StockQuantity >= 0)
        )
        
        cleaned_product_df.write.format("delta").mode("overwrite")/
        .save("/delta/cleaned_product_inventory")
        
        logging.info("Task 2 completed: Data cleaned and saved to new Delta table.")
        
        logging.info("Starting Task 3: Performing inventory analysis...")
        
        inventory_df = spark.read.format("delta").load("/delta/cleaned_product_inventory")
        
        inventory_df = inventory_df.withColumn("TotalStockValue", inventory_df.StockQuantity * inventory_df.Price)
        
        restock_products_df = inventory_df.filter(inventory_df.StockQuantity < 100)
        
        inventory_df.write.format("delta").mode("overwrite").save("/delta/inventory_analy
        sis")
        
        restock_products_df.write.format("delta").mode("overwrite")/
        .save("/delta/restock_products")
        
        logging.info("Task 3 completed: Inventory analysis completed and results saved.")
        
        logging.info("Pipeline executed successfully.")
    
    except FileNotFoundError:
        logging.error("File not found: %s", file_path)
    
    except AnalysisException as e:
        logging.error("Error reading or writing Delta table: %s", str(e))
    
    except Exception as e:
        logging.error("Pipeline execution failed: %s", str(e))

run_pipeline()
```

---

### Task 5: Inventory Monitoring
- Create a monitoring notebook that checks the Delta table for any products that need restocking (e.g., StockQuantity < 50 ).
- The notebook should send an alert if any product is below the threshold.
```python
restock_df = spark.read.format("delta").load("/delta/restock_products")

if restock_df.count() > 0:
    logging.warning("The following products need restocking:")
    restock_df.show()

else:
    logging.info("All products are sufficiently stocked.")
```
---
---

## Data Ingestion (5)
### Task 1: Employee Attendance Data Ingestion
- Use the following CSV data representing employee attendance logs:
```csv
EmployeeID,Date,CheckInTime,CheckOutTime,HoursWorked
E001,2024-03-01,09:00,17:00,8
E002,2024-03-01,09:15,18:00,8.75
E003,2024-03-01,08:45,17:15,8.5
E004,2024-03-01,10:00,16:30,6.5
E005,2024-03-01,09:30,18:15,8.75
```
```python
dbutils.fs.cp(‘file:/Workspace/Shared/employee_attendance.csv’,’dbfs:/FileStore/employee_attendance.csv’)
```

- Ingest this CSV data into a Delta table in Databricks.
- Handle potential issues, such as the file being missing or having inconsistent columns, and log these errors.
```python
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

import logging

spark = SparkSession.builder \
.appName("Employee Attendance Ingestion") \
.getOrCreate()

file_path = ’dbfs:/FileStore/employee_attendance.csv’

logging.basicConfig(level=logging.INFO)
try:
    attendance_df = spark.read.option("header", "true")/
    .csv("dbfs:/FileStore/employee_attendance.csv")
    
    attendance_df.write.format("delta").mode("overwrite").save("/mnt/delta/attendance")

except FileNotFoundError:
    print("CSV file is missing.")
except Exception as e:
    print(f"Error during ingestion: {e}")
```

---

### Task 2: Data Cleaning
- Clean the ingested attendance data:
- - Remove any rows with null or invalid values in the CheckInTime or CheckOutTime columns.
- - Ensure the HoursWorked column is calculated correctly ( CheckOutTime - CheckInTime ).
- - Save the cleaned data into a new Delta table.
```python
from pyspark.sql.functions import col, unix_timestamp

cleaned_df = attendance_df.filter(col("CheckInTime").isNotNull() & col("CheckOutTime").isNotNull())

cleaned_df = cleaned_df.withColumn("HoursWorked",(unix_timestamp(col("CheckOutTime"), 'HH:mm') - unix_timestamp(col("CheckInTime"),'HH:mm')) / 3600)

cleaned_df.write.format("delta").mode("overwrite").save("/mnt/delta/cleaned_attendance")
```
---

### Task 3: Attendance Summary
- Create a notebook that summarizes employee attendance:
- - Calculate the total hours worked by each employee for the current month.
- - Find employees who have worked overtime (e.g., more than 8 hours on any given day).
- - Save the summary to a new Delta table.
```python
from pyspark.sql.functions import sum

monthly_summary_df = cleaned_df.groupBy("EmployeeID").agg(sum("HoursWorked")/
.alias("TotalHoursWorked"))

overtime_df = cleaned_df.filter(col("HoursWorked") > 8)

monthly_summary_df.write.format("delta").mode("overwrite")/
.save("/mnt/delta/attendance_summary")

overtime_df.write.format("delta").mode("overwrite").save("/mnt/delta/overtime_summary")
```
---

### Task 4: Create an Attendance Pipeline
- Build a pipeline in Databricks that:
1. Ingests employee attendance data (from Task 1).
2. Cleans the data (from Task 2).
3. Summarizes the attendance and calculates overtime (from Task 3).
- Ensure the pipeline logs errors and handles scenarios like missing data.
```python
def attendance_pipeline():
    try:
        attendance_df = spark.read.option("header","true").csv("/path/to/employee_attendance.csv")
        
        attendance_df.write.format("delta").mode("overwrite").save("/mnt/delta/attendance")
        
        cleaned_df = attendance_df.filter(col("CheckInTime").isNotNull() & col("CheckOutTime").isNotNull())
        
        cleaned_df = cleaned_df.withColumn("HoursWorked",(unix_timestamp(col("CheckOutTime"), 'HH:mm') - unix_timestamp(col("CheckInTime"),'HH:mm')) / 3600)
        
        cleaned_df.write.format("delta").mode("overwrite").save("/mnt/delta/cleaned_attendance")
        
        monthly_summary_df = cleaned_df.groupBy("EmployeeID").agg(sum("HoursWorked")/
        .alias("TotalHoursWorked"))

        overtime_df = cleaned_df.filter(col("HoursWorked") > 8)
        
        monthly_summary_df.write.format("delta").mode("overwrite")/
        .save("/mnt/delta/attendance_summary")
        
        overtime_df.write.format("delta").mode("overwrite").save("/mnt/delta/overtime_summary")
    
    except FileNotFoundError:
        print("CSV file is missing.")
    
    except Exception as e:
        print(f"Error in pipeline: {e}")

attendance_pipeline()
```
---

### Task 5: Time Travel with Delta Lake
- Implement time travel using Delta Lake:
- - Roll back the attendance data to a previous version (e.g., the day before a change was made).
- - Use the DESCRIBE HISTORY command to inspect the changes made to the Delta table.
```python
attendance_previous_df = spark.read.format("delta").option("versionAsOf",1)/
.load("/mnt/delta/cleaned_attendance")

spark.sql("DESCRIBE HISTORY delta.`/mnt/delta/cleaned_attendance`").show()
```
---
---