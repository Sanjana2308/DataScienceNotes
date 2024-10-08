# Day 20

## Unity Catalog Workflow (Mini project using Unity Catalog)
1. **Data Discovery**
2. **Data Audit**
3. **Data Lineage**
4. **Data Access Control**

We'll create a mini project that mimics a **retail data platform** where you:
1. Set up a **Unity Catalog** with schemas and tables.
2. Insert, update, and manage data in the catalog.
3. Implement **Access Control** to limit user permissions.
4. Explore **Data Lineage** and **Audit Logs** for a set of operations.

---

### Mini Project: Retail Sales Data Governance Platform
**Project Goals:**
1. **Setup a Unity Catalog Metastore**
2. **Create a Sales Data Schema**
3. **Create and Manage Tables in the Catalog**
4. **Set Up Views and Perform Operations on the Data**
5. **Control Access to the Data**
6. **Explore Data Lineage and Auditing**
---

#### Step 1: Setup Unity Catalog Metastore
1. Create a metastore from the Databricks admin console.
2. Assign the metastore to your workspace.
---

#### Step 2: Create a Retail Catalog and Sales Schema
1. **Create the retail_data catalog:**
```sql
CREATE CATALOG retail_data;
```
2. **Create a sales schema in the catalog:**
```sql
CREATE SCHEMA retail_data.sales;
```

#### Step 3: Create Tables in the Sales Schema
1. **Create the product_sales table to store transactional sales data:**
```sql
CREATE TABLE retail_data.sales.product_sales (
SaleID INT,
ProductName STRING,
Quantity INT,
SaleDate DATE
);
```

2. **Insert sample data into the product_sales table:**

```sql
INSERT INTO retail_data.sales.product_sales
VALUES
(1, 'Product A', 10, '2024-01-01'),
(2, 'Product B', 5, '2024-02-01'),
(3, 'Product C', 20, '2024-03-01');
```

3. **Create the customer_data table to store customer information:**
```sql
CREATE TABLE retail_data.sales.customer_data (
CustomerID INT,
CustomerName STRING,
Email STRING,
JoinDate DATE
);
```

4. **Insert sample data into the customer_data table:**
```sql
INSERT INTO retail_data.sales.customer_data
VALUES
(1, 'Abdullah Khan', 'abdullah@example.com', '2023-01-01'),
(2, 'John Smith', 'john@example.com', '2023-02-01'),
(3, 'Sharma', 'sharma@example.com', '2023-03-01');
```
---

#### Step 4: Create Views and Manage Data
1. **Create a View for recent sales (last 30 days):**
```sql
CREATE VIEW retail_data.sales.recent_sales AS
SELECT *
FROM retail_data.sales.product_sales
WHERE SaleDate >= current_date() - INTERVAL 30 DAYS;
```

2. **Create a View to join customer and sales data:**
```sql
CREATE VIEW retail_data.sales.customer_sales AS
SELECT c.CustomerID, c.CustomerName, p.ProductName, p.Quantity, p.SaleDate
FROM retail_data.sales.customer_data c
JOIN retail_data.sales.product_sales p
ON c.CustomerID = p.SaleID;
```
---

#### Step 5: Implement Data Access Controls
1. **Grant read access to a user (e.g., an analyst) to the recent_sales view:**
```sql
GRANT SELECT ON VIEW retail_data.sales.recent_sales TO `analyst@example.com`;
```

2. **Grant full access to the sales data to a manager:**
```sql
GRANT ALL PRIVILEGES ON TABLE retail_data.sales.product_sales TO
`manager@example.com`;
```

3. **Revoke access from a user (if needed):**
```sql
REVOKE SELECT ON VIEW retail_data.sales.recent_sales FROM `analyst@example.com`;
```

---

#### Step 6: Explore Data Lineage and Auditing

1. **Lineage**: Navigate to the Databricks UI under `Catalog Explorer` to check the `lineage` of the product_sales table and recent_sales view.
- Verify that you can track where the data is coming from and where it is used.

2. **Audit Logs**: In the Databricks admin console, view the `Audit Logs` for the operations performed.
- Confirm that logs show actions such as table creation, data insertion,
and access control modifications.

---

#### Step 7: Explore Advanced Capabilities (Optional)

**Data Retention (Vacuum):**
1. **Vacuum the product_sales table to remove files older than 7 days:**
```sql
VACUUM retail_data.sales.product_sales RETAIN 168 HOURS;
```

**Time Travel:**
2. **View the history of the product_sales table:**
~~~sql
DESCRIBE HISTORY retail_data.sales.product_sales;
~~~

3. **Query the table as it existed at a previous version:**
```sql
SELECT *
FROM retail_data.sales.product_sales VERSION AS OF 2;
```

---

#### Conclusion:
This project showcases the use of `Unity Catalog` for setting up data governance in a retail environment. You can:
- Create and manage catalogs, schemas, tables, and views.
- Control data access using SQL commands.
- Explore data lineage and audit logs to ensure the governance of your data.
- Apply advanced capabilities like Vacuum and Time Travel to manage data retention and versioning.
---
<br>

# Assignment
## Exercise: Mini Project Using Unity Catalog and Data Governance
**Objective:**

Develop a mini project using Unity Catalog to demonstrate key data governance
capabilities such as **Data Discovery**, **Data Audit**, **Data Lineage**, and **Access Control**.

---

### Part 1: Setting Up the Environment
**Task 1: Create a Metastore**

- Set up a Unity Catalog metastore that will act as the central location to
manage all catalogs and schemas.

**Task 2: Create Department-Specific Catalogs**

Create separate catalogs for the following departments:
- Marketing
```sql
CREATE CATALOG Marketing;
```

- Engineering
```sql
CREATE CATALOG Engineering;
```

- Operations
```sql
CREATE CATALOG Operations;
```

**Task 3: Create Schemas for Each Department**
Inside each catalog, create specific schemas to store different types of data, e.g.:

- For the Marketing catalog, create schemas such as ads_data and customer_data.
```sql
CREATE SCHEMA Marketing.ads_data;
CREATE SCHEMA Marketing.customer_data;
```

- For the Engineering catalog, create schemas such as projects and development_data.
```sql
CREATE SCHEMA Engineering.projects;
CREATE SCHEMA Engineering.development_data;
```

- For the Operations catalog, create schemas such as logistics_data and supply_chain.
```sql
CREATE SCHEMA Operations.logistics_data;
CREATE SCHEMA Operations.supply_chain;
```

---

### Part 2: Loading Data and Creating Tables

**Task 4: Prepare Datasets**

Use sample datasets for each schema (create CSV or JSON files if required):
- **Marketing - Ads Data**: Contains columns such as ad_id , impressions , clicks , cost_per_click .

```sql
CREATE TABLE Marketing.ads_data.ad_details (
ad_id int,
impressions int,
clicks int,
cost_per_click double);

INSERT INTO Marketing.ads_data.ad_details (ad_id, impressions, clicks,
cost_per_click)
VALUES
(1, 10000, 500, 0.25),
(2, 15000, 750, 0.30),
(3, 12000, 600, 0.20);
```

```sql
CREATE TABLE Marketing.customer_data.customer_detail(
cust_id int,
ad_id int);

INSERT INTO Marketing.customer_data.customer_detail (cust_id, ad_id)
VALUES
(101, 1),
(102, 2),
(103, 3);
```

- **Engineering - Projects**: Contains columns such as project_id , project_name , start_date , end_date .
```sql
CREATE TABLE Engineering.projects.project_data(
project_id int,
project_name string);

INSERT INTO Engineering.projects.project_data (project_id, project_name)
VALUES
(1, 'Website Redesign'),
(2, 'Mobile App Development'),
(3, 'Database Optimization');
```

```sql
CREATE TABLE Engineering.projects.development_data(
dev_id int,
project_id int,
start_data date,
end_date date);

INSERT INTO Engineering.projects.development_data (dev_id, project_id,
start_data, end_date)
VALUES
(1, 1, '2024-01-01', '2024-06-30'),
(2, 2, '2024-03-15', '2024-12-31'),
(3, 3, '2024-02-01', '2024-04-30');
```

- **Operations - Logistics**: Contains columns such as shipment_id , origin ,destination , status .
```sql
CREATE TABLE Operations.logistics_data.logistics (
shipment_id int,
status string);

INSERT INTO Operations.logistics_data.logistics (shipment_id, status)
VALUES
(1001, 'Delivered'),
(1002, 'In Transit'),
(1003, 'Processing');
```
```sql
CREATE TABLE Operations.supply_chain.supply_chain_data(
Id_no int,
origin string,
destination string,
shipment_id int);

INSERT INTO Operations.supply_chain.supply_chain_data (Id_no, origin, destination,
shipment_id)
VALUES
(1, 'Chennai', 'Bangalore', 1001),
(2, 'Chennai', 'Hyderabad', 1002),
(3, 'Chennai', 'Mumbai', 1003);
```

**Task 5: Create Tables from the Datasets**

Load the datasets into their respective schemas as tables.
- Example: Create a table for ads_data in the marketing catalog.
- Example: Create a table for projects in the engineering catalog.

---

### Part 3: Data Governance Capabilities
**Data Access Control**

**Task 6: Create Roles and Grant Access**

Create specific roles for each department and grant access to the relevant catalogs and schemas.
- For example: create roles such as marketing_role , engineering_role , and operations_role .
```sql
CREATE ROLE marketing_role;
CREATE ROLE engineering_role;
CREATE ROLE operations_role;
```

**Task 7: Configure Fine-Grained Access Control**

- Set up fine-grained access control, where users in the marketing department can only access customer-related data, while engineers can only access project data. Define permissions accordingly.

**For Marketing role:**
```sql
GRANT SELECT ON TABLE Marketing.customer_data.customer_detail TO marketing_role;
GRANT SELECT ON TABLE Marketing.ads_data.ad_details TO marketing_role;
```

**For Engineering role**:
```sql
GRANT SELECT ON TABLE Engineering.projects.project_data TO engineering_role;
GRANT SELECT ON TABLE Engineering.projects.development_data TO engineering_role;
```

**For Operations role**:
```sql
GRANT SELECT ON TABLE operations.logistics_data.logistics TO operations_role;
GRANT SELECT ON TABLE operations.supply_chain.supply_chain_data TO operations_role;
```

---

### Data Lineage
**Task 8: Enable and Explore Data Lineage**

- Enable data lineage for the tables created in Part 2.
- Perform some queries (e.g., aggregate queries) on the datasets and examine how the data lineage feature traces the origin of data and tracks transformations.

```
Navigate to the databricks UI to Catalog Explorer to check the lineage of the tables we created
```
---

#### Data Audit
**Task 9: Monitor Data Access and Modifications**

- Set up audit logging to track who is accessing or modifying the datasets.Access the audit logs to view data access patterns and identify who performed which actions on the data.

```
In the Admin Console, we can view the Audit logs for the operations performed.
```
---

### Data Discovery
**Task 10: Explore Metadata in Unity Catalog**
- Explore the metadata of the tables you’ve created. Document information such as table schema, number of rows, and table properties for each department.
- Make sure that the appropriate descriptions and properties are added to each
catalog, schema, and table.


**For Marketing Tables:**
```sql
DESCRIBE TABLE Marketing.ads_data.ad_details;
DESCRIBE TABLE Marketing.customer_data.customer_detail;
SELECT COUNT(*) FROM marketing.ads_data.ad_details;
SELECT COUNT(*) FROM marketing.customer_data.customer_detail;
```

<br>

**For Engineering Tables**:
```sql
DESCRIBE TABLE Engineering.projects.project_data;
DESCRIBE TABLE Engineering.projects.development_data;
SELECT COUNT(*) FROM engineering.projects.project_data;
SELECT COUNT(*) FROM engineering.projects.development_data;
```

**For Operations Tables**:
```sql
DESCRIBE TABLE Operations.logistics_data.logistics;
DESCRIBE TABLE Operations.supply_chain.supply_chain_data;
SELECT COUNT(*) FROM Operations.logistics_data.logistics;
SELECT COUNT(*) FROM Operations.supply_chain.supply_chain_data;
```
---

### Deliverables:
- Department catalogs, schemas, and tables created in Unity Catalog.
- Access roles and controls in place for each department.
- Demonstrations of data governance capabilities such as Data Lineage, Data Audit, and Data Discovery.
---

## Data Pipeline
![alt text](<../Images/Azure DataBricks/20_1.png>)

### Creating a Data Pipeline
- Create a new job
- Create task 1 attach it to the cluster.
- Write task name attach the notebook select cluster
- Click on Create task
- In the grid click on Add task
- Click on notebook


**Data_loading**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("DataLoading").getOrCreate()

# Sample data
data = [(1, "Abdullah", 1000), (2, "Sharma", 1500), (3, "Suman", 1200)]
columns = ["ID", "Name", "Salary"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Write DataFrame to Delta table
df.write.format("delta").mode("overwrite").save("/delta/sample_data")

print("Data loaded and saved as Delta table.")
```

**Data_transformation**
```python
from pyspark.sql.functions import col

# Read from Delta table
df = spark.read.format("delta").load("/delta/sample_data")

# Apply transformation (increase salary by 10$)
df_transformed = df.withColumn("Salary", col("Salary")*1.1)

# Save the transformed data to Delta
df_transformed.write.format("delta").mode("overwrite").save("/delta/transformed_data")

print("Data transformed and saved.")
```

**Data_analysis**
```python
# Read from transformed Delta table
df = spark.read.format("delta").load("/delta/transformed_data")

# Perform analysis: Calculate the average salary
df.groupby().avg("Salary").show()

print("Data analysis complete.")
```
Run jobs at end

## Missing File problem
Designing pipeline that handles missing file.
**1.load_missing_file:**
```python
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("RobustDataLoading").getOrCreate()

# Define file path
file_path = "/dbfs/mnt/data/input_data.csv"

# Check if file exists
if os.path.exists(file_path):
    # Load the data
    try:
        df = spark.read.format("csv").option("header", "true").load(file_path)

    except AnalysisException as e:
        print(f"Failed to load data: {str(a)}")

else:
    # Log missing file error
    print(f"File not found: {file_path}")
```

**2.retry_file_load:**
```python
from pyspark.sql.functions import col
import time

# NUmber of retries
max_retries = 3
retries = 0
success = false

# Define the transformation logic
def transform_data():
    df = spark.read.format("csv").option("header", "true").load("/dbfs/mnt/data/input_data.csv")
    df_transformed = df.withColumn("Salary", col("Salary").cast("double") * 1.1)
    df_transformed.write.format("delta").mode("overwrite").save("/delta/transformed_data")
    print("Data transformed successfully")

# Retry loop
while retries < max_retries and not success:
    try:
        transform_data()
        success = True

    except Exception as e:
        retries+=1
        print(f"Transformation failed. Retry {retries}/{max_retries}")
        time.sleep(5)

if not success:
    print("Transformation failed after max retries")
```

**3.data_analysis**
```python
# Define Delta table path
delta_table_path = "/delta/transformed_data"

# Check if the Delta table exists
try:
    spark.sql(f"DESCRIBE HISTORY delta.`{delta_table_path}`").show()

    # If table exists, perform analysis
    df = spark.read.format("delta").load(delta_table_path)
    df.groupby("Department").avg("Salary").show()
    print("Data analysis completed successfully")

except AnalysisException as e:
    print(f"Data not found or incomplete: {str(e)}. Skipping analysis")
```


