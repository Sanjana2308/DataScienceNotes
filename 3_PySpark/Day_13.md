# Day 13

## Loading files in CSV

### CSV File
CSV File:
```csv
Name, Age, Gender
John, 28, Male
Jane, 32, Female
```

1. Create a spark session
```python
from pyspark.sql import SparkSession

# Create a spark session
spark = SparkSession.builder\
        .appName('Data Ingestion')\
        .getOrCreate()
``` 

2. Show file in the form of DataFrames

```python
csv_file_path = "/content/sample_data/People.csv"
df_csv = spark.read.format("csv").option("header", "true").load(csv_file_path)
df_csv.show()
```


## JSON File
```json
[
  {
    "name": "John",
    "age": 28,
    "gender": "Male",
    "address": {
      "street": "123 Main St",
      "city": "New York"
    }
  },
  {
    "name": "Jane",
    "age": 32,
    "gender": "Female",
    "address": {
      "street": "456 Elm St",
      "city": "San Francisco"
    }
  }
]
```

1. Create a SparkSession
```python
from pyspark.sql import SparkSession

# Create a spark session
spark = SparkSession.builder\
        .appName('Data Ingestion')\
        .getOrCreate()
``` 

2. Create a schema for JSON File
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema for the JSON file
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True)
    ]), True)
])
```

3. Load the JSON file and read it
```python
# Load the complex JSON file with the correct path
json_file_path = "/content/sample_data/sample.json"

# Read the JSON File with schema
df_json_complex = spark.read.schema(schema).load(json_file_path)

# Read the file as text to inspect its contents
with open(json_file_path, "r") as file:
    data = file.read()
    print(data)
```

## Temporary Views

### Create a CSV file 
```python
import pandas as pd

data = {
    "name": ["John", "Jane", "Mike", "Emily"],
    "age": [28, 32, 35, 23],
    "gender": ["Male", "Female", "Male", "Female"],
    "city": ["New York", "San Francisco", "Los Angeles", "Chicago"]
}

df = pd.DataFrame(data)

# Save the DataFrame to a CSV file in the Colab environment
csv_file_path = "/content/sample_people.csv"
df.to_csv(csv_file_path, index=False)

# Confirm the file has been created
print(f"CSV file created at: {csv_file_path}")
```

### Create a Spark Session
```python
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder\
        .appName("CSVtoParquet")\
        .getOrCreate()

# Load the CSV file into a PySpark DataFrame
df_people = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_file_path)

# Show the DataFrame
df_people.show()
```

### Creating local and temporary views
Available only in the notebook in which we have created it.
```python
# Create a temporary view 
df_people.createOrReplaceTempView("people_temp_view")

# Run SQL queries on the view
result_temp_view = spark.sql("SELECT name, age, gender, city FROM people_temp_view WHERE age > 30")

# Show the result
result_temp_view.show()
```


### Creating global views
Available in all the notebooks of PySpark
```python
# Create a global temporary view 
df_people.createOrReplaceGlobalTempView("people_global_view")

# Run SQL queries on the global temporary view
result_global_view = spark.sql("SELECT name, age, gender, city FROM global_temp.people_global_view WHERE age < 30")

# Show the result
result_global_view.show()
```

### Listing all Temporary Views
```python
# List all temporary views and tables
spark.catalog.listTables()
```

### Dropping temporary views
```python
# Drop the local temporary view
spark.catalog.dropTempView("people_temp_view")

# Drop the global temporary view
spark.catalog.dropGlobalTempView("people_global_view")
```

## Creating a Database in Python
```python
# Create a new database in Spark SQL
spark.sql("CREATE DATABASE IF NOT EXISTS my_database")
 
# Use the created database
spark.sql("USE my_database")
 
# Verify that the database is being used
spark.sql("SHOW DATABASES").show()
```

## ETL - Extract, Transform, Load
1. Extract the Data
```python
import pandas as pd
 
# Create a sample CSV data
data = {
    "name": ["John", "Jane", "Mike", "Emily", "Alex"],
    "age": [28, 32, 45, 23, 36],
    "gender": ["Male", "Female", "Male", "Female", "Male"],
    "salary": [60000, 72000, 84000, 52000, 67000]
}
 
df = pd.DataFrame(data)
 
# Save the DataFrame as a CSV file
csv_file_path = "/content/sample_people.csv"
df.to_csv(csv_file_path, index=False)
 
# Confirm the CSV file is created
print(f"CSV file created at: {csv_file_path}")
```

2. Transform the data 


3. Load the Data

## Hands-on Exercise on ETL
### Problem Statement: Employee Salary Data Transformation and Analysis

A company has collected a CSV file containing employee data, including names, ages, genders, and salaries. The company’s management is interested in conducting a detailed analysis of their workforce, focusing on the salary structure. They need to implement an ETL (Extract, Transform, Load) pipeline to transform the raw employee data into a more usable format for business decision-making.

**Objective**:
The goal is to build an ETL pipeline using PySpark to transform the raw employee data by applying filtering, creating new salary-related metrics, and calculating salary statistics by gender. After the transformations, the processed data should be saved in an efficient file format (Parquet) for further analysis and reporting.

### **Task Requirements**:
1. **Extract**:
   - Load the employee data from a CSV file containing the following columns: `name`, `age`, `gender`, and `salary`.
   
2. **Transform**:
   - **Filter**: Only include employees aged 30 and above in the analysis.
   - **Add New Column**: Calculate a 10% bonus on the current salary for each employee and add it as a new column (`salary_with_bonus`).
   - **Aggregation**: Group the employees by gender and compute the average salary for each gender.
   
3. **Load**:
   - Save the transformed data (including the bonus salary) in a Parquet file format for efficient storage and retrieval.
   - Ensure the data can be easily accessed for future analysis or reporting.

### **Key Deliverables**:
1. A PySpark-based ETL pipeline that performs the following:
   - Loads the raw employee CSV data.
   - Applies filtering, transformations, and aggregations.
   - Saves the transformed data to a Parquet file.
2. A summary report showing the following:
   - The list of employees aged 30 and above with their original salary and salary with the 10% bonus.
   - The average salary per gender.

### **Sample Data**:

| name  | age  | gender | salary  |
|-------|------|--------|---------|
| John  | 28   | Male   | 60000   |
| Jane  | 32   | Female | 72000   |
| Mike  | 45   | Male   | 84000   |
| Emily | 23   | Female | 52000   |
| Alex  | 36   | Male   | 67000   |

### **Expected Output**:

1. A filtered DataFrame that shows the employees aged 30 and above, with an additional column `salary_with_bonus` (10% bonus added to their salary).
   
2. A Parquet file containing the transformed data.

3. A DataFrame showing the average salary by gender.

### **Challenges**:
- The raw data may contain employees below the age threshold of 30, who need to be filtered out.
- Calculating new metrics (like salary bonuses) and ensuring data integrity during transformation.
- Efficiently saving the transformed data in a format suitable for large-scale data analytics (e.g., Parquet).

### **Success Criteria**:
- The company should be able to retrieve the filtered and transformed data with accurate salary information, including the bonus.
- The saved Parquet file should be structured for efficient retrieval and further analysis.
- The aggregated data (average salary by gender) should provide insights into the company's pay structure across genders.

`Answer`: CSV File Creation
```python
import pandas as pd
 
# Create a sample CSV data
data = {
    "name": ["John", "Jane", "Mike", "Emily", "Alex"],
    "age": [28, 32, 45, 23, 36],
    "gender": ["Male", "Female", "Male", "Female", "Male"],
    "salary": [60000, 72000, 84000, 52000, 67000]
}
 
df = pd.DataFrame(data)
 
# Save the DataFrame as a CSV file
csv_file_path = "/content/sample_people.csv"
df.to_csv(csv_file_path, index=False)
 
# Confirm the CSV file is created
print(f"CSV file created at: {csv_file_path}")
```

1. Extract Data:
```python
# Load employee data from CSV
csv_file_path = "/content/sample_people.csv"
df = pd.read_csv(csv_file_path)
```

2. Transform Data:
```python
# Filter employees aged 30 and above
df_filtered = df[df["age"] >= 30]

print("\nFiltered Employee Data:")
print(df_filtered)

# Calculate bonus and add as a new column
df_bonus = df
df_bonus["salary_with_bonus"] = df_bonus["salary"] * 1.1

print("\nEmployee Data with Bonus:")
print(df_bonus)

# Group by gender and calculate average salary
average_salary_by_gender = df.groupby("gender")["salary"].mean()

print("Average salary by gender:")
print(average_salary_by_gender)
```

3. Load/save data to a Parquet file
```python
# Save transformed data in Parquet format
parquet_file_path = "/content/employee_data.parquet"
df.to_parquet(parquet_file_path, index=False)

print("Parquet file created at:", parquet_file_path)
```

## Full Refresh with partitions
Partition data into 3 partitions of data to work on the separately

#### Write the data partition by date and give parquet as output
`Parquet`: is a special type of File which converts the file into columner file format which is understood by tools like `Hadoop` and `Hive` and easy to process rather than JSON and CSV. Helps to deal with large data sets at a time.<br>
When we deal with data we convert the data into log files and new log files are created of manageable size like 500Mb. It is easy to transfer over the internet. 
<br>
If we are dealing with a `Parquet` then ultimately we are dealing with Gb or Terabytes of data.

`CSV File`:
```csv
transaction_id,date,customer_id,product,quantity,price,updated_at
1,2024-09-01,101,Laptop,1,1000,2024-09-01 08:00:00
2,2024-09-01,102,Phone,2,500,2024-09-01 09:00:00
3,2024-09-02,103,Tablet,1,300,2024-09-02 10:00:00
4,2024-09-02,104,Monitor,2,200,2024-09-02 11:00:00
5,2024-09-03,105,Keyboard,1,50,2024-09-03 12:00:00
6,2024-09-03,106,Mouse,3,30,2024-09-03 13:00:00

```

1. Creaing a Spark Session
```python
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder\
        .appName("SparkSQLExample")\
        .getOrCreate()
```

2. Converting to Parquet File
```python
# Full refresh load the entire dataset
df_sales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/content/sample_data/sales_data.csv")

# Apply transformations (if necessary)
df_transformed = df_sales.withColumn("total_sales", df_sales["quantity"]* df_sales["price"])

# Full refresh: Partition the data by date and overwrite the existing data
output_path = "/content/sample_data/partitioned_data"
df_transformed.write.partitionBy("date").mode("overwrite").parquet(output_path)

# Verify partitioned data
partitioned_df = spark.read.parquet(output_path)
partitioned_df.show()
```

### Incremental Load
Add new partitions only if necessary<br>
`Incremental Update`: U dont have to overwrite everything u just need to add new things.
<br>
Append data only if necessary.<br>
Using `.mode("append)` in the above code.

```python
# Full refresh load the entire dataset
df_sales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/content/sample_data/sales_data.csv")

# Apply transformations (if necessary)
df_transformed = df_sales.withColumn("total_sales", df_sales["quantity"]* df_sales["price"])

# Full refresh: Partition the data by date and overwrite the existing data
output_path = "/content/sample_data/partitioned_data"
df_transformed.write.partitionBy("date").mode("append").parquet(output_path)

# Verify partitioned data
partitioned_df = spark.read.parquet(output_path)
partitioned_df.show()
```

## Widgets

Dropdowns, Sliders, Buttons, Output

### Install widgets
```python
# Install ipywidgets in Colab or Jupyter if needed
!pip install ipywidgets
```

### Code
```python
from pyspark.sql import SparkSession
import ipywidgets as widgets
from IPython.display import display
 
# Step 1: Initialize a Spark session
spark = SparkSession.builder.appName("PySpark with Widgets Example").getOrCreate()

# Step 2: Create a simple DataFrame
data = [
    ("John", 28, "Male", 60000),
    ("Jane", 32, "Female", 72000),
    ("Mike", 45, "Male", 84000),
    ("Emily", 23, "Female", 52000),
    ("Alex", 36, "Male", 67000)
]
 
df = spark.createDataFrame(data, ["name", "age", "gender", "salary"])
 
# Show the DataFrame
df.show()

# Step 3: Create widgets

# Dropdown widget to select column for filtering
column_dropdown = widgets.Dropdown(
    options=["age", "salary"],
    value="age",
    description='Filter by:'
)

# Slider widget to choose a value for filtering
slider = widgets.IntSlider(
    value=30,
    min=20,
    max=100,
    step=5,
    description='Threshold:',
    continuous_update=False
)

# Button to trigger filtering
button = widgets.Button(description="Apply Filter")

# Output area to show the results
output = widgets.Output()

# Display the widgets 
display(column_dropdown, slider, button, output)

# Step 4: Define the function to apply filtering based on widget inputs

def apply_filter(b):
    column = column_dropdown.value
    threshold = slider.value

    # Clear previous output
    output.clear_output()

    # Filter the DataFrame based on widget values
    df_filtered = df.filter(df[column] > threshold)

    # Show the filtered DataFrame
    with output:
        print(f"Filtering by column {column} > {threshold}")
        df_filtered.show()

# Step 5: Attach the function to the button's click event
button.on_click(apply_filter)
```

### DAG - Directed Acyclic Graph
`Load on demand`: Loads data only when we say show data until then everything is in the logical memory.<br>
When we say show then the master node sends the data to slave node where data is executed and then again returned to the master node.<br>
Here, we are differing the load time by showing the data only when we use show or collect operation until then data is differentiated.

The graph is `Directed` i.e., has edges and `Acyclic` i.e., the data is not interdependent.

## Hands on Exercise

### **Exercise: PySpark Data Transformations on Movie Data**

#### **Objective:**
You have a dataset containing movie details. The goal is to use PySpark to apply data transformations to derive insights.

#### **Dataset**:

Here’s a sample dataset of movies (save this as a CSV file if necessary):

```csv
movie_id,title,genre,rating,box_office,date
1,Inception,Sci-Fi,8.8,830000000,2010-07-16
2,The Dark Knight,Action,9.0,1004000000,2008-07-18
3,Interstellar,Sci-Fi,8.6,677000000,2014-11-07
4,Avengers: Endgame,Action,8.4,2797000000,2019-04-26
5,The Lion King,Animation,8.5,1657000000,1994-06-15
6,Toy Story 4,Animation,7.8,1073000000,2019-06-21
7,Frozen II,Animation,7.0,1450000000,2019-11-22
8,Joker,Drama,8.5,1074000000,2019-10-04
9,Parasite,Drama,8.6,258000000,2019-05-30
```

### **Tasks**:

1. **Load the Dataset**:
   - Read the CSV file into a PySpark DataFrame.
```python
from pyspark.sql import SparkSession

# Define the path to your CSV file
csv_file_path = "/content/sample_data/movies.csv"

# Create a SparkSession
spark = SparkSession.builder\
        .appName("CSVtoParquet")\
        .getOrCreate()

# Load the CSV file into a PySpark DataFrame
df_movies = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_file_path)

# Show the DataFrame
df_movies.show()
```

2. **Filter Movies by Genre**:
   - Find all movies in the "Sci-Fi" genre.

```python
sci_fi_movies = df_movies.filter(df_movies.genre == "Sci-Fi")
sci_fi_movies.show()
```

3. **Top-Rated Movies**:
   - Find the top 3 highest-rated movies.

```python
top_rated_movies = df_movies.orderBy(df_movies.rating.desc()).limit(3)
top_rated_movies.show()
```

4. **Movies Released After 2010**:
   - Filter out all movies released after the year 2010.

```python
from pyspark.sql.functions import year

movies_after_2010 = df_movies.filter(year(df_movies.date) > 2010)
movies_after_2010.show()
```

5. **Calculate Average Box Office Collection by Genre**:
   - Group the movies by `genre` and calculate the average box office collection for each genre.
```python
from pyspark.sql.functions import avg

average_box_office_by_genre = df_movies.groupBy("genre").agg(avg("box_office").alias("average_box_office"))
average_box_office_by_genre.show()
```

6. **Add a New Column for Box Office in Billions**:
   - Add a new column that shows the box office collection in billions.

```python
df_movies = df_movies.withColumn("box_office_billions", df_movies.box_office / 1000000000)
df_movies.show()
```

7. **Sort Movies by Box Office Collection**:
   - Sort the movies in descending order based on their box office collection.

```python
sorted_movies_by_box_office = df_movies.orderBy(df_movies.box_office.desc())
sorted_movies_by_box_office.show()
```

8. **Count the Number of Movies per Genre**:
   - Count the number of movies in each genre.
```python
movie_count_by_genre = df_movies.groupBy("genre").count()
movie_count_by_genre.show()
```
