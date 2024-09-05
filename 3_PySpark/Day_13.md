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