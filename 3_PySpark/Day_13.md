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