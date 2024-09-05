# Day 13

## Loading files in CSV

### CSV File

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