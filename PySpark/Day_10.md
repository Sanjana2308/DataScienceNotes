# Day 10

## Big Data
[Big Data](https://cloud.google.com/learn/what-is-big-data)

![What happens in an Internet Minute](../Images/PySpark/10_1.png)

## PySpark - Apache
Used to deal with large volume of data<br>
[Apache PySpark](https://aws.amazon.com/what-is/apache-spark/)

## Architecture of Spark
[Architecture of Spark](https://medium.com/@amitjoshi7/spark-architecture-a-deep-dive-2480ef45f0be)

## Components of Apache Spark
[Components of Apache Spark](https://www.knowledgehut.com/tutorials/apache-spark-tutorial/apache-spark-components)

## PySpark
[PySpark](https://www.databricks.com/glossary/pyspark)

## PySpark Coding part

```python
# Import PySpark and initialize a Spark session
from pyspark.sql import SparkSession
# Initialize SparkSession
spark = SparkSession.builder \     
.appName("PySpark Notebook Example") \     
.getOrCreate()
# Verify the Spark session is working 
print(spark)
```

2. Creating DataFrames using PySpark
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession

spark = SparkSession.builder \
    .appName("PySpark DataFrame Example") \
    .getOrCreate()

# Sample data representing employees
data = [
    ("John Doe", "Engineering", 75000),
    ("Jane Smith", "Marketing", 60000),
    ("Sam Brown", "Engineering", 80000),
    ("Emily Davis", "HR", 50000),
    ("Michael Johnson", "Marketing", 70000),
]
 
# Define schema for DataFrame
columns = ["Name", "Department", "Salary"]
 
# Create DataFrame
df = spark.createDataFrame(data, schema=columns)
 
# Show the DataFrame
df.show()
```

3. Filter: Select employees with a salary greater than 65000
```python
high_salary_df = df.filter(col("Salary") > 65000)
print("Employees with a salary greater than 65000: ")
high_salary_df.show()
```

4. Group by Department and calculate the average salary
```python
avg_salary_df = df.groupBy("Department").avg("Salary")
print("Average Salary by Department: ")
avg_salary_df.show()
```