# Day 10

## Big Data
[Big Data](https://cloud.google.com/learn/what-is-big-data)

![alt text](Images/10_1.png)

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
![alt text](Images/10_2.png)

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

