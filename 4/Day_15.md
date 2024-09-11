# Day 15

## Databricks
![alt text](<../Images/New folder/14_1.png>)

## Working with Microsoft Azure
![alt text](<../Images/New folder/14_2.png>)
1. **Resource group:** arranges data into buckets

2. **Region:** places where Microsoft has its datacentres. Stay nearer to the customer so that response time is good. Choose region according to the customer's location.

3. **Managed Resource Group:** It is much more efficient when we use this.

### Machine Learning
![alt text](<../Images/New folder/14_3.png>)

**Data Bricks** allow us o clean data and then this good data is sent to the machine model for machine learning. 

## Cluster
Cluster is leveraged by PySpark to deal with data.

### Steps to work on Microsoft Azure
#### Creating a Azure Databrick
1. Click on Azure Databricks

2. Click on Create
![alt text](<../Images/New folder/14_4.png>)

3. Click on Review+Create

#### Creating a Compute
1. Click on **Compute** in the left corner.

2. Click on Create Compute
![alt text](<../Images/New folder/14_5.png>)

3. Click on Create Compute in the bottom corner.

#### Creating a Notebook

1. Click on Create dropdown menu 

2. Click on Notebook

3. Start typing and change the **Notebook** name if u want.

## Coding in Notebook
### Creating a DataFrame
```python
# Create a Spark DataFrame
data = [("John", 25), ("Jane", 30), ("Sam", 22)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Display the DataFrame
display(df)
```

### Visualizing data
```python
# Display the data as a bar chart
df.groupBy("Age").count().display()
```

**Kafka:** Used to build real-time streaming data pipelines and real-time streaming applications.
<br>
Used for bulk publish and messaging a lot of ppl at the same time.

### Problem of LinkedIn in 2010
![alt text](<../Images/New folder/14_6.png>)

![alt text](<../Images/New folder/14_7.png>)

### Kafka - Solution of LinkedIn Problem
![alt text](<../Images/New folder/14_8.png>)

![alt text](<../Images/New folder/14_9.png>)

![alt text](<../Images/New folder/14_10.png>)

### Message Processing
Here **socket** is doing the work of **Kafka** 
```python
# Read streaming data from a socket (simulated source)
lines = spark.readStream.format("socket").option("host", "localhost").option("port",9999).load()

# Split the lines into words
words = lines.selectExpr("explode(split(value, ' ')) as word")

# Count the number of words
wordCount = words.groupBy("word").count()

# Start the streaming query to console
query = wordCount.writeStream.outputMode("complete").format("console").start()

# Await termination (keep it running)
query.awaitTermination()
```



