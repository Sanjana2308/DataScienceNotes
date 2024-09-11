# Day 15

## Databricks
![alt text](<../Images/Azure DataBricks/14_1.png>)

## Working with Microsoft Azure
![alt text](<../Images/Azure DataBricks/14_2.png>)

1. **Resource group:** arranges data into buckets

2. **Region:** places where Microsoft has its datacentres. Stay nearer to the customer so that response time is good. Choose region according to the customer's location.

3. **Managed Resource Group:** It is much more efficient when we use this.

### Machine Learning
![alt text](<../Images/Azure DataBricks/14_3.png>)

**Data Bricks** allow us o clean data and then this good data is sent to the machine model for machine learning. 

## Cluster
Cluster is leveraged by PySpark to deal with data.

### Steps to work on Microsoft Azure
#### Creating a Azure Databrick
1. Click on Azure Databricks

2. Click on Create
![alt text](<../Images/Azure DataBricks/14_4.png>)

3. Click on Review+Create

#### Creating a Compute
1. Click on **Compute** in the left corner.

2. Click on Create Compute
![alt text](<../Images/Azure DataBricks/14_5.png>)

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
![alt text](<../Images/Azure DataBricks/14_6.png>)

![alt text](<../Images/Azure DataBricks/14_7.png>)

### Kafka - Solution of LinkedIn Problem
![alt text](<../Images/Azure DataBricks/14_8.png>)

![alt text](<../Images/Azure DataBricks/14_9.png>)

![alt text](<../Images/Azure DataBricks/14_10.png>)

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

### Tasks

#### Task - 1 :Load the data set
1. Load a CSV file into a Spark DataFrame in DataBricks.

```csv
FL_DATE,CARRIER,ORIGIN,DEST,DEP_DELAY,ARR_DELAY
2023-09-01,AA,ATL,DFW,5,10
2023-09-01,UA,LAX,JFK,-3,0
2023-09-01,DL,SFO,ORD,7,15
2023-09-02,AA,DFW,LAX,0,-5
2023-09-02,UA,JFK,ATL,-2,0
2023-09-02,DL,ORD,LAX,20,30
2023-09-03,AA,LAX,SFO,10,12
2023-09-03,UA,ATL,ORD,0,-10
2023-09-03,DL,SFO,JFK,5,25
2023-09-04,AA,JFK,LAX,0,0
2023-09-04,UA,ORD,ATL,15,20
2023-09-04,DL,LAX,SFO,-5,-10
2023-09-05,AA,LAX,JFK,20,25
2023-09-05,UA,DFW,ATL,0,0
2023-09-05,DL,JFK,LAX,10,15
```

2. - Display the first 10 rows and inspect the schema of the dataset.
```python

```

#### Task - 2: Data Cleaning
1. **Handle missing values**: Drop rows with missing values from the dataset.
   - Use `.na.drop()` to remove rows containing `null` values.
   - Verify if there are any null values left using `.filter()`.

   Example:
   ```python
   df_cleaned = df.na.drop()
   df_cleaned.show()
   ```

2. **Filter rows**: Create a filtered DataFrame where arrival delays are greater than `0`.
   
   Example:
   ```python
   df_filtered = df.filter(df['ARR_DELAY'] > 0)
   df_filtered.show()
   ```

#### Task - 3: Aggregation and Summary Statistics
1. **Find the average arrival delay by airline**:
   - Group by `CARRIER` and calculate the average of `ARR_DELAY`.

   Example:
   ```python
   df.groupBy("CARRIER").agg({"ARR_DELAY": "avg"}).show()
   ```

2. **Count the number of flights per airline**:
   - Group by `CARRIER` and count the total number of flights.

   Example:
   ```python
   df.groupBy("CARRIER").count().show()
   ```

3. **Find the minimum and maximum delay** for all flights:
   - Use `.agg()` to calculate both the minimum and maximum delay.

   Example:
   ```python
   df.agg({"ARR_DELAY": "min", "ARR_DELAY": "max"}).show()
   ```

#### Task 4: Data Visualization

1. **Plot the average delay per airline** using Databricks’ built-in visualization tools:
   - Use `display()` to visualize the result from the average delay aggregation.

   Example:
   ```python
   display(df.groupBy("CARRIER").agg({"ARR_DELAY": "avg"}))
   ```

2. **Visualize flight count by airline** using a bar chart:
   - Use the `display()` function and convert the table into a bar chart in the UI.

   Example:
   ```python
   display(df.groupBy("CARRIER").count())
   ```

3. **Plot the distribution of arrival delays** using a histogram:
   - Group by `ARR_DELAY` and count, then use the Databricks visualization tool to create a histogram.

   Example:
   ```python
   display(df.groupBy("ARR_DELAY").count())
   ```

