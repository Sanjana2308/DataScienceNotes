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