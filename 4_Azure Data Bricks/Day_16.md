# Day 16

## Using Matplotlib
For data.csv
```python
# Move the file from Workspace to DBFS
dbutils.fs.cp("file:/Workspace/Shared/data.csv", "dbfs:/FileStore/data.csv")

# Load the file from DBFS
df = spark.read.format("csv").option("header", "true").load("/FileStore/data.csv")
df.show()
```

**Plotting the graph**
```python
import matplotlib.pyplot as plt

# Convert to Pandas
df_pandas = df.groupBy("CARRIER").agg({"ARR_DELAY": "avg"}).toPandas()

# Plot using Matplotlib
df_pandas.plot(kind='bar',x="CARRIER", y="avg(ARR_DELAY)", legend=False)
plt.title("Average Arrival Delay by Airline")
plt.xlabel("Airline")
plt.ylabel("Average Delay (minutes)")
plt.show()
```

**Output:**
![Output](<../Images/Azure DataBricks/15_1.png>)

### Sales data
```csv
Date,Region,Product,Quantity,Price
2024-09-01,North,Widget A,10,25.50
2024-09-01,South,Widget B,5,15.75
2024-09-02,North,Widget A,12,25.50
2024-09-02,East,Widget C,8,22.50
2024-09-03,West,Widget A,15,25.50
2024-09-03,South,Widget B,20,15.75
2024-09-03,East,Widget C,10,22.50
2024-09-04,North,Widget D,7,30.00
2024-09-04,West,Widget B,9,15.75
```

**Loading the file:**
```python
# Move the file from Workspace to DBFS
dbutils.fs.cp("file:/Workspace/Shared/sales_data.csv", "dbfs:/FileStore/sales_data.csv")

# Load the file from DBFS
df_csv = spark.read.format("csv").option("header", "true").load("/FileStore/sales_data.csv")
df_csv.show()
```