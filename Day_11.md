# Day 11

## Data Cleaning

1. 
```python
from pyspark.sql.import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
        .appName("PySpark Notebook Example") \
        .getOrCreate()

# Sample data for customers
customers = [
    (1, "Ravi", "Mumbai"),
    (2, "Priya", "Delhi"),
    (3, "Vijay", "Bangalore"),
    (4, "Anita", "Chennai"),
    (5, "Raj", "Hyderabad")
]

# Sample data for transactions
transactions = [
    (1, 1, 10000.50),
    (2, 2, 20000.75),
    (3, 1, 15000.25),
    (4, 3, 30000.00),
    (5, 2, 40000.50),
    (6, 4, 25000.00),
    (7, 5, 18000.75),
    (8, 1, 5000.00)
]

# Define schema for DataFrames
customer_columns = ["CustomerID", "Name", "City"]
transaction_columns = ["TransactionID", "CustomerID", "Amount"]

# Create DataFrames
customer_df = spark.createDataFrame(customers, schema=customer_columns)
transaction_df = spark.createDataFrame(transactions, schema=transaction_columns)

# Show the DataFrames
print("Customers DataFrame:")
customer_df.show()

print("Transactions DataFrame:")
transaction_df.show()
```

2. 
```python
# Join the DataFrames on CustomerID
customer_transactions_df = customer_df.join(transaction_df, on="CustomerID")

print("Customer Transactions DataFrame:")
customer_transactions_df.show()

# Calculate the total amount spent by each customer
total_spent_df = customer_transactions_df.groupBy("Name").sum("Amount").withColumnRenamed("sum(Amount)", "TotalSpent")  

print("Total Amount Spent by Each Customer:")
total_spent_df.show()

# Find customers who have spent more than 30000
big_spenders_df = total_spent_df.filter(col("TotalSpent") > 30000)

print("Customers who have spent more than 30000:")
big_spenders_df.show()

# Count the number of transactions per customer
transaction_count_df = customer_transactions_df.groupBy("Name").count().withColumnRenamed("count", "TransactionCount")

print("Number of Transactions per Customer:")
transaction_count_df.show()

# Sort customers by total amount spent in descending order
sorted_customers_df = total_spent_df.orderBy(col("TotalSpent").desc())

print("Customers sorted by total amount spent:")
sorted_customers_df.show()
```

