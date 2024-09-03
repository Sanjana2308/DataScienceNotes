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