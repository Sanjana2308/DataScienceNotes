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

## **Exercise: Product Sales Analysis**

### **Step 1: Create DataFrames**

You will create two DataFrames: one for products and another for sales transactions. Then, you’ll perform operations like joining these DataFrames and analyzing the data.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Product Sales Analysis") \
    .getOrCreate()

# Sample data for products
products = [
    (1, "Laptop", "Electronics", 50000),
    (2, "Smartphone", "Electronics", 30000),
    (3, "Table", "Furniture", 15000),
    (4, "Chair", "Furniture", 5000),
    (5, "Headphones", "Electronics", 2000),
]

# Sample data for sales transactions
sales = [
    (1, 1, 2),
    (2, 2, 1),
    (3, 3, 3),
    (4, 1, 1),
    (5, 4, 5),
    (6, 2, 2),
    (7, 5, 10),
    (8, 3, 1),
]

# Define schema for DataFrames
product_columns = ["ProductID", "ProductName", "Category", "Price"]
sales_columns = ["SaleID", "ProductID", "Quantity"]

# Create DataFrames
product_df = spark.createDataFrame(products, schema=product_columns)
sales_df = spark.createDataFrame(sales, schema=sales_columns)

# Show the DataFrames
print("Products DataFrame:")
product_df.show()

print("Sales DataFrame:")
sales_df.show()
```

### **Step 2: Perform the Following Tasks**

1. **Join the DataFrames:** 
   - Join the `product_df` and `sales_df` DataFrames on `ProductID` to create a combined DataFrame with product and sales data.

```python
joined_df = product_df.join(sales_df, on="ProductID", how="inner")

# Show the joined DataFrame
print("Joined DataFrame:")
joined_df.show()
```

2. **Calculate Total Sales Value:**
   - For each product, calculate the total sales value by multiplying the price by the quantity sold.

```python
total_sales_df = joined_df.withColumn("TotalSalesValue", col("Price") * col("Quantity"))

# Show the DataFrame with total sales value
print("DataFrame with Total Sales Value:")
total_sales_df.show()
```

3. **Find the Total Sales for Each Product Category:**
   - Group the data by the `Category` column and calculate the total sales value for each product category.

```python
total_sales_by_category = total_sales_df.groupBy("Category").agg({"TotalSalesValue": "sum"})

# Show the DataFrame with total sales by category
print("DataFrame with Total Sales by Category:")
total_sales_by_category.show()
```

4. **Identify the Top-Selling Product:**
   - Find the product that generated the highest total sales value.

```python
top_selling_product = total_sales_df.groupBy("ProductName").agg({"TotalSalesValue": "sum"}).orderBy("sum(TotalSalesValue)", ascending=False).limit(1).withColumnRenamed("sum(TotalSalesValue)", "TotalSalesValue")

# Show the top-selling product
print("Top-Selling Product:")
print(top_selling_product.show())
```

5. **Sort the Products by Total Sales Value:**
   - Sort the products by total sales value in descending order.
```python
sorted_products_df = total_sales_df.orderBy("TotalSalesValue", ascending=False)

# Show the DataFrame with products sorted by total sales value
print("DataFrame with Products Sorted by Total Sales Value:")
sorted_products_df.show()
```

6. **Count the Number of Sales for Each Product:**
   - Count the number of sales transactions for each product.

```python
sales_count_df = total_sales_df.groupBy("ProductName").agg({"SaleID": "count"}).withColumnRenamed("count(SaleID)", "SalesCount")

# Show the DataFrame with sales count for each product
print("DataFrame with Sales Count for Each Product:")
sales_count_df.show()
```

7. **Filter the Products with Total Sales Value Greater Than ₹50,000:**
   - Filter out the products that have a total sales value greater than ₹50,000.

```python
filtered_products_df = total_sales_by_category.filter(col("sum(TotalSalesValue)") > 50000)

# Show the DataFrame with filtered products
print("DataFrame with Filtered Products:")
filtered_products_df.show()
```

### **Expected Results:**

1. **Products DataFrame:**

   ```
   +---------+------------+-----------+-----+
   |ProductID| ProductName|   Category|Price|
   +---------+------------+-----------+-----+
   |        1|      Laptop|Electronics|50000|
   |        2|  Smartphone|Electronics|30000|
   |        3|       Table|  Furniture|15000|
   |        4|       Chair|  Furniture| 5000|
   |        5|  Headphones|Electronics| 2000|
   +---------+------------+-----------+-----+
   ```

2. **Sales DataFrame:**

   ```
   +-------+---------+--------+
   | SaleID|ProductID|Quantity|
   +-------+---------+--------+
   |      1|        1|       2|
   |      2|        2|       1|
   |      3|        3|       3|
   |      4|        1|       1|
   |      5|        4|       5|
   |      6|        2|       2|
   |      7|        5|      10|
   |      8|        3|       1|
   +-------+---------+--------+
   ```

### **Additional Notes:**

- Use PySpark DataFrame operations such as `join`, `groupBy`, `agg`, `orderBy`, and `filter` to complete the tasks.
- This exercise will help you practice joining DataFrames, performing aggregations, filtering, and sorting data in PySpark.



---

## **Exercise: Analyzing a Sample Sales Dataset Using PySpark**

In this exercise, you'll work with a simulated sales dataset and perform various data transformations and analyses using PySpark. The dataset includes fields like `TransactionID`, `CustomerID`, `ProductID`, `Quantity`, `Price`, and `Date`. Your task is to generate the dataset, load it into PySpark, and answer specific questions by performing data operations.


### **Part 1: Dataset Preparation**

#### **Step 1: Generate the Sample Sales Dataset**

Before starting the analysis, you'll need to create the sample sales dataset. Use the following Python code to generate the dataset and save it as a CSV file.

1. **Run the Dataset Preparation Script:**

   ```python
   import pandas as pd
   from datetime import datetime

   # Sample sales data
   data = {
       "TransactionID": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
       "CustomerID": [101, 102, 103, 101, 104, 102, 103, 104, 101, 105],
       "ProductID": [501, 502, 501, 503, 504, 502, 503, 504, 501, 505],
       "Quantity": [2, 1, 4, 3, 1, 2, 5, 1, 2, 1],
       "Price": [150.0, 250.0, 150.0, 300.0, 450.0, 250.0, 300.0, 450.0, 150.0, 550.0],
       "Date": [
           datetime(2024, 9, 1),
           datetime(2024, 9, 1),
           datetime(2024, 9, 2),
           datetime(2024, 9, 2),
           datetime(2024, 9, 3),
           datetime(2024, 9, 3),
           datetime(2024, 9, 4),
           datetime(2024, 9, 4),
           datetime(2024, 9, 5),
           datetime(2024, 9, 5)
       ]
   }

   # Create a DataFrame
   df = pd.DataFrame(data)

   # Save the DataFrame to a CSV file
   df.to_csv('sales_data.csv', index=False)

   print("Sample sales dataset has been created and saved as 'sales_data.csv'.")
   ```

2. **Verify the Dataset:**
   - After running the script, ensure that the file `sales_data.csv` has been created in your working directory.

---

### **Part 2: Load and Analyze the Dataset Using PySpark**

Now that you have the dataset, your task is to load it into PySpark and perform the following analysis tasks.

#### **Step 2: Load the Dataset into PySpark**

1. **Initialize the SparkSession:**
   - Create a Spark session named `"Sales Dataset Analysis"`.

2. **Load the CSV File into a PySpark DataFrame:**
   - Load the `sales_data.csv` file into a PySpark DataFrame.
   - Display the first few rows of the DataFrame to verify that the data is loaded correctly.

#### **Step 3: Explore the Data**

Explore the data to understand its structure.

1. **Print the Schema:**
   - Display the schema of the DataFrame to understand the data types.

2. **Show the First Few Rows:**
   - Display the first 5 rows of the DataFrame.

3. **Get Summary Statistics:**
   - Get summary statistics for numeric columns (`Quantity` and `Price`).

#### **Step 4: Perform Data Transformations and Analysis**

Perform the following tasks to analyze the data:

1. **Calculate the Total Sales Value for Each Transaction:**
   - Add a new column called `TotalSales`, calculated by multiplying `Quantity` by `Price`.

2. **Group By ProductID and Calculate Total Sales Per Product:**
   - Group the data by `ProductID` and calculate the total sales for each product.

3. **Identify the Top-Selling Product:**
   - Find the product that generated the highest total sales.

4. **Calculate the Total Sales by Date:**
   - Group the data by `Date` and calculate the total sales for each day.

5. **Filter High-Value Transactions:**
   - Filter the transactions to show only those where the total sales value is greater than ₹500.

---

### **Additional Challenge (Optional):**

If you complete the tasks above, try extending your analysis with the following challenges:

1. **Identify Repeat Customers:**
   - Count how many times each customer has made a purchase and display the customers who have made more than one purchase.

2. **Calculate the Average Sale Price Per Product:**
   - Calculate the average price per unit for each product and display the results.

---
