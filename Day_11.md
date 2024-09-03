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
