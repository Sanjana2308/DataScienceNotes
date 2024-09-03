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
```python
with open('sales_data.csv', 'r') as file:
    print(file.read())
```

---

### **Part 2: Load and Analyze the Dataset Using PySpark**

Now that you have the dataset, your task is to load it into PySpark and perform the following analysis tasks.

#### **Step 2: Load the Dataset into PySpark**

1. **Initialize the SparkSession:**
   - Create a Spark session named `"Sales Dataset Analysis"`.
```python
import pyspark
from pyspark.sql import SparkSession

SalesDatasetAnalysis = SparkSession.builder.appName("SalesDataAnalysis").getOrCreate()
```
2. **Load the CSV File into a PySpark DataFrame:**
   - Load the `sales_data.csv` file into a PySpark DataFrame.
   - Display the first few rows of the DataFrame to verify that the data is loaded correctly.
```python
sales_data_df = SalesDatasetAnalysis.read.csv("sales_data.csv", header=True, inferSchema=True)

sales_data_df.show(5)
```

#### **Step 3: Explore the Data**

Explore the data to understand its structure.

1. **Print the Schema:**
   - Display the schema of the DataFrame to understand the data types.
```python
sales_data_df.printSchema()
```


2. **Show the First Few Rows:**
   - Display the first 5 rows of the DataFrame.
```python
sales_data_df.show(5)
```

3. **Get Summary Statistics:**
   - Get summary statistics for numeric columns (`Quantity` and `Price`).
```python
sales_data_df.describe(["Quantity", "Price"]).show()
```

#### **Step 4: Perform Data Transformations and Analysis**

Perform the following tasks to analyze the data:

1. **Calculate the Total Sales Value for Each Transaction:**
   - Add a new column called `TotalSales`, calculated by multiplying `Quantity` by `Price`.
```python
sales_data_df = sales_data_df.withColumn("TotalSales", sales_data_df["Quantity"] * sales_data_df["Price"])
sales_data_df.show()
```

2. **Group By ProductID and Calculate Total Sales Per Product:**
   - Group the data by `ProductID` and calculate the total sales for each product.
```python
total_sales_by_product = sales_data_df.groupBy("ProductID").agg({"TotalSales": "sum"})
total_sales_by_product = total_sales_by_product.withColumnRenamed("sum(TotalSales)", "TotalSales")
total_sales_by_product.show()
```

3. **Identify the Top-Selling Product:**
   - Find the product that generated the highest total sales.
```python
top_selling_product = total_sales_by_product.orderBy("TotalSales", descending=True).first()
print("Top-selling product:", top_selling_product)
```


4. **Calculate the Total Sales by Date:**
   - Group the data by `Date` and calculate the total sales for each day.
```python
total_sales_by_date = sales_data_df.groupBy("Date").agg({"TotalSales": "sum"})
total_sales_by_date = total_sales_by_date.withColumnRenamed("sum(TotalSales)", "TotalSales")
total_sales_by_date.show()
```

5. **Filter High-Value Transactions:**
   - Filter the transactions to show only those where the total sales value is greater than ₹500.
```python
high_value_transactions = sales_data_df.filter(sales_data_df["TotalSales"] > 500)
print("High Value Transactions: ")
high_value_transactions.show()
```
---

### **Additional Challenge (Optional):**

If you complete the tasks above, try extending your analysis with the following challenges:

1. **Identify Repeat Customers:**
   - Count how many times each customer has made a purchase and display the customers who have made more than one purchase.
```python
repeat_customers = sales_data_df.groupBy("CustomerID").count().filter("count > 1")
repeat_customers = repeat_customers.withColumnRenamed("count", "PurchaseCount")
repeat_customers.show()
```

2. **Calculate the Average Sale Price Per Product:**
   - Calculate the average price per unit for each product and display the results.
```python
average_sale_price_per_product = sales_data_df.groupBy("ProductID").agg({"Price": "avg"})
average_sale_price_per_product = average_sale_price_per_product.withColumnRenamed("avg(Price)", "AverageSalePrice")
average_sale_price_per_product.show()
```
---

## RDD
We convert our data into RDD for executing parallel processing.

1. Setting up the environment
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName('RDD Transformation Example') \
        .getOrCreate()

# Get the SparkContent from SparkSession
sc = spark.sparkContext
print("Spark Session Created")
```

2. Creating a RDD
```python
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = sc.parallelize(data)
print("Original RDD: ",rdd.collect())
```

3. Map Transformation
```python
# .map() will do a for loop around your rdd
rdd2 = rdd.map(lambda x: x * 2)
# Print the transformed RDD
print("RDD after map transformation (x*2): ",rdd2.collect()) 
```

4. Filter Transformations
```python
rdd3 = rdd2.filter(lambda x: x % 2 == 0)
# Print the filtered RDD
print("RDD after filter transformation (even numbers): ",rdd3.collect())
```

5. flatMap Transformations
```python
sentences = ["Hello world", "PySpark is great", "RDD transformations"]
rdd4 = sc.parallelize(sentences)
words_rdd = rdd4.flatMap(lambda x: x.split(" "))

# Print the flatMapped RDD
print("RDD after flatMap transformation (split into words): ",words_rdd.collect())
```

6. .collect()
```python
results = rdd3.collect()
print(results)
```

7. .count()
```python
count = rdd3.count()
print(f"Number of elements: {count}")
```

8. .reduce()
```python
total_sum = rdd.reduce(lambda x, y: x+y)
print(f"Total sum: {total_sum}")
```

## Hands-on
---

### **Exercise: Working with Key-Value Pair RDDs in PySpark**

#### **Objective:**

In this exercise, you will work with key-value pair RDDs in PySpark. You will create RDDs, perform operations like grouping, aggregating, and sorting, and extract meaningful insights from the data.

---

### **Dataset:**

You will be working with the following sales data. Each entry in the dataset represents a product and its corresponding sales amount.

```python
sales_data = [
    ("ProductA", 100),
    ("ProductB", 150),
    ("ProductA", 200),
    ("ProductC", 300),
    ("ProductB", 250),
    ("ProductC", 100)
]
```

You will also be working with an additional dataset for regional sales:

```python
regional_sales_data = [
    ("ProductA", 50),
    ("ProductC", 150)
]
```

---

### **Step 1: Initialize Spark Context**

1. **Initialize SparkSession and SparkContext:**
   - Create a Spark session in PySpark and use the `spark.sparkContext` to create an RDD from the provided data.
```python
from pyspark.sql import SparkSession


spark = SparkSession.builder \
        .appName("Key-value Pair RDD Exercise") \
        .getOrCreate()

sc = spark.sparkContext
```
---

### **Step 2: Create and Explore the RDD**

2. **Task 1: Create an RDD from the Sales Data**
   - Create an RDD from the `sales_data` list provided above.
   - Print the first few elements of the RDD.
```python
sales_data = [
    ("ProductA", 100),
    ("ProductB", 150),
    ("ProductA", 200),
    ("ProductC", 300),
    ("ProductB", 250),
    ("ProductC", 100)
]

sales_rdd = sc.parallelize(sales_data)
print(sales_rdd.collect())
```
---

### **Step 3: Grouping and Aggregating Data**

3. **Task 2: Group Data by Product Name**
   - Group the sales data by product name using `groupByKey()`.
   - Print the grouped data to understand its structure.
```python
grouped_sales = sales_rdd.groupByKey()
grouped_sales_data = [(k, list(v)) for k, v in grouped_sales.collect()]
print(grouped_sales_data)
```

4. **Task 3: Calculate Total Sales by Product**
   - Use `reduceByKey()` to calculate the total sales for each product.
   - Print the total sales for each product.
```python
total_sales = sales_rdd.reduceByKey(lambda x, y: x + y)
print(total_sales.collect())
```

5. **Task 4: Sort Products by Total Sales**
   - Sort the products by their total sales in descending order.
   - Print the sorted list of products along with their sales amounts.
```python
sorted_sales = total_sales.sortBy(lambda x: x[1], ascending=False)
print(sorted_sales.collect())
```

---

### **Step 4: Additional Transformations**

6. **Task 5: Filter Products with High Sales**
   - Filter the products that have total sales greater than 200.
   - Print the products that meet this condition.
```python
high_sales = total_sales.filter(lambda x: x[1] > 200)
print(high_sales.collect())
```

7. **Task 6: Combine Regional Sales Data**
   - Create another RDD from the `regional_sales_data` list.
   - Combine this RDD with the original sales RDD using `union()`.
   - Calculate the new total sales for each product after combining the datasets.
   - Print the combined sales data.
```python
regional_sales_data = [
    ("ProductA", 50),
    ("ProductC", 150)
]

regional_sales_rdd = sc.parallelize(regional_sales_data)
combined_sales_rdd = sales_rdd.union(regional_sales_rdd)
new_total_sales = combined_sales_rdd.reduceByKey(lambda x, y: x + y)
print(new_total_sales.collect())
```

---

### **Step 5: Perform Actions on the RDD**

8. **Task 7: Count the Number of Distinct Products**
   - Count the number of distinct products in the RDD.
   - Print the count of distinct products.
```python
distinct_products = new_total_sales.keys().distinct().count()
print(f"Number of distinct products: {distinct_products}")
```

9. **Task 8: Identify the Product with Maximum Sales**
   - Find the product with the maximum total sales using `reduce()`.
   - Print the product name and its total sales amount.
```python
max_sales_product = new_total_sales.reduce(lambda x, y: x if x[1] > y[1] else y)
print(f"Product with maximum sales: {max_sales_product[0]}, Sales Amount: {max_sales_product[1]}")
```

---

### **Challenge Task: Calculate the Average Sales per Product**

10. **Challenge Task:**
    - Calculate the average sales amount per product using the key-value pair RDD.
    - Print the average sales for each product.
```python
product_count = combined_sales_rdd.mapValues(lambda x: (x, 1))
total_sales_and_count = product_count.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_sales = total_sales_and_count.mapValues(lambda x: x[0] / x[1])
print(average_sales.collect())
```

---

### **Expected Outcomes**

- **Task 1:** You should be able to create an RDD with key-value pairs and display its contents.
- **Task 2:** Group the data by key (product name) and explore the structure of grouped data.
- **Task 3:** Aggregate data to calculate total sales per product.
- **Task 4:** Sort the products by total sales and explore the results.
- **Task 5:** Filter the products based on a sales threshold.
- **Task 6:** Combine two RDDs and compute the new total sales for each product.
- **Task 7:** Count the number of distinct products in the dataset.
- **Task 8:** Identify the product with the highest sales.
- **Challenge Task:** Calculate and understand the average sales per product.
