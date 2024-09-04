# Day 12

## Hands-on Exercise
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Employee Data Analysis") \
    .getOrCreate()

# Sample employee data
data = [
    (1, 'Arjun', 'IT', 75000),
    (2, 'Vijay', 'Finance', 85000),
    (3, 'Shalini', 'IT', 90000),
    (4, 'Sneha', 'HR', 50000),
    (5, 'Rahul', 'Finance', 60000),
    (6, 'Amit', 'IT', 55000)
]

# Define schema (columns)
columns = ['EmployeeID', 'EmployeeName', 'Department', 'Salary']

# Create DataFrame
employee_df = spark.createDataFrame(data, columns)

# Show the DataFrame
employee_df.show()
```

### Tasks:

1. **Task 1: Filter Employees by Salary**  
   Filter the employees who have a salary greater than 60,000 and display the result.

   **Hint**: Use the `filter` method to filter based on the salary column.
```python
high_salary_df = employee_df.filter(col("Salary") > 60000)
high_salary_df.show()
```

2. **Task 2: Calculate the Average Salary by Department**  
   Group the employees by department and calculate the average salary for each department.

   **Hint**: Use `groupBy` and `avg` functions.
```python
avg_salary_by_dept_df = employee_df.groupBy("Department").avg("Salary")
avg_salary_by_dept_df.show()
```

3. **Task 3: Sort Employees by Salary**  
   Sort the employees in descending order of their salary.

   **Hint**: Use the `orderBy` function and sort by the `Salary` column.
```python
sorted_by_salary_df = employee_df.orderBy(col("Salary").desc())
sorted_by_salary_df.show()
```

4. **Task 4: Add a Bonus Column**  
   Add a new column called `Bonus` which should be 10% of the employee's salary.

   **Hint**: Use `withColumn` to add a new column.
```python
bonus_df = employee_df.withColumn("Bonus", col("Salary") * 0.1)
bonus_df.show()
```

## Ways to Handle NULL values

`Data`:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Employee Data Analysis") \
    .getOrCreate()

# Sample employee data
data = [
    (1, 'Arjun', 'IT', 75000),
    (2, 'Vijay', 'Finance', 85000),
    (3, None, 'IT', 90000),
    (4, 'Sneha', 'HR', None),
    (5, 'Rahul', 'None', 60000),
    (6, 'Amit', 'IT', 55000)
]

# Define schema (columns)
columns = ['EmployeeID', 'EmployeeName', 'Department', 'Salary']

# Create DataFrame
employee_df = spark.createDataFrame(data, columns)

# Show the DataFrame
employee_df.show()
```

1. Drop rows with NULL values
```python
# Drop rows where 'Salary' is NULL
dropped_null_salary_df = filled_df.dropna(subset=['Salary'])
dropped_null_salary_df.show()
```

2. Filling up NULL values with a default value
```python
# Fill null values in  'EmployeeName' and 'Department' with 'Unknown'
filled_df = employee_df.fillna({'EmployeeName': 'Unknown', 'Department': 'Unknown'})
filled_df.show()
```

3. Fill NULL values in 'Salary' with 50000
```python
# Fill NULL values in 'Salary' with 50000
salary_filled_df = employee_df.fillna({'Salary': 50000})
salary_filled_df.show()
```

4. Using Functions
```python 
# Check for null values in the entire Dataframe
null_counts = employee_df.select([col(c).isNull().alias(c) for c in employee_df.columns]).show()
```

5. Replace all NULL values with a placeholder (N/A) here
```python
# Replace all NULL values in the DataFrame with 'N/A'
na_filled_df = employee_df.na.fill('N/A')
na_filled_df.show()
```

## Joining DataFrames
1. Creating, joining and showing DataFrames
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Employee Data Analysis") \
    .getOrCreate()

# Create two sample DataFrames
data1 = [
    (1, 'Arjun', 'IT', 75000, '2022-01-15'),
    (2, 'Vijay', 'Finance', 85000, '2022-03-12'),
    (3, 'Shalini', 'IT', 90000, '2021-06-30')
]

data2 =[
    (4, 'Sneha', 'HR', 50000, '2022-05-01'),
    (5, 'Rahul', 'Finance', 60000, '2022-08-20'),
    (6, 'Amit', 'IT', 55000, '2021-12-15')
]

# Define schema (columns)
columns = ['EmployeeID', 'EmployeeName', 'Department', 'Salary', 'JoiningDate']

# Create DataFrame
employee_df1 = spark.createDataFrame(data1, columns)
employee_df2 = spark.createDataFrame(data2, columns)

# Show the DataFrame
employee_df1.show()
employee_df2.show()
```

2. Union of DataFrames(With and without duplicates)
```python
# Union of two DataFrames (remove duplicates)
union_df = employee_df1.union(employee_df2).dropDuplicates()
union_df.show()

# Union of two DataFrames (includes duplicates)
union_all_df = employee_df1.union(employee_df2)
union_all_df.show()
```

3. Rank the employees by their salary<br>
Do this on the `union` database
`Partition` the data by `Department` and `orderby` `Salary` this is the window.
And then rank this window

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

# Define a window specification to rank employees by salary within each department
window_spec = Window.partitionBy('Department').orderBy(col('Salary').desc())

# Add a rank column to the DataFrame
ranked_df = union_all_df.withColumn('Rank', rank().over(window_spec))

# Show the DataFrame with ranks
ranked_df.show()
```

4. Total salary being drawn by the employees
Getting the `running total`<br>
Window contains guy who has joined first comes on top.

```python
from pyspark.sql.functions import sum

# Define a window specification for cumulative sum of salaries within each department
window_spec_sum = Window.partitionBy('Department').orderBy('JoiningDate').rowsBetween(Window.unboundedPreceding, Window.currentRow) 

# Calculate the running total of salaries
running_total_df = union_all_df.withColumn('RunningTotal', sum(col('Salary')).over(window_spec_sum))

# Show the DataFrame with running totals
running_total_df.show()
```

5. Converting String to DateTime
```python
# Convert JoiningDate from string to date type
date_converted_df = union_all_df.withColumn('JoiningDate', F.to_date(col('JoiningDate'), 'yyyy-MM-dd'))

# Show the DataFrame with converted date type
date_converted_df.show()
```

6. Calculate years that have passed since they joined the company
```python
# Calculate the number of years since joining
experience_df = date_converted_df.withColumn('YearsOfExperience', F.round(F.datediff(F.current_date(), col('JoiningDate')) / 365, 2))

# Show the DataFrame with experience information
experience_df.show()
```

7. Add a new column for next evaluation date (one year after joining)
```python
# Add a new column for the next evaluation date (one year after joining)
eval_date_df = date_converted_df.withColumn('NextEvaluationDate', F.date_add(col('JoiningDate'), 365))

# Show the DataFrame with evaluation dates
eval_date_df.show()
```

### Built-in Functions for DataFrames
1. Calculating Average Salary
```python
# Calculate average salary per department
avg_salary_df = union_all_df.groupBy('Department').agg(F.avg('Salary').alias('AverageSalary'))

# Show the DataFrame with average salaries
avg_salary_df.show()
```

2. Calculate total number of employees in your company
```python
# Calculate total number of employees in your company
total_employees_df = union_all_df.agg(F.count('EmployeeID').alias('TotalEmployees'))

# Show the DataFrame with total employees
total_employees_df.show()
```

3. Convert employee names to uppercase
```python
# Convert employee names to uppercase
upper_name_df = union_all_df.withColumn('EmployeeNameUpper', F.upper(col('EmployeeName')))

# Show the DataFrame with uppercase names
upper_name_df.show()
```

## Hands on

### Data Setup:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Advanced DataFrame Operations - Different Dataset") \
    .getOrCreate()

# Create two sample DataFrames for Product Sales
data1 = [
    (1, 'Product A', 'Electronics', 1200, '2022-05-10'),
    (2, 'Product B', 'Clothing', 500, '2022-07-15'),
    (3, 'Product C', 'Electronics', 1800, '2021-11-05')
]

data2 = [
    (4, 'Product D', 'Furniture', 3000, '2022-03-25'),
    (5, 'Product E', 'Clothing', 800, '2022-09-12'),
    (6, 'Product F', 'Electronics', 1500, '2021-10-19')
]

# Define schema (columns)
columns = ['ProductID', 'ProductName', 'Category', 'Price', 'SaleDate']

# Create DataFrames
sales_df1 = spark.createDataFrame(data1, columns)
sales_df2 = spark.createDataFrame(data2, columns)
```

### Tasks:

1. **Union of DataFrames (removing duplicates)**:  
   Combine the two DataFrames (`sales_df1` and `sales_df2`) using `union` and remove any duplicate rows.
```python
union_df = sales_df1.union(sales_df2).dropDuplicates()

print("\nUnion of DataFrames (removing duplicates):")
union_df.show()
```

2. **Union of DataFrames (including duplicates)**:  
   Combine both DataFrames using `unionAll` (replaced by `union`) and include duplicate rows.
```python
union_all_df = sales_df1.union(sales_df2)

print("\nUnion of DataFrames (including duplicates):")
union_all_df.show()
```

3. **Rank products by price within their category**:  
   Use window functions to rank the products in each category by price in descending order.
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

window_spec = Window.partitionBy("Category").orderBy(col("Price").desc())

ranked_products_df = union_all_df.withColumn("Rank",rank().over(window_spec))

print("\nRank products by price within their category:")
ranked_products_df.show()
```

4. **Calculate cumulative price per category**:  
   Use window functions to calculate the cumulative price of products within each category.
```python
from pyspark.sql.functions import sum

windowSpec = Window.partitionBy("Category").orderBy("SaleDate").orderBy("ProductID")

cumulative_price_df = union_all_df.withColumn("CumulativePrice", sum("Price").over(windowSpec))

print("\nCalculate cumulative price per category:")
cumulative_price_df.show()
```

5. **Convert `SaleDate` from string to date type**:  
   Convert the `SaleDate` column from string format to a PySpark date type.
```python
date_converted_df = union_all_df.withColumn("SaleDate", F.to_date(col("SaleDate"), "yyyy-MM-dd"))

print("\nConvert SaleDate from string to date type:")
date_converted_df.show()
```

6. **Calculate the number of days since each sale**:  
   Calculate the number of days since each product was sold using the current date.
```python
number_of_days_since_sale_df = date_converted_df.withColumn("DaysSinceSale", F.datediff(F.current_date(), col("SaleDate")))

print("\nCalculate the number of days since each sale:")
number_of_days_since_sale_df.show()
```

7. **Add a column for the next sale deadline**:  
   Add a new column `NextSaleDeadline`, which should be 30 days after the `SaleDate`.
```python
next_sales_df = date_converted_df.withColumn("NextSaleDeadline", F.date_add(col("SaleDate"), 30))

print("\nAdd a column for the next sale deadline:")
next_sales_df.show()
```

8. **Calculate total revenue and average price per category**:  
   Find the total revenue (sum of prices) and the average price per category.
```python
total_revenue_df = union_all_df.groupBy("Category").agg(
    F.sum("Price").alias("TotalRevenue"),
    F.avg("Price").alias("AveragePrice")
)

print("\nCalculate total revenue and average price per category:")
total_revenue_df.show()
```

9. **Convert all product names to lowercase**:  
   Create a new column with all product names in lowercase.
```python
lower_case_df = union_all_df.withColumn("ProductName", F.lower(col("ProductName")))

print("\nConvert all product names to lowercase:")
lower_case_df.show()
```
