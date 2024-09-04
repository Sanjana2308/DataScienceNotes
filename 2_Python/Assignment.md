
### **Exercise 1: Creating DataFrame from Scratch**
1. Create a DataFrame with the following columns: `"Product"`, `"Category"`, `"Price"`, and `"Quantity"`. Use the following data:
   - Product: `['Laptop', 'Mouse', 'Monitor', 'Keyboard', 'Phone']`
   - Category: `['Electronics', 'Accessories', 'Electronics', 'Accessories', 'Electronics']`
   - Price: `[80000, 1500, 20000, 3000, 40000]`
   - Quantity: `[10, 100, 50, 75, 30]`
2. Print the DataFrame.

```python
data_1 = {
    'Product': ['Laptop', 'Mouse', 'Monitor', 'Keyboard', 'Phone'],
    'Category': ['Electronics', 'Accessories', 'Electronics', 'Accessories', 'Electronics'],
    'Price': [80000, 1500, 20000, 3000, 40000],
    'Quantity': [10, 100, 50, 75, 30]
}

df_1 = pd.DataFrame(data_1)
print(df_1)
```


### **Exercise 2: Basic DataFrame Operations**
1. Display the first 3 rows of the DataFrame.
2. Display the column names and index of the DataFrame.
3. Display a summary of statistics (mean, min, max, etc.) for the numeric columns in the DataFrame.
```python
print(df_1.head(3)) # first 3 rows
print(df_1.columns) # Column names
print(df_1.index) # index
print(df_1.describe()) # mean, min, max
```


### Exercise 3: Selecting Data
1. Select and display the `"Product"` and `"Price"` columns.
2. Select rows where the `"Category"` is `"Electronics"` and print them.
```python
print(df_1[["Product", "Price"]])
print(df_1[df_1["Category"] == "Electronics"])
```

### **Exercise 4: Filtering Data**
1. Filter the DataFrame to display only the products with a price greater than `10,000`.
2. Filter the DataFrame to show only products that belong to the `"Accessories"` category and have a quantity greater than `50`.
```python
print(df_1[df_1["Price"] > 10000])
print(df_1[(df_1["Category"] == "Accessories") & (df_1["Quantity"] > 50)])
```

### **Exercise 5: Adding and Removing Columns**
1. Add a new column `"Total Value"` which is calculated by multiplying `"Price"` and `"Quantity"`.
2. Drop the `"Category"` column from the DataFrame and print the updated DataFrame.
```python
df_1['Total Value'] = df_1['Price'] * df_1['Quantity']
print(df_1)

df_dropped = df_1.drop(columns=['Category'])
print(df_dropped)
```


### **Exercise 6: Sorting Data**
1. Sort the DataFrame by `"Price"` in descending order.
2. Sort the DataFrame by `"Quantity"` in ascending order, then by `"Price"` in descending order (multi-level sorting).
```python
sorted_df_by_price = df_1.sort_values(by="Price", ascending=False)
print(sorted_df_by_price)

sorted_df_quantity = df_1.sort_values(by="Quantity", ascending=True)
sorted_df_multi_level = sorted_df_quantity.sort_values(by="Price", ascending=False)
print(sorted_df_multi_level)
```

### **Exercise 7: Grouping Data**
1. Group the DataFrame by `"Category"` and calculate the total quantity for each category.
2. Group by `"Category"` and calculate the average price for each category.
```python
df_grouped_1 = df_1.groupby("Category")["Quantity"].mean()
print(df_grouped_1)

df_grouped_2 = df_1.groupby("Category")['Price'].mean()
print(df_grouped_2)
```

### **Exercise 8: Handling Missing Data**
1. Introduce some missing values in the `"Price"` column by assigning `None` to two rows.
2. Fill the missing values with the mean price of the available products.
3. Drop any rows where the `"Quantity"` is less than `50`.
```python
df_1_none = df_1
df_1_none.loc[0, 'Price'] = None
df_1_none.loc[2, 'Price'] = None

mean_price = df_1_none['Price'].mean()
df_1_none['Price'] = df_1_none['Price'].fillna(mean_price)

df_1_dropped = df_1[df_1['Quantity'] < 50]
print("Exercise 8: ",df_1_dropped)
```

### **Exercise 9: Apply Custom Functions**
1. Apply a custom function to the `"Price"` column that increases all prices by 5%.
2. Create a new column `"Discounted Price"` that reduces the original price by 10%.
```python
def add_price(price):
    return price*1.10

df_1_functions = df_1
df_1_functions['Price'] = df_1_functions['Price'].apply(add_price)
print(df_1_functions)

df_1['Discounted Price'] = df_1['Price'].apply(lambda x: x*0.90)
print("Exercise 9:",df_1)
```

### **Exercise 10: Merging DataFrames**
1. Create another DataFrame with columns `"Product"` and `"Supplier"`, and merge it with the original DataFrame based on the `"Product"` column.
```python
data_10 = {
    "Product": ['Laptop', 'Mouse', 'Monitor', 'Keyboard', 'Phone'],
    "Supplier": ['Dell', 'Logitech', 'LG', 'Corsair', 'Samsung']
}
df_new = pd.DataFrame(data_10)

merged_df = pd.merge(df_1, df_new, on='Product')
print("Exercise: 10",merged_df)
```

### **Exercise 11: Pivot Tables**
1. Create a pivot table that shows the total quantity of products for each category and product combination.
```python
pivot_table = pd.pivot_table(merged_df, values='Quantity', index=['Category', 'Product'], aggfunc='sum')
print("Exercise 11:",pivot_table)
```

### **Exercise 12: Concatenating DataFrames**
1. Create two separate DataFrames for two different stores with the same columns (`"Product"`, `"Price"`, `"Quantity"`).
2. Concatenate these DataFrames to create a combined inventory list.
```python
store1_data = {
    "Product": ["A", "B", "C"],
    "Price": [100, 200, 300],
    "Quantity": [10, 20, 30]
}
store1_df = pd.DataFrame(store1_data)

store2_data = {
    "Product": ["D", "E", "F"],
    "Price": [400, 500, 600],
    "Quantity": [40, 50, 60]
}
store2_df = pd.DataFrame(store2_data)

# Concatenate
combined_df = pd.concat([store1_df, store2_df], ignore_index=True)

print(combined_df)
```

### **Exercise 13: Working with Dates**
1. Create a DataFrame with a `"Date"` column that contains the last 5 days starting from today.
2. Add a column `"Sales"` with random values for each day.
3. Find the total sales for all days combined.
```python
data_date = {
    "Date": ['2024-08-30', '2024-08-29', '2024-08-28', '2024-08-27','2024-08-26']
}
df_date = pd.DataFrame(data_date)

df_date['Sales'] = [20000, 60000, 70000, 20000, 50000]

total_sales = df_date['Sales'].sum()

print(df_date)
print("Total Sales: ",total_sales)
```

### **Exercise 14: Reshaping Data with Melt**
1. Create a DataFrame with columns `"Product"`, `"Region"`, `"Q1_Sales"`, `"Q2_Sales"`.
2. Use `pd.melt()` to reshape the DataFrame so that it has columns `"Product"`, `"Region"`, `"Quarter"`, and `"Sales"`.
```python
data = {
    "Product": ["A", "B", "C"],
    "Region": ["East", "West", "North"],
    "Q1_Sales": [100, 200, 300],
    "Q2_Sales": [400, 500, 600]
}
df = pd.DataFrame(data)

# Melt the DataFrame
melted_df = pd.melt(df, id_vars=["Product", "Region"], var_name="Quarter", value_name="Sales")

print(melted_df)
```

### **Exercise 15: Reading and Writing Data**
1. Read the data from a CSV file named `products.csv` into a DataFrame.
2. After performing some operations (e.g., adding a new column or modifying values), write the DataFrame back to a new CSV file named `updated_products.csv`.
```python
df = pd.read_csv("products.csv")

df['NewColumn'] = df['Column1'] + df['Column2']

df.to_csv("updated_products.csv", index=False)
```

### **Exercise 16: Renaming Columns**
1. Given a DataFrame with columns `"Prod"`, `"Cat"`, `"Price"`, `"Qty"`, rename the columns to `"Product"`, `"Category"`, `"Price"`, and `"Quantity"`.
2. Print the renamed DataFrame.
```python
data = {
    "Prod": ["A", "B", "C"],
    "Cat": ["X", "Y", "Z"],
    "Price": [100, 200, 300],
    "Qty": [10, 20, 30]
}
df = pd.DataFrame(data)

df.columns = ["Product", "Category", "Price", "Quantity"]

print(df)
```

### **Exercise 17: Creating a MultiIndex DataFrame**
1. Create a DataFrame using a MultiIndex (hierarchical index) with two levels: `"Store"` and `"Product"`. The DataFrame should have columns `"Price"` and `"Quantity"`, representing the price and quantity of products in different stores.
2. Print the MultiIndex DataFrame.
```python
index = pd.MultiIndex.from_tuples([('Store1', 'Product A'), ('Store1', 'Product B'),
                                   ('Store2', 'Product A'), ('Store2', 'Product B')],
                                  names=['Store', 'Product'])

data = {
    "Price": [100, 150, 200, 250],
    "Quantity": [50, 75, 100, 125]
}
df = pd.DataFrame(data, index=index)

print(df)
```

### **Exercise 18: Resample Time-Series Data**
1. Create a DataFrame with a `"Date"` column containing a range of dates for the past 30 days and a `"Sales"` column with random values.
2. Resample the data to show the total sales by week.
```python
df_date.set_index('Date', inplace=True)
resampled_df = df_date.resample('W').sum()

print(resampled_df)
```

### **Exercise 19: Handling Duplicates**
1. Given a DataFrame with duplicate rows, identify and remove the duplicate rows.
2. Print the cleaned DataFrame.
```python
data = {
    "Product": ["A", "B", "C", "A"],
    "Price": [100, 200, 300, 100],
    "Quantity": [50, 75, 100, 50]
}
df = pd.DataFrame(data)

# Remove duplicate rows based on all columns
df = df.drop_duplicates()

print(df)
```

### **Exercise 20: Correlation Matrix**
1. Create a DataFrame with numeric data representing different features (e.g., `"Height"`, `"Weight"`, `"Age"`, `"Income"`).
2. Compute the correlation matrix for the DataFrame.
3. Print the correlation matrix.
```python
data = {
    "Height": [170, 180, 165, 175],
    "Weight": [70, 80, 65, 75],
    "Age": [25, 30, 28, 27],
    "Income": [50000, 60000, 45000, 55000]
}
df = pd.DataFrame(data)

# Compute the correlation matrix
correlation_matrix = df.corr()

print(correlation_matrix)
```

### **Exercise 21: Cumulative Sum and Rolling Windows**
1. Create a DataFrame with random sales data for each day over the last 30 days.
2. Calculate the cumulative sum of the sales and add it as a new column `"Cumulative Sales"`.
3. Calculate the rolling average of sales over the past 7 days and add it as a new column `"Rolling Avg"`.
```python
df_date['Cumulative Sales'] = df_date['Sales'].cumsum()
df_date['Rolling Avg'] = df_date['Sales'].rolling(window=7).mean()

print(df)
```

### **Exercise 22: String Operations**
1. Create a DataFrame with a column `"Names"` containing values like `"John Doe"`, `"Jane Smith"`, `"Sam Brown"`.
2. Split the `"Names"` column into two separate columns: `"First Name"` and `"Last Name"`.
3. Convert the `"First Name"` column to uppercase.
```python
data = {
"Names": ["John Doe", "Jane Smith", "Sam Brown"]
}
df = pd.DataFrame(data)

df[["First Name", "Last Name"]] = df["Names"].str.split(expand=True)

df["First Name"] = df["First Name"].str.upper()

print(df)
```

### **Exercise 23: Conditional Selections with `np.where`**
1. Create a DataFrame with columns `"Employee"`, `"Age"`, and `"Department"`.
2. Create a new column `"Status"` that assigns `"Senior"` to employees aged 40 or above and `"Junior"` to employees below 40 using `np.where()`.
```python
data = {
    "Employee": ["Alice", "Bob", "Charlie", "David"],
    "Age": [35, 45, 28, 42],
    "Department": ["Sales", "HR", "IT", "Finance"]
}
df = pd.DataFrame(data)

# Create a new column "Status" using np.where()
df["Status"] = np.where(df["Age"] >= 40, "Senior", "Junior")

print(df)
```

### **Exercise 24: Slicing DataFrames**
1. Given a DataFrame with data on `"Products"`, `"Category"`, `"Sales"`, and `"Profit"`, slice the DataFrame to display:
   - The first 10 rows.
   - All rows where the `"Category"` is `"Electronics"`.
   - Only the `"Sales"` and `"Profit"` columns for products with sales greater than 50,000.
```python
data = {
    "Products": ["Laptop", "Smartphone", "TV", "Camera", "Headphones"],
    "Category": ["Electronics", "Electronics", "Electronics", "Electronics", "Electronics"],
    "Sales": [100000, 80000, 70000, 50000, 20000],
    "Profit": [30000, 20000, 15000, 10000, 5000]
}
df = pd.DataFrame(data)

first_10_rows = df.head(10)

electronics_category = df[df["Category"] == "Electronics"]

high_sales_data = df[df["Sales"] > 50000][["Sales", "Profit"]]

print("First 10 rows:")
print(first_10_rows)
print("\nElectronics category:")
print(electronics_category)
print("\nHigh sales data:")
print(high_sales_data)
```

### **Exercise 25: Concatenating DataFrames Vertically and Horizontally**
1. Create two DataFrames with identical columns `"Employee"`, `"Age"`, `"Salary"`, but different rows (e.g., one for employees in `"Store A"` and one for employees in `"Store B"`).
2. Concatenate the DataFrames vertically to create a combined DataFrame.
3. Now create two DataFrames with different columns (e.g., `"Employee"`, `"Department"` and `"Employee"`, `"Salary"`) and concatenate them horizontally based on the common `"Employee"` column.
```python
df1 = pd.DataFrame({
    "Employee": ["Alice", "Bob", "Charlie"],
    "Age": [30, 35, 28],
    "Salary": [50000, 60000, 45000]
})

df2 = pd.DataFrame({
    "Employee": ["David", "Emily", "Frank"],
    "Age": [25, 32, 40],
    "Salary": [40000, 55000, 70000]
})

combined_df = pd.concat([df1, df2])
print("Vertically concatenated DataFrame:")
print(combined_df)

df3 = pd.DataFrame({
    "Employee": ["Alice", "Bob", "Charlie"],
    "Department": ["Sales", "HR", "IT"]
})

df4 = pd.DataFrame({
    "Employee": ["Alice", "Bob", "Charlie"],
    "Salary": [50000, 60000, 45000]
})

merged_df = pd.merge(df3, df4, on="Employee")
print("\nHorizontally concatenated DataFrame:")
print(merged_df)
```

### **Exercise 26: Exploding Lists in DataFrame Columns**
1. Create a DataFrame with a column `"Product"` and a column `"Features"` where each feature is a list (e.g., `["Feature1", "Feature2"]`).
2. Use the `explode()` method to create a new row for each feature in the list, so each product-feature pair has its own row.
```python
data = {
    "Product": ["Product A", "Product B"],
    "Features": [["Feature1", "Feature2"], ["Feature3", "Feature4"]]
}
df = pd.DataFrame(data)

# Use the explode() method to create a new row for each feature
exploded_df = df.explode("Features")

print(exploded_df)
```

### **Exercise 27: Using `.map()` and `.applymap()`**
1. Given a DataFrame with columns `"Product"`, `"Price"`, and `"Quantity"`, use `.map()` to apply a custom function to increase `"Price"` by 10% for each row.
2. Use `.applymap()` to format the numeric values in the DataFrame to two decimal places.
```python
data = {
    "Product": ["Item1", "Item2", "Item3"],
    "Price": [100, 120, 150],
    "Quantity": [5, 8, 12]
}
df = pd.DataFrame(data)

def increase_price(price):
    return price * 1.1

df["Price"] = df["Price"].map(increase_price)

df = df.applymap(lambda x: round(x, 2) if isinstance(x, float) else x)

print(df)
```

### **Exercise 28: Combining `groupby()` with `apply()`**
1. Create a DataFrame with `"City"`, `"Product"`, `"Sales"`, and `"Profit"`.
2. Group by `"City"` and apply a custom function to calculate the profit margin (Profit/Sales) for each city.
```python
data = {
    "City": ["New York", "Los Angeles", "Chicago", "New York", "Los Angeles"],
    "Product": ["Product A", "Product B", "Product C", "Product D", "Product E"],
    "Sales": [10000, 8000, 12000, 5000, 7000],
    "Profit": [3000, 2500, 4000, 1500, 2000]
}
df = pd.DataFrame(data)

def calculate_profit_margin(group):
    return group["Profit"].sum() / group["Sales"].sum()

profit_margins = df.groupby("City").apply(calculate_profit_margin)

print(profit_margins)
```

### **Exercise 29: Creating a DataFrame from Multiple Sources**
1. Create three different DataFrames from different sources (e.g., CSV, JSON, and a Python dictionary).
2. Merge the DataFrames based on a common column and create a consolidated report.
```python
csv_data = pd.read_csv("data.csv")

json_data = pd.read_json("data.json")

dict_data = pd.DataFrame({
    "Column1": [1, 2, 3],
    "Column2": ["A", "B", "C"]
})

merged_df = pd.merge(csv_data, json_data, on="Column1")
merged_df = pd.merge(merged_df, dict_data, on="Column1")
```

### **Exercise 30: Dealing with Large Datasets**
1. Create a large DataFrame with 1 million rows, representing data on `"Transaction ID"`, `"Customer"`, `"Product"`, `"Amount"`, and `"Date"`.
2. Split the DataFrame into smaller chunks (e.g., 100,000 rows each), perform a simple analysis on each chunk (e.g., total sales), and combine the results.
```python
num_rows = 1000000
data = {
    "Transaction ID": np.arange(num_rows),
    "Customer": np.random.choice(["Customer A", "Customer B", "Customer C"], num_rows),
    "Product": np.random.choice(["Product X", "Product Y", "Product Z"], num_rows),
    "Amount": np.random.randint(100, 1000, num_rows),
    "Date": pd.date_range(start="2023-01-01", periods=num_rows)
}
df = pd.DataFrame(data)

chunk_size = 100000
chunks = [df[i:i+chunk_size] for i in range(0, len(df), chunk_size)]

total_sales_per_chunk = [chunk["Amount"].sum() for chunk in chunks]
total_sales = sum(total_sales_per_chunk)

print("Total sales:", total_sales)
```

