# Day 8

## APIs - Application Programming Interface
A project we work on will have 3 layers:
1. UI Layer
2. API Layer
3. Database

API layer returns a JSON file because JSON is cross-platform. 

Examples of API: 
1. Cricket Score
2. Stock market

### Methods of APIs
1. __get()__: used to request data from a server. The data is typically in the form of a file or a web page.<br>
__.json()__: This method attempts to decode the response content into a Python dictionary. Since the requests library automatically detects JSON data, it uses the appropriate decoder.

```python
import requests

response = requests.get("https://dummyjson.com/products/1")
print(response.json()) # Print the HTTP status code
```

## DataFrames
pandas allows us to convert data to dataframes using DataFrames class.<br>
Improves data viewing by converting dictionary to dataframes.
```python
import pandas as pd

# Creating a DataFrame from a dictionary with Indian names
data = {
    "Name":["Amit", "Priya", "Vikram", "Neha", "Ravi"],
    "Age": [25, 30, 35, 40, 45],
    "City": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune"]
}

df = pd.DataFrame(data)
print(df)
```
Output:
![output](Images/8_2.png)

### Accessing data from DataFrames
```python
import pandas as pd

# Creating a DataFrame from a dictionary with Indian names
data = {
    "Name":["Amit", "Priya", "Vikram", "Neha", "Ravi"],
    "Age": [25, 30, 35, 40, 45],
    "City": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune"]
}

df = pd.DataFrame(data)
# print(df)

# Accessing a single column
print("Names: ",df["Name"])

# Accessing multiple columns
print(df[["Name", "Age"]])

# Accessing rows using index
print(df.iloc[0]) # First row

# Accessing multiple rows using index
print(df.iloc[:3])
```
`iloc` stands for "integer location".

We use `dataframe_name.iloc[index_number]` to access rows using index for DataFrames.

### Filtering Data
Using DataFrames it is easy to filter data.
```python
import pandas as pd

# Creating a DataFrame from a dictionary with Indian names
data = {
    "Name":["Amit", "Priya", "Vikram", "Neha", "Ravi"],
    "Age": [25, 30, 35, 40, 45],
    "City": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune"]
}

df = pd.DataFrame(data)
print(df)

# Filter rows where Age is greater than 30
filtered_df = df[df["Age"] > 30]
print(filtered_df)
```

### Adding a new column to the DataFrame
```python
import pandas as pd

# Creating a DataFrame from a dictionary with Indian names
data = {
    "Name":["Amit", "Priya", "Vikram", "Neha", "Ravi"],
    "Age": [25, 30, 35, 40, 45],
    "City": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune"]
}

df = pd.DataFrame(data)

# Adding a new column 'Salary'
df["Salary"] = [50000, 60000, 70000, 80000, 90000]
print(df)
```

### Sorting values in a DataFrame
```python
import pandas as pd

# Creating a DataFrame from a dictionary with Indian names
data = {
    "Name":["Amit", "Priya", "Vikram", "Neha", "Ravi"],
    "Age": [25, 30, 35, 40, 45],
    "City": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune"]
}

df = pd.DataFrame(data)

# Sorting by Age
sorted_df = df.sort_values(by="Age", ascending=False)
print(sorted_df)
```

### Renaming columns
```python
import pandas as pd

# Creating a DataFrame from a dictionary with Indian names
data = {
    "Name":["Amit", "Priya", "Vikram", "Neha", "Ravi"],
    "Age": [25, 30, 35, 40, 45],
    "City": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune"]
}

df = pd.DataFrame(data)

# Rename the 'Name' column to 'Full Name' and 'Age' to 'Years'
df_renamed = df.rename(columns={"Name": "Full Name", "Age": "Years"})
print(df_renamed)
```

### Dropping columns
```python
import pandas as pd

# Creating a DataFrame from a dictionary with Indian names
data = {
    "Name":["Amit", "Priya", "Vikram", "Neha", "Ravi"],
    "Age": [25, 30, 35, 40, 45],
    "City": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune"]
}

df = pd.DataFrame(data)

# Drop the 'City' column
df_dropped = df.drop(columns=["City"])
print(df_dropped)
```

### Dropping rows by index
```python
import pandas as pd

# Creating a DataFrame from a dictionary with Indian names
data = {
    "Name":["Amit", "Priya", "Vikram", "Neha", "Ravi"],
    "Age": [25, 30, 35, 40, 45],
    "City": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune"]
}

df = pd.DataFrame(data)

# Drop the row at index 2 (Vikram)
df_dropped_row = df.drop(index=2)
print(df_dropped_row)
```

### Giving meaning to the DataFrame 
__apply()__: The `apply` method is used to apply a function to each element of the "Age" column.
```python
import pandas as pd

# Creating a DataFrame from a dictionary with Indian names
data = {
    "Name":["Amit", "Priya", "Vikram", "Neha", "Ravi"],
    "Age": [25, 30, 35, 40, 45],
    "City": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune"]
}

df = pd.DataFrame(data)

# Create a new column 'Seniority' based on the Age
df['Seniority'] = df['Age'].apply(lambda x: 'Senior' if x >= 35 else 'Junior')
print(df)
```

### Grouping columns
```python
import pandas as pd

# Creating a DataFrame from a dictionary with Indian names
data = {
    "Name":["Amit", "Priya", "Vikram", "Neha", "Ravi"],
    "Age": [25, 30, 35, 40, 45],
    "City": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune"]
}

df = pd.DataFrame(data)

# Adding a new column 'Salary'
df['Salary'] = [50000, 60000, 70000, 80000, 90000]

# Group by 'City' and calculate the average Salary in each city
df_grouped = df.groupby("City")["Salary"].mean()
print(df_grouped)
```

Selects the "Salary" column from the grouped DataFrame.<br>
Calculates the mean (average) value of the "Salary" column for each group (city). 👇
```python
df_grouped = df.groupby("City")["Salary"].mean()
```

### Adding a particular value to columns
```python
import pandas as pd

# Creating a DataFrame from a dictionary with Indian names
data = {
    "Name":["Amit", "Priya", "Vikram", "Neha", "Ravi"],
    "Age": [25, 30, 35, 40, 45],
    "City": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune"]
}

df = pd.DataFrame(data)

# Adding a new column 'Salary'
df['Salary'] = [50000, 60000, 70000, 80000, 90000]

# Apply a custom function to the 'Salary' column to add a 10% bonus
def add_bonus(salary):
    return salary*1.10

df['Salary_with_Bonus'] = df['Salary'].apply(add_bonus)
print(df)
```

Uses the apply method to apply the add_bonus function to each element of the "Salary" column. 👇
```python
df['Salary_with_Bonus'] = df['Salary'].apply(add_bonus)
```

### Merging DataFrames
```python
import pandas as pd

# Creating a DataFrame from a dictionary with Indian names
data = {
    "Name":["Amit", "Priya", "Vikram", "Neha", "Ravi"],
    "Age": [25, 30, 35, 40, 45],
    "City": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune"]
}

df = pd.DataFrame(data)

# Adding a new column 'Salary'
df['Salary'] = [50000, 60000, 70000, 80000, 90000]

df_new = pd.DataFrame({
    "Name": ["Amit", "Priya", "Ravi"],
    "Bonus": [5000, 6000, 7000]
})

# Merge based on the 'Name' column - it's kind of a left join
df_merged = pd.merge(df, df_new, on="Name", how="left")
print(df_merged)
```
Output:
![Output](Images/8_3.png)

The table has null values because in the new DataFrame there are no values present for the `Name` : `Vikram` and `Neha`

### Concatenate DataFrames
When we need to concatenate the data shoud be exactly of the same structure.
```python
import pandas as pd

# Creating a DataFrame from a dictionary with Indian names
data = {
    "Name":["Amit", "Priya", "Vikram", "Neha", "Ravi"],
    "Age": [25, 30, 35, 40, 45],
    "City": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune"]
}

df = pd.DataFrame(data)

# Adding a new column 'Salary'
df['Salary'] = [50000, 60000, 70000, 80000, 90000]

df_new = pd.DataFrame({
    "Name": ["Sonia", "Rahul"],
    "Age": [29, 31],
    "City": ["Kolkata", "Hyderabad"],
    "Salary": [58000, 63000]
})

# Concatenate the two DataFrames
df_concat = pd.concat([df, df_new], ignore_index=True)
print(df_concat)
```

Using `ignore_index` we are saying to the compiler that ignore the index and give me the DataFrame perfectly.



