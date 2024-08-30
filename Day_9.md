# Day 9

## Hands-on : DataFrames

### **Exercise 5: Handling Missing Values**
1. Create a DataFrame with missing values:
   ```python
   data = {
       "Name": ["Amit", "Neha", "Raj", "Priya"],
       "Age": [28, None, 35, 29],
       "City": ["Delhi", "Mumbai", None, "Chennai"]
   }
   df = pd.DataFrame(data)
   ```
2. Fill missing values in the `"Age"` column with the average age.
3. Drop rows where any column has missing data.
```python
data = {
    "Name": ["Amit", "Neha", "Raj", "Priya"],
    "Age": [28, None, 35, 29],
    "City": ["Delhi", "Mumbai", None, "Chennai"]
}
df = pd.DataFrame(data)

average_age = df['Age'].mean()
df['Age'] = df['Age'].fillna(average_age)

df_dropped = df.dropna()

print(df_dropped)
```

### **Exercise 6: Adding and Removing Columns**
1. Add a new column `"Salary"` with the following values: `[50000, 60000, 70000, 65000]`.
2. Remove the `"City"` column from the DataFrame.
```python
df['Salary'] = [50000, 60000, 70000, 65000]

df_dropped_2 = df.drop(columns=["City"])

print(df)
print(df_dropped_2)
```

### **Exercise 7: Sorting Data**
1. Sort the DataFrame by `"Age"` in ascending order.
2. Sort the DataFrame first by `"City"` and then by `"Age"` in descending order.
```python
df_sorted = df.sort_values(by='Age', ascending=True)
print(df_sorted)

df_sorted_city = df.sort_values(by='City', ascending=True)
df_sorted_city_age = df.sort_values(by='Age', ascending=False)
print(df_sorted_city_age)
```

### **Exercise 8: Grouping and Aggregation**
1. Group the DataFrame by `"City"` and calculate the average `"Age"` for each city.
2. Group the DataFrame by `"City"` and `"Age"`, and count the number of occurrences for each group.
```python
df_grouped_1 = df.groupby("City")["Age"].mean()
print(df_grouped_1)

df_grouped_2 = df.groupby(["City", "Age"]).size().reset_index(name='Count')
print(df_grouped_2)
```

### **Exercise 9: Merging DataFrames**
1. Create two DataFrames:A
   ```python
   df1 = pd.DataFrame({
       "Name": ["Amit", "Neha", "Raj"],
       "Department": ["HR", "IT", "Finance"]
   })

   df2 = pd.DataFrame({
       "Name": ["Neha", "Raj", "Priya"],
       "Salary": [60000, 70000, 65000]
   })
   ```
2. Merge `df1` and `df2` on the `"Name"` column using an inner join.
3. Merge the same DataFrames using a left join.
```python
df1 = pd.DataFrame({
       "Name": ["Amit", "Neha", "Raj"],
       "Department": ["HR", "IT", "Finance"]
   })

df2 = pd.DataFrame({
       "Name": ["Neha", "Raj", "Priya"],
       "Salary": [60000, 70000, 65000]
})

merged_df_inner = pd.merge(df1, df2, on="Name", how="inner")
merged_df_left = pd.merge(df1, df2, on="Name", how="left")

print("Inner Join:\n",merged_df_inner)
print("\nLeft Join:",merged_df_left)
```

## Modules in Python
1. 
FileName: `mymodule.py`
```python
def greet(name):
    return f"Hello, {name}!"
```

FileName: `main.py`
```python
import mymodule

print(mymodule.greet("Sanjana"))
```

2. 
FileName: `math_operations.py`
```python
def add(a, b):
    return a + b

def subtract(a, b):
    return a-b

def multiply(a, b):
    return a*b

def divide(a, b):
    return a/b
```

FileName: `main.py`
```python
import math_operations

a = 20
b = 10

print("Add: ",math_operations.add(a, b))
print("Subtract: ",math_operations.subtract(a, b))
print("Multiply: ",math_operations.multiply(a, b))
print("Division: ",math_operations.divide(a, b))
```
3. 
![alt text](Images/9_1.png)
Filename: `string_utils.py`
```python
def capitalize_words(str):
    words = str.split()
    capitalize_words = [word.capitalize() for word in words]
    return " ".join(capitalize_words)

def reverse_string(str):
    return str[::-1]

def count_vowels(str):
    vowels = "aeiouAEIOU"
    count = 0

    for char in str:
        if char in vowels:
            count+=1

    return count

def is_palindrome(str):
    cleaned_str = ''.join(c.lower() for c in str if c.isalnum())

    reversed_str = cleaned_str[::-1]

    if cleaned_str == reversed_str:
        return True
    else:
        return False
```

Filename: `main.py`
```python
import string_utils
from string_utils import capitalize_words

capitalize = string_utils.capitalize_words("hello, I am Sanjana")
print("Capitalized words string: ",capitalize_words)

reverse = string_utils.reverse_string("Hello I am Somesh")
print("Reversed String",reverse)

count = string_utils.count_vowels("Count vowels, please")
print("Number of vowels: ",count)

palindrome = string_utils.is_palindrome("Malayalam")
print("The string is palindrome?: ",palindrome)
```
