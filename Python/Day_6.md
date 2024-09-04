# Day 6

## PYTHON

![alt text](Images/6_1.png)

![alt text](Images/6_2.png)

![Syntax Rules](Images/6_3.png)

![alt text](Images/6_4.png)

### First Program
```python
print("Hello World")
```

### Printing String
1. Single quote and double quote are the same.
```python
single_quote_str = 'This is single quote String'
double_quote_str = "This is a double quote string"

print(single_quote_str)
print(double_quote_str)
```
2. We can use "double quote " inside single quotes to print the output.
```python
single_quote_str = 'This is single quote String - "Hey"'
double_quote_str = "This is a double quote string"

print(single_quote_str)
print(double_quote_str)
```
3. Double quotes inside double quotes is an error because now we are confusing python. 
```python
single_quote_str = "This is single quote String - "Hey""
double_quote_str = "This is a double quote string"

print(single_quote_str)
print(double_quote_str)
```

4. Multi-line String
```python
next_string = '''This is a 
Multiple line string
'''
print(next_string)
```

### Appending Strings
1. Using Concatenation
```python
greeting = "Hello"
name = "Alice"
full_greeting = greeting+", "+name+"!"
print(full_greeting)
```

2. Using format()
```python

greeting = "Hello"
name = "Alice"
formatted_greeting = "{}, {}".format(greeting, name)
print(formatted_greeting)
```

3. Using f-Strings
```python
formatted_greeting_f = f"{greeting}, {name}!"
print(formatted_greeting_f)
```

### Operations on String

1. __strip()__: Eliminates the space arond text
```python
text = "  Python Programming  "
stripped_text = text.strip()
print(stripped_text)
```

2. __upper()__: Converts text to uppercase
```python
uppercase_text = text.upper()
print(uppercase_text)
```

3. __startswith()__: Checks whether the string text starts with the subtring text.
```python
starts_with_python = text.startswith("Python")
print(starts_with_python)
```
States false because there is some space in text

After removing spaces using strip()
```python
formatted_text = text.strip()
starts_with_python = formatted_text.startswith("Python")
print(starts_with_python)
```
4. __replace()__: Replaces the given substring with mentioned substring in the main string text
```python
replaced_text = text.replace("Programming", "Coding")
print(replaced_text)
```

### Integers
Can be positive, negative or neutral
```python
positive_int = 42
negative_int = -42
zero = 0

print(positive_int)
print(negative_int)
print(zero)
```

### Operations on Numbers
```python
a = 10
b = 3

# Addition
addition = a+b

# Subtraction
subtraction = a-b

# Multiplication
multiplication = a*b

# Division
division = a/b

# Floor Division
floor_division = a//b

# Modulus
modulus = a%b

# Exponentiation
exponentiation = a**b

print("Addition: ",addition)
print("Subtraction: ",subtraction)
print("Multiplication: ", multiplication)
print("Division: ",division)
print("Floor Division: ",floor_division)
print("Modulus: ", modulus)
print("Exponentiation: ",exponentiation)
```

#### Type casting
1. String to integer using __int()__
```python
num_str = "100"
num_int = int(num_str)

print("String to Integer: ", num_int)
```
2. Convert float to integer using __int()__
```python
num_float = 12.34
num_int_from_float = int(num_float)

print("Float to Integer: ", num_int_from_float)
```

3. Converting different data types to boolean
```python
# Convert integer to boolean
bool_from_int = bool(1) # True

# Convert zero to boolean
bool_from_zero = bool(0) # False

# Convert string to boolean
bool_from_str = bool("Hello") # True

# Convert empty string to boolean
bool_from_empty_str = bool("") # False

print("Boolean from integer 1: ",bool_from_int)
print("Boolean from integer 0: ",bool_from_zero)
print("Boolean from non-empty string: ",bool_from_str)
print("Boolean from empty string: ",bool_from_empty_str)
```

### Comparison Operator
```python
x = 10
y = 5
is_greater = x > y # True
is_equal = x == y # False
print("x >y",is_greater)
print("x == y", is_equal)
```

### Logical Operator
```python
a = True
b = False
#Logical AND
and_operation = a and b # a * b -- True * False -- 1 * 0 - False

# Logical OR
or_operation = a or b # True + False -- True -- 1

# Logical NOT
not_operation = not a # False

print("a AND b: ",and_operation)
print("a OR b: ",or_operation)
print("NOT a: ",not_operation)
```

## Data Structures

### Lists
1. It is a collection of elements.
2. We can have same or different datatypes in a list
```python
empty_list = []
numbers = [1, 2, 3, 4, 5]
mixed_list = [1, 'Hello', 3.14, True]

print(empty_list)
print(numbers)
print(mixed_list)
```

#### Accessing Elements in a List
```python
numbers = [1, 2, 3, 4, 5]

first_element = numbers[0]
third_element = numbers[2]
last_element = numbers[-1]

print("First Element: ", first_element)
print("Third Element: ",third_element)
print("Last Element: ",last_element)
```

#### Modifying Elements in a List
```python
numbers = [1, 2, 3, 4, 5]

numbers[0] = 10
numbers[2] = 30

print(numbers)
```

#### Adding elements in the list
```python
numbers = [1, 2, 3, 4, 5]

# Add elements at the end of list
numbers.append(6)

# Insert elements in the list
numbers.insert(2, 2.5)

# Adding multiple elements at the end of list
numbers.extend([7, 8, 9])

print(numbers) # [1, 2, 2.5, 3, 4, 5, 6, 7, 8, 9]
```

#### Removing element from list
```python
numbers = [1, 2, 3, 4, 5]

numbers.remove(3) # by element

popped_element = numbers.pop(2) # by index

print(numbers) # [1, 2, 4, 5]
```

#### Slicing elements 
```python
numbers = [1, 2, 3, 4, 5]

# Slicing a list
first_three = numbers[:3]
middle_two = numbers[1:3]
last_two = numbers[-2:]

print("First three elements: ",first_three) # [1, 2, 3]
print("Middle two elements",middle_two) # [2, 3]
print("Last two elements: ",last_two) # [4, 5]
```

#### Printing elements of a list 
```python
numbers = [1, 2, 3, 4, 5]

# Iterating over a list
for num in numbers:
    print(num) # [1, 2, 3, 4, 5]
```

#### List Comprehension
```python
numbers = [1, 2, 3, 4, 5]

# List Comprehension
# Creating a list of squares
squares = [x**2 for x in range(6)]
print(squares) # [0, 1, 4, 9, 16, 25]
```

## Hands-on Exercises - Lists
1. Create a List
Create a list called fruits with the following items: "apple", "banana", "cherry", "date", and "elderberry".
Print the list.
```python
fruits = ["apple", "banana", "cherry", "date", "elderberry"]
print(fruits)
```

2. Access List Elements
Print the first and last items from the fruits list.
Print the second and fourth items from the list.
```python
first_element = fruits[0]
last_element = fruits[-1]
second_element = fruits[1]
fourth_element = fruits[3]
print("First Element: ",first_element)
print("Last Element: ",last_element)
print("Second Element: ",second_element)
print("Fourth Element: ",fourth_element)
```

3. Modify a List
Replace "banana" in the fruits list with "blueberry".
Print the modified list.
```python
fruits[1] = "blueberry"
print(fruits)
```

4. Add and Remove Elements
Append "fig" and "grape" to the fruits list.
Remove "apple" from the list.
Print the final list.
```python
fruits.extend(["fig", "grape"])
fruits.remove("apple")

print(fruits)
```

5. Slice a List
Slice the first three elements from the fruits list and assign them to a new list called first_three_fruits.
Print first_three_fruits.
```python
first_three_fruits = fruits[:3]
print(first_three_fruits)
```

6. Find List Length
Find and print the length of the fruits list.
```python
length_of_list = len(fruits)
print(length_of_list)
```

7. List Concatenation
Create a second list called vegetables with the following items: "carrot", "broccoli", "spinach".
Concatenate the fruits and vegetables lists into a new list called food.
Print the food list.
```python
vegetables = ["carrot", "broccoli", "spinach"]
food = fruits + vegetables
print(food)
```

8. Loop Through a List
Loop through the fruits list and print each item on a new line.
```python
for fruit in fruits:
    print(fruit)
```

9. Check for Membership
Check if "cherry" and "mango" are in the fruits list. Print a message for each check.
```python
if "cherry" in fruits:
    print("Cherry is present")
else:
    print("Cherry is not present")

if " mango" in fruits:
    print("Mango is present")
else:
    print("Mango is not present")
```

10. List Comprehension
Use list comprehension to create a new list called fruit_lengths that contains the lengths of each item in the fruits list.
Print the fruit_lengths list.
```python
fruit_lengths = [len(fruit) for fruit in fruits]
print(fruit_lengths)
```

11. Sort a List
Sort the fruits list in alphabetical order and print it.
Sort the fruits list in reverse alphabetical order and print it.
```python
fruits.sort()
print("Fruits in alphabetical order: ",fruits)
fruits.sort(reverse=True)
print("Fruits in reverse alphabetical order: ",fruits)
```

12. Nested Lists
Create a list called nested_list that contains two lists: one with the first three fruits and one with the last three fruits.
Access the first element of the second list inside nested_list and print it.
```python
nested_list = [fruits[:3], [fruits[-3:]]]
print(nested_list)
```

13. Remove Duplicates
Create a list called numbers with the following elements: [1, 2, 2, 3, 4, 4, 4, 5].
Remove the duplicates from the list and print the list of unique numbers.
```python
numbers = [1, 2, 2, 3, 4, 4, 4, 5]
print("Original list: ",numbers)
result = []
[result.append(n) for n in numbers if n not in result]
print(result)
```

14. Split and Join Strings
Split the string "hello, world, python, programming" into a list called words using the comma as a delimiter.
Join the words list back into a string using a space as the separator and print it.
```python
str = "hello, world, python, programming"
# Splitting the string to a list
words = str.split(",")
print("Splitting the string to a list: ",words)

joined_words = " ".join(words)
print("Joining the list to a string: ", joined_words)
```

### Dictionaries
Consists of key-value pairs.
#### Creating a dictionary
```python
# Creating dictionaries
empty_dict = {}
person = {
    "name": "Mark",
    "age": 30,
    "email": "mark@example.com"
}
print(empty_dict)
print(person)
```

#### Accessing values of a dictionary
```python
empty_dict = {}
person = {
    "name": "Mark",
    "age": 30,
    "email": "mark@example.com"
}

# Accessing Values
name = person["name"]
age = person["age"]

print("Name: ", name)
print("Age: ",age)
```

#### Modifying values of a dictionary
```python
empty_dict = {}
person = {
    "name": "Mark",
    "age": 30,
    "email": "mark@example.com"
}

# Modifying Values
person["age"] = 31
person["email"] = "mark_new@example.com"

print(person)
```

#### Adding a new key-value pair
```python
empty_dict = {}
person = {
    "name": "Mark",
    "age": 30,
    "email": "mark@example.com"
}

# Adding a key value pair
person["address"] = "123, Main Street"

# Removing a key-value pair
del person["email"]

print(person)
```

#### Using dictionary methods
```python
empty_dict = {}
person = {
    "name": "Mark",
    "age": 30,
    "email": "mark@example.com"
}

# Using dictionary methods
keys = person.keys()
values = person.values()
items = person.items()

print("Keys: ",keys)
print("Values: ",values)
print("Items: ",items)
```

#### Iterating over values or keys
```python
empty_dict = {}
person = {
    "name": "Mark",
    "age": 30,
    "email": "mark@example.com"
}

# Iterating over keys
for key in person:
    print(key)

# Iterating over values
for value in person.values():
    print(value)

# Iterating over key-value pairs
for key, value in person.items():
    print(key, " :",value)
```


