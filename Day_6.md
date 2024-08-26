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




