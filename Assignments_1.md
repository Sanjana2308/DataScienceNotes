## Date Function Exercises<br>
1. Calculate the number of months between your birthday and the current date.
~~~sql
SELECT DATEDIFF(MONTH, '2002-08-23', GETDATE())
~~~

2. Retrieve all orders that were placed in the last 30 days.
~~~sql
SELECT * 
from Orders
WHERE order_date BETWEEN DATEADD(DAY, -30, GETDATE()) AND GETDATE()
~~~

3. Write a query to extract the year, month, and day from the current date.
~~~sql
SELECT YEAR(GETDATE()) AS Year,
MONTH(GETDATE()) AS Month,
DAY(GETDATE()) AS Day
~~~

4. Calculate the difference in years between two given dates.
~~~sql
SELECT DATEDIFF(YEAR, '1896-07-06', '1990-05-02')
~~~

5. Retrieve the last day of the month for a given date.
~~~sql
SELECT DAY(EOMONTH('1942-02-08')) AS LastDayOfMonth
~~~


## String Function Exercises<br>
1. Convert all customer names to uppercase.
~~~sql
SELECT UPPER(Name) AS UpperCaseNames
from Customer
~~~

2. Extract the first 5 characters of each product name.
~~~sql
SELECT LEFT(product_name, 5) AS FirstFiveCharacters
FROM Products 
~~~

3. Concatenate the product name and category with a hyphen in between.
~~~sql
SELECT CONCAT(product_name, '-', Category) AS ConcatenatedProductName
FROM Products 
~~~

4. Replace the word 'Phone' with 'Device' in all product names.
~~~sql
SELECT REPLACE(product_name,'Phone', 'Device') AS UpdatedProductName
FROM Products
~~~

5. Find the position of the letter 'a' in customer names.
~~~sql
SELECT CHARINDEX('a', Name) AS PositionOfLetterA
FROM Customer
~~~

## Aggregate Function Exercises<br>
1. Calculate the total sales amount for all orders.
~~~sql
SELECT SUM(p.price) AS TotalSalesAmount
FROM Orders o
JOIN Products p
ON o.product_id = p.product_id
~~~

2. Find the average price of products in each category.
~~~sql
SELECT AVG(price) AS AveragePrice
FROM Products
GROUP BY Category
~~~

3. Count the number of orders placed in each month of the year.
~~~sql
SELECT MONTH(order_date) AS OrderMonth, COUNT(*) AS ProductCount
FROM Orders
GROUP BY MONTH(order_date)
~~~

4. Find the maximum and minimum order quantities.
~~~sql
SELECT MAX(quantity) AS MaximumQuantity, MIN(quantity) AS MinimumQuantity
FROM Orders
~~~

5. Calculate the sum of stock quantities grouped by product category.
~~~sql

~~~

## Join Exercises<br>
Write a query to join the Customers and Orders tables to display customer names and their order details.<br>
Perform an inner join between Products and Orders to retrieve product names and quantities sold.<br>
Use a left join to display all products, including those that have not been ordered.<br>
Write a query to join Employees with Departments and list employee names and their respective department names.<br>
Perform a self-join on an Employees table to show pairs of employees who work in the same department.<br>


## Subquery Exercises
Write a query to find products whose price is higher than the average price of all products.<br>
Retrieve customer names who have placed at least one order by using a subquery.<br>
Find the top 3 most expensive products using a subquery.<br>
Write a query to list all employees whose salary is higher than the average salary of their department.<br>
Use a correlated subquery to find employees who earn more than the average salary of all employees in their department.<br>


## Grouping and Summarizing Data Exercises<br>
Group orders by customer and calculate the total amount spent by each customer.<br>
Group products by category and calculate the average price for each category.<br>
Group orders by month and calculate the total sales for each month.<br>
Write a query to group products by category and calculate the number of products in each category.<br>
Use the HAVING clause to filter groups of customers who have placed more than 5 orders.<br>


## Set Operations (UNION, INTERSECT, EXCEPT)<br>
Write a query to combine the results of two queries that return the names of customers from different tables using UNION.<br>
Find products that are in both the Electronics and Accessories categories using INTERSECT.<br>
Write a query to find products that are in the Electronics category but not in the Furniture category using EXCEPT.<br>