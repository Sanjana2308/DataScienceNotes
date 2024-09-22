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
SELECT COUNT(o.quantity) AS Quantity, p.Category
FROM Products p 
JOIN Orders o
ON o.product_id = p.product_id
GROUP BY p.Category
~~~

## Join Exercises<br>
1. Write a query to join the Customers and Orders tables to display customer names and their order details.
~~~sql
SELECT Name, o.*
FROM Customer c
JOIN Orders o
ON o.order_id = c.OrderId
~~~

2. Perform an inner join between Products and Orders to retrieve product names and quantities sold.
~~~sql
SELECT p.product_name, SUM(o.quantity)
FROM Products p
INNER JOIN Orders o
ON p.product_id = o.product_id
GROUP BY product_name
~~~

3. Use a left join to display all products, including those that have not been ordered.
~~~sql
SELECT * 
FROM Products p
LEFT JOIN Orders o
ON p.product_id = o.product_id
~~~

4. Write a query to join Employees with Departments and list employee names and their respective department names.
~~~sql
SELECT e.Name, d.DepartmentName 
FROM tblEmployee e
LEFT JOIN tblDepartment d
ON e.DepartmentId = d.ID
~~~

5. Perform a self-join on an Employees table to show pairs of employees who work in the same department.
~~~sql
SELECT e1.ID AS EmployeeID1, e1.Name AS EmployeeName1, e2.ID AS EmployeeID2, e2.Name AS EmployeeName2, e1.DepartmentID
FROM tblEmployee e1
JOIN tblEmployee e2
ON e1.DepartmentID = e2.DepartmentID AND e1.ID < e2.ID
ORDER BY e1.DepartmentID, e1.ID, e2.ID
~~~

`ON e1.DepartmentId = e2.DepartmentId:` The condition ensures that both employees (e1 and e2) are from the same department (i.e., they share the same DepartmentId).

`AND e1.ID < e2.ID`: This condition ensures that the pair of employees is listed in order such that the first employee (e1) has a smaller ID than the second employee (e2). This avoids pairing an employee with themselves and ensures each pair appears only once.

`ORDER BY e1.DepartmentId, e1.ID, e2.ID`: 
<br>This clause sorts the results by:

- `e1.DepartmentId`: All pairs from the same department are grouped together.
- `e1.ID`: Within each department, the results are sorted by the ID of the first employee in the pair.
- `e2.ID`: If multiple pairs have the same first employee, they are sorted by the ID of the second employee.

## Subquery Exercises
1. Write a query to find products whose price is higher than the average price of all products.
~~~sql
SELECT * 
FROM Products
WHERE price > (
	SELECT AVG(price)
	FROM Products
)
~~~

2. Retrieve customer names who have placed at least one order by using a subquery.
~~~sql
SELECT ID, Name 
FROM Customer
WHERE OrderId IN (
	SELECT OrderId
	FROM Orders
)
~~~

3. Find the top 3 most expensive products using a subquery.
~~~sql
SELECT *
FROM Products
WHERE price IN(
	SELECT TOP 3 price
	FROM Products
	ORDER BY price DESC
)
~~~

4. Write a query to list all employees whose salary is higher than the average salary of their department.
~~~sql
SELECT ID, Name, Gender, Salary
FROM tblEmployee e1
WHERE Salary >(
	SELECT AVG(Salary)
	FROM tblEmployee
	WHERE e1.DepartmentId = DepartmentId
)
~~~

5. Use a correlated subquery to find employees who earn more than the average salary of all employees in their department
~~~sql
SELECT ID, Name, Gender, Salary
FROM tblEmployee e1
WHERE Salary >(
	SELECT AVG(e2.Salary)
	FROM tblEmployee e2
	WHERE e1.DepartmentId = e2.DepartmentId
)
~~~

## Grouping and Summarizing Data Exercises<br>
1. Group orders by customer and calculate the total amount spent by each customer.
~~~sql
SELECT o.customer_id, c.Name, SUM(PurchaseAmt) 
FROM Customer c
JOIN Orders o
ON c.OrderId = o.order_id
GROUP BY o.customer_id, c.Name
~~~

2. Group products by category and calculate the average price for each category.
~~~sql
SELECT  Category, AVG(price) AS AvgPriceOfCategory
FROM Products
GROUP BY Category
~~~

3. Group orders by month and calculate the total sales for each month.
~~~sql
SELECT MONTH(o.order_date) AS Month,  SUM(p.price) AS TotalSales
FROM Orders o
JOIN Products p
ON o.product_id = p.product_id
GROUP BY MONTH(o.order_date)
~~~

4. Write a query to group products by category and calculate the number of products in each category.
~~~sql
SELECT Category, COUNT(product_name) AS NumberOfProducts
FROM Products
GROUP BY Category
~~~

5. Use the HAVING clause to filter groups of customers who have placed more than 5 orders.
~~~sql
SELECT customer_id, COUNT(order_id) AS total_orders
FROM orders
GROUP BY customer_id
HAVING COUNT(order_id) > 5;
~~~


## Set Operations (UNION, INTERSECT, EXCEPT)<br>
1. Write a query to combine the results of two queries that return the names of customers from different tables using UNION.
~~~sql
SELECT Name
FROM Customer
UNION
SELECT Name
FROM Customer
JOIN Orders
ON Customer.OrderId = Orders.order_id
~~~

2. Find products that are in both the Electronics and Accessories categories using INTERSECT.
~~~sql
SELECT product_name
FROM Products
WHERE Category LIKE 'Electronics' INTERSECT
SELECT product_name
FROM Products
WHERE Category LIKE 'Accessories'
~~~

3. Write a query to find products that are in the Electronics category but not in the Furniture category using EXCEPT.
~~~sql
SELECT product_name
FROM Products
WHERE Category LIKE 'Electronics' 
EXCEPT
SELECT product_name
FROM Products
WHERE Category LIKE 'Furniture'
~~~
