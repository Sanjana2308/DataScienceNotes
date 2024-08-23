# Day 2

## Joins
![alt text](Images/2_1.png)
Create these two tables for further queries
1. Create and use DB
~~~sql
create database CompanyDB

use CompanyDB
~~~

2. Create tables
~~~sql
CREATE TABLE tblEmployee
(ID int Primary Key,
Name nVarchar(100) not null,
Gender nVarchar(50),
Salary int not null,
DepartmentId int)

CREATE TABLE tblDepartment
(ID int Primary Key,
DepartmentName nVarchar(100) not null,
Location nVarchar(50) not null,
DepartmentHead nvarchar(100) not null)
~~~

3. Inserting values to tables
~~~sql
INSERT INTO tblEmployee(ID, Name, Gender, Salary, DepartmentId)
VALUES
(1, 'Tom', 'Male', 4000, 1),
(2, 'Pam' , 'Female', 3000, 3),
(3, 'John', 'Male', 3500, 1),
(4, 'Sam', 'Male', 4500, 2),
(5, 'Todd', 'Male', 2800, 2),
(6, 'Ben', 'Male', 7000, 1),
(7, 'Sara', 'Female', 4800, 3),
(8, 'Valarie', 'Female', 5500, 1),
(9, 'James', 'Male', 6500, null),
(10, 'Russell', 'Male', 8800, null)

INSERT INTO tblDepartment(ID, DepartmentName, Location, DepartmentHead)
VALUES
(1, 'IT', 'London', 'Rick'),
(2, 'Payroll', 'Delhi', 'Ron'),
(3, 'HR', 'New York', 'Christie'),
(4, 'Other Department', 'Sydney', 'Cindrella')
~~~

4. Display values of tables
~~~sql
select * from tblEmployee

select * from tblDepartment
~~~

### Inner Join
Selecting common values from both tables
Only when both records match it gives the entry as output
~~~sql
select Name, Gender, Salary, DepartmentName
from tblEmployee
INNER JOIN tblDepartment
ON tblEmployee.DepartmentId = tblDepartment.ID
~~~

### Left Outer Join OR Left Join
Returns the entries even if there is a null value 
~~~sql
select Name, Gender, Salary, DepartmentName
from tblEmployee
LEFT OUTER JOIN tblDepartment
ON tblEmployee.DepartmentId = tblDepartment.ID
~~~

### Right Outer Join
~~~sql
select Name, Gender, Salary, DepartmentName
from tblEmployee
RIGHT OUTER JOIN tblDepartment
ON tblEmployee.DepartmentId = tblDepartment.ID
~~~

### Full Outer Join
Give me matching and unmatching from left and right side 
~~~sql
select Name, Gender, Salary, DepartmentName
from tblEmployee
FULL OUTER JOIN tblDepartment
ON tblEmployee.DepartmentId = tblDepartment.ID
~~~

### Practice on Joins
Create tables with following data and perfrom all join queries<br><br>
![alt text](Images/2_2.png)

1. Create Tables
~~~sql
CREATE TABLE Products
(product_id int Primary Key,
product_name nvarchar(50) not null,
price DECIMAL(6, 2))

CREATE TABLE Orders
(order_id int Primary Key,
product_id int,
quantity int,
order_date DATE)
~~~

2. Inserting values to tables
~~~sql
INSERT INTO Products(product_id, product_name, price)
VALUES
(1, 'Laptop', 800.00),
(2, 'Smartphone', 500.00),
(3, 'Tablet', 300.00),
(4, 'Headphones', 50.00),
(5, 'Monitor', 150.00)

INSERT INTO Orders(order_id, product_id, quantity, order_date)
VALUES
(1, 1, 2, '2024-08-01'),
(2, 2, 1, '2024-08-02'),
(3, 3, 3, '2024-08-03'),
(4, 1, 1, '2024-08-04'),
(5, 4, 4, '2024-08-05'),
(6, 5, 2, '2024-08-06'),
(7, 6, 1, '2024-08-07')
~~~

3.1. INNER JOIN
~~~sql
select product_name, price, quantity, order_date
from Orders
INNER JOIN Products
ON Orders.product_id = Products.product_id
~~~

3.2. LEFT OUTER JOIN
~~~sql
select product_name, price, quantity, order_date
from Orders
LEFT OUTER JOIN Products
ON Orders.product_id = Products.product_id
~~~

3.3. RIGHT OUTER JOIN
~~~sql
select product_name, price, quantity, order_date
from Orders
RIGHT OUTER JOIN Products
ON Orders.product_id = Products.product_id
~~~

3.4. FULL OUTER JOIN
~~~sql
select product_name, price, quantity, order_date
from Orders
FULL OUTER JOIN Products
ON Orders.product_id = Products.product_id
~~~

### Group by and Grouping sets
Group by used for grouping single column <br>
Grouping Sets is used for grouping multiple columns 
Example - 1
~~~sql
SELECT p.product_name, o.order_date, SUM(o.quantity) AS total_quantity
FROM Orders o
JOIN Products p ON o.product_id = p.product_id
GROUP BY GROUPING SETS ((p.product_name), (o.order_date))
~~~

### Subqueries and some clauses
1. __SubQuery in Select__<br>
Example - 1
~~~sql
SELECT o.order_id, o.product_id,
	(SELECT p.product_name FROM Products p WHERE p.product_id = o.product_id) AS product_name
FROM Orders o
~~~

2. __Sub Query in where clause__<br>
Example - 2: Orders which have price > 500
~~~sql
SELECT order_id, order_date, product_id
FROM orders
WHERE product_id IN (select product_id FROM Products WHERE price > 500)
~~~

3. __EXISTS__<br>
Example - 3: Check there is at least one user who has placed at least one order<br>
Find at least one record in order table that has at least one user id record in it
<br>
EXISTS gives at least one record

~~~sql
SELECT u.user_id, u.user_name
from Users u
WHERE EXISTS(SELECT 1 FROM Orders o WHERE o.user_id = u.user_id)
~~~

4. __ANY__<br>
Example - 4: Find the products from the table where the price of the product is greater than any of the Laptops

~~~sql
SELECT p.product_name, p.price
FROM products p
WHERE p.price > ANY (SELECT price from Products where product_name LIKE 'Laptop%')
~~~

Example 5: Give me all products which has price greater than any of the smartphone
~~~sql
SELECT p.product_name, p.price
FROM products p
WHERE p.price > ALL (SELECT price from Products where product_name LIKE 'Smartphone%')
~~~

5. __Nested Sub Query__<br> 
Example 6: Query to retrive users who have ordered products priced above 1000. The query uses two levels of nested subqueries.

~~~sql
SELECT user_id, user_name
FROM Users
WHERE user_id IN(
	SELECT user_id
	FROM Orders
	WHERE product_id IN(
		SELECT product_id 
		FROM Products
		WHERE price > 1000
	)
)
~~~

### SET Operations
1. __UNION__<br>
~~~sql
SELECT product_name FROM Products Where price > 500
UNION
SELECT product_name FROM Products WHERE product_name LIKE 'Smart%'
~~~

2. __INTERSECT__<br>
~~~sql
SELECT product_name FROM Products Where price > 500
INTERSECT
SELECT product_name FROM Products WHERE product_name LIKE 'Smart%'
~~~

3. __EXCEPT__<br>
~~~sql
SELECT product_name FROM Products WHERE price > 500
EXCEPT
SELECT product_name FROM Products WHERE product_name LIKE 'Smart%'
~~~

## Activity
Creating Tables and inserting values
~~~sql
CREATE TABLE Employees (
    employee_id INT PRIMARY KEY,
    employee_name VARCHAR(255),
    department VARCHAR(255),
    manager_id INT
);

CREATE TABLE Salaries (
    salary_id INT PRIMARY KEY,
    employee_id INT,
    salary DECIMAL(10, 2),
    salary_date DATE,
    FOREIGN KEY (employee_id) REFERENCES Employees(employee_id)
);


INSERT INTO Employees (employee_id, employee_name, department, manager_id) VALUES
(1, 'John Doe', 'HR', NULL),
(2, 'Jane Smith', 'Finance', 1),
(3, 'Robert Brown', 'Finance', 1),
(4, 'Emily Davis', 'Engineering', 2),
(5, 'Michael Johnson', 'Engineering', 2);

INSERT INTO Salaries (salary_id, employee_id, salary, salary_date) VALUES
(1, 1, 5000, '2024-01-01'),
(2, 2, 6000, '2024-01-15'),
(3, 3, 5500, '2024-02-01'),
(4, 4, 7000, '2024-02-15'),
(5, 5, 7500, '2024-03-01');
~~~


•  Using an Equi Join:
Write a query to list all employees and their salaries using an equi join between the Employees and Salaries tables.
--Code--
select e.employee_name, s.salary
from Employees e, Salaries s
where e.employee_id = s.employee_id
----

•  Using a Self Join:
Write a query to list each employee and their manager's name using a self join on the Employees table.
--Code--
select e1.employee_name AS Employee_name, e2.employee_name AS Manager_name
FROM Employees as e1
LEFT JOIN Employees as e2
ON e2.employee_id = e1.manager_id
----

•  Using GROUP BY with HAVING:
Write a query to calculate the average salary by department. Use GROUP BY and filter out departments where the average salary is below 6000.
--Code--
select e.department, AVG(s.salary) AS average_salary
FROM Employees e
JOIN Salaries s
ON e.employee_id = s.employee_id
GROUP BY e.department
HAVING AVG(s.salary) >= 6000
----

•  Using GROUP BY with Grouping Sets:
Write a query using grouping sets to calculate the total salary by department and the overall total salary.
--Code--
SELECT e.department, SUM(s.salary) AS total_salary
FROM Employees e
JOIN Salaries s
ON e.employee_id = s.employee_id
GROUP BY GROUPING SETS((e.department), ())
----

•  Querying Data by Using Subqueries:
Write a query to list all employees whose salary is above the average salary using a subquery


•  Using the EXISTS Keyword:
Write a query to list all employees who have received a salary in 2024 using the EXISTS keyword.
•  Using the ANY Keyword:
Write a query to find employees whose salary is greater than the salary of any employee in the Engineering department.
•  Using the ALL Keyword:
Write a query to find employees whose salary is greater than the salary of all employees in the Finance department.
•  Using Nested Subqueries:
Write a query to list employees who earn more than the average salary of employees in the HR department using nested subqueries.
•  Using Correlated Subqueries:
Write a query to find employees whose salary is above the average salary for their respective department using a correlated subquery.
•  Using UNION:
Write a query to list all employee names from the HR and Finance departments using UNION.
•  Using INTERSECT:
Write a query to list employees who have worked in both Finance and Engineering using INTERSECT.
•  Using EXCEPT:
Write a query to list employees who are in Finance but not in HR using EXCEPT.
•  Using MERGE:
Write a query using MERGE to update employee salaries based on a new table of salary revisions. If the employee exists, update their salary; if not, insert the new employee and salary.