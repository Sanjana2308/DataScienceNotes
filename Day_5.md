# Day 5

## Cleaning of Data And Filtering it
1. If data is taking a particular direction then data is bad data.

2. The Machine Learning model is trained because of the data we feed into it like product pairing or recommendations in Amazon and this happens because of machine learning models 

3. Logos are designed with red colour because it is attractive.

4. Instagram reels offer infinitive scrolling
Pulling is used because of casino.

5. We deal with data, data manipulation we need to remove spaces and remove redundant data to get good data .

#### Data driven decisions 
1. A machine learning model gets confused because of uppercase letters and this is called noise.
2. Thus we clean up data firstly by converting the data to lowercase removing spaces and some words;
Is, a, the, are, adverbs are also noise words.  

### Data Cleaning
1. Converting Uppercase to Lowercase to reduce noise
~~~sql
UPDATE Customers
SET FirstName = LTRIM(RTRIM(LOWER(FirstName))),
	LastName = LTRIM(RTRIM(LOWER(LastName)));
~~~

2. Filtering Data 
~~~sql
SELECT * 
FROM Customers
WHERE FirstName LIKE 'A%';
~~~

3. Checking proper format of data
~~~sql
SELECT * 
FROM Customers
WHERE PhoneNumber LIKE '[0-9][0-9][0-9]-[0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'
~~~

4. Filtering data based on number of characters
~~~sql
SELECT * 
FROM Customers 
WHERE LastName LIKE '_____'
~~~

### Partitioning
Partition is done here to see who has spent the most money
~~~sql
SELECT CustomerID, TotalAmount, SUM(TotalAmount) OVER (PARTITION BY CustomerID ORDERBY OrderDate) AS RunningTotal
FROM Orders;
~~~
PARTITION shows running data like given below

|CustomerID|OrderID|MoneySpent|TotalMoneySpent|
|-----------|------|---------|----------------|
|1|1|400.00|400.00|
|1|2|500.00|900.00|
|1|3|200.00|1100.00|
|2|4|500.00|500.00|
|2|5|700.00|1200.00|

### Ranking
RANK(): Gives each row a rank according to specific ordering. 
~~~sql
SELECT CustomerID, TotalSales, RANK() OVER (ORDER BY TotalSales DESC) AS SalesRank
FROM(
	SELECT CustomerID, SUM(TotalAmount) AS TotalSales
	FROM Orders
	GROUP BY CustomerID
) AS SalesData
~~~

### CTE: Common Table Expression
__CTE__ is a temporary result set that can be used in a query, and is similar to a derived table.<br>
Defined using __WITH__ keyword.<br>
__CTE__ helps to keep your code organized and allows us to reuse the results in the same query, and perform multi-level aggregations
~~~sql
WITH RecursiveEmployeeCTE AS (
	SELECT EmployeeID, ManagerID, EmployeeName
	FROM Employees
	WHERE ManagerID IS NULL
	UNION ALL
	SELECT e.EmployeeID, e.ManagerID, e.EmployeeName
	FROM Employees e
	INNER JOIN RecursiveEmployeeCTE r ON e.ManagerID = r.EmployeeID
)

SELECT * FROM RecursiveEmployeeCTE;
~~~
Anchor member is the member here who has no manager that is manager_id is NULL 👇
~~~sql
SELECT EmployeeID, ManagerID, EmployeeName
	FROM Employees
	WHERE ManagerID IS NULL
~~~

People who are reporting to the this top level guy
Then ppl reporting to the ppl on upper post in a recursive manner.
Here basically we are trying to get who is Reporting to whom 👇
~~~sql
SELECT e.EmployeeID, e.ManagerID, e.EmployeeName
	FROM Employees e
	INNER JOIN RecursiveEmployeeCTE r ON e.ManagerID = r.EmployeeID
~~~

We are joining the table by itself using UNION ALL it leads to recursion 
The below code is deciding if the loop should continue or not.
~~~sql
RecursiveEmployeeCTE r ON e.ManagerID = r.EmployeeID
~~~

### ROLLUP()
~~~sql
SELECT Category, SUM(Amount) AS TotalSales
FROM Sales
GROUP BY ROLLUP(Category);
~~~
Enables a SELECT statement to calculate multiple levels of subtotals across a specified group of dimensions.

Explanation
~~~sql
SELECT Category, SUM(Amount) AS TotalSales
FROM Sales
GROUP BY Category;
~~~
Output:
|Category|TotalSales|
|--------|----------|
|Electronics|4700.00|
Furniture|1700.00|

~~~sql
SELECT Category, SUM(Amount) AS TotalSales
FROM Sales
GROUP BY ROLLUP(Category);
~~~
Output:
|Category|TotalSales|
|--------|----------|
|Electronics|4700.00|
|Furniture|1700.00|
|NULL|6400.00|

### Correlated Subquery
1. Finding customer who has placed more than one order
~~~sql
SELECT DISTINCT o1.CustomerID
FROM Orders2 o1
WHERE(
	SELECT COUNT(*)
	FROM Orders2 o2
	WHERE o2.CustomerID = o1.CustomerID
) > 1;
~~~