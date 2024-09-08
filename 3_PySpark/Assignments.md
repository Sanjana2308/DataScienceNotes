# Assignment

## Dataset - 1 : Fitness Tracker Data
This dataset contains information from a fitness tracker app. Each row represents a
user's daily activity, including steps taken, calories burned, distance traveled, and
active minutes.

### Sample Data:
```csv
user_id,date,steps,calories,distance_km,active_minutes
1,2023-07-01,12000,500,8.5,90
2,2023-07-01,8000,350,5.6,60
3,2023-07-01,15000,600,10.2,120
1,2023-07-02,11000,480,7.9,85
2,2023-07-02,9000,400,6.2,70
3,2023-07-02,13000,520,9.0,100
1,2023-07-03,10000,450,7.1,80
2,2023-07-03,7000,320,4.9,55
3,2023-07-03,16000,620,11.0,130
```

### Exercises
**Setting up the environment:**
```python
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practice').getOrCreate()

file_name = '/content/sample_data/fitness_tracker.csv'
df = spark.read.csv(file_name, header=True, inferSchema=True)
df.show()
```


**1. Find the Total Steps Taken by Each User**
- Calculate the total number of steps taken by each user across all days.
```python
df_total_steps = df.groupBy('user_id').agg({'steps': 'sum'})
df_total_steps.show()
```

**2. Filter Days Where a User Burned More Than 500 Calories**
- Identify all days where a user burned more than 500 calories.
```python
calories_burned = df.filter(df['calories'] > 500)
calories_burned.show()
```

**3. Calculate the Average Distance Traveled by Each User**
- Calculate the average distance traveled ( distance_km ) by each user across all days.
```python
average_distance = df.groupBy('user_id').agg({'distance_km': 'avg'})
average_distance.show()
```

**4. Identify the Day with the Maximum Steps for Each User**
- For each user, find the day when they took the maximum number of steps.
```python
day_with_max_steps = df.groupBy('user_id').agg({'steps': 'max'})
day_with_max_steps.show()
```

**5. Find Users Who Were Active for More Than 100 Minutes on Any Day**
- Identify users who had active minutes greater than 100 on any day.
```python
users_active_minutes = df.filter(df['active_minutes'] > 100)
users_active_minutes.show()
```

**6. Calculate the Total Calories Burned per Day**
Group by date and calculate the total number of calories burned by all users combined for each day.
```python
total_calories_per_day = df.groupBy('date').agg({'calories': 'sum'})
total_calories_per_day.show()
```

**7. Calculate the Average Steps per Day**
- Find the average number of steps taken across all users for each day.
```python
average_steps_per_day = df.groupBy('date').agg({'steps': 'avg'})
average_steps_per_day.show()
```

**8. Rank Users by Total Distance Traveled**
- Rank the users by their total distance traveled, from highest to lowest.
```python
ranking_by_distance_traveled = df.groupBy('user_id').agg({'distance_km': 'sum'}).orderBy('sum(distance_km)', ascending=False)
ranking_by_distance_traveled.show()
```

**9. Find the Most Active User by Total Active Minutes**
- Identify the user with the highest total active minutes across all days.
```python
most_active_user = df.groupBy('user_id').agg({'active_minutes': 'sum'}).orderBy('sum(active_minutes)', ascending=False)
most_active_user.show()
```

**10. Create a New Column for Calories Burned per Kilometer**
- Add a new column called calories_per_km that calculates how many calories were burned per kilometer ( calories / distance_km ) for each row.
```python
df = df.withColumn('calories_per_km', df['calories'] / df['distance_km'])
df.show()
```

## Dataset - 2 : Book Sales Data
This dataset contains information about book sales in a store. Each row represents a
sale, including details about the book, author, genre, sale price, and the date of the
transaction.

### Sample Data:
```csv
sale_id,book_title,author,genre,sale_price,quantity,date
1,The Catcher in the Rye,J.D. Salinger,Fiction,15.99,2,2023-01-05
2,To Kill a Mockingbird,Harper Lee,Fiction,18.99,1,2023-01-10
3,Becoming,Michelle Obama,Biography,20.00,3,2023-02-12
4,Sapiens,Yuval Noah Harari,Non-Fiction,22.50,1,2023-02-15
5,Educated,Tara Westover,Biography,17.99,2,2023-03-10
6,The Great Gatsby,F. Scott Fitzgerald,Fiction,10.99,5,2023-03-15
7,Atomic Habits,James Clear,Self-Help,16.99,3,2023-04-01
8,Dune,Frank Herbert,Science Fiction,25.99,1,2023-04-10
9,1984,George Orwell,Fiction,14.99,2,2023-04-12
10,The Power of Habit,Charles Duhigg,Self-Help,18.00,1,2023-05-01
```

### Exercises:

**Setting up the environment:**
```python
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practice').getOrCreate()

file_name = '/content/sample_data/book_sales.csv'
df = spark.read.csv(file_name, header=True, inferSchema=True)
df.show()
```

**1. Find Total Sales Revenue per Genre**
- Group the data by genre and calculate the total sales revenue for each genre. (Hint: Multiply sale_price by quantity to get total sales for each book.)
```python
total_sales_revenue_per_genre = df.groupBy('genre').agg({'sale_price': 'sum'})
total_sales_revenue_per_genre.show()
```

**2. Filter Books Sold in the "Fiction" Genre**
- Filter the dataset to include only books sold in the "Fiction" genre.
```python
fiction_books = df.filter(df['genre'] == 'Fiction')
fiction_books.show()
```

**3. Find the Book with the Highest Sale Price**
- Identify the book with the highest individual sale price.
```python
highest_sale_price_book = df.orderBy('sale_price', ascending=False).limit(1)
highest_sale_price_book.show()
```

**4. Calculate Total Quantity of Books Sold by Author**
- Group the data by author and calculate the total quantity of books sold for each author.
```python
total_quantity_sold_by_author = df.groupBy('author').agg({'quantity': 'sum'})
total_quantity_sold_by_author.show()
```

**5. Identify Sales Transactions Worth More Than $50**
- Filter the sales transactions where the total sales amount (sale_price * quantity) is greater than $50.
```python
sales_transactions_over_50 = df.filter((df['sale_price'] * df['quantity']) > 50)
sales_transactions_over_50.show()
```

**6. Find the Average Sale Price per Genre**
- Group the data by genre and calculate the average sale price for books in each genre.
```python
average_sale_price_per_genre = df.groupBy('genre').agg({'sale_price': 'avg'})
average_sale_price_per_genre.show()
```
**7. Count the Number of Unique Authors in the Dataset**
- Count how many unique authors are present in the dataset.
```python
unique_authors_count = df.select('author').distinct().count()
print(unique_authors_count)
```

**8. Find the Top 3 Best-Selling Books by Quantity**
- Identify the top 3 best-selling books based on the total quantity sold.
```python
top_3_best_selling_books = df.orderBy('quantity', ascending=False).limit(3)
top_3_best_selling_books.show()
```

**9. Calculate Total Sales for Each Month**
- Group the sales data by month and calculate the total sales revenue for each month.
```python
from pyspark.sql.functions import month

df_with_month = df.withColumn('month', month(df['date']))
total_sales_per_month = df_with_month.groupBy('month').agg({'sale_price': 'sum'})
total_sales_per_month.show()
```

**10. Create a New Column for Total Sales Amount**
- Add a new column total_sales that calculates the total sales amount for each transaction ( sale_price * quantity ).
```python
df_with_total_sales = df.withColumn('total_sales', df['sale_price'] * df['quantity'])
df_with_total_sales.show()
```

## Dataset - 3 : Food Delivery Orders
This dataset contains information about food delivery orders placed by customers. Each
row represents a single order, including details like the order ID, customer ID,
restaurant name, food item, quantity, price, delivery time, and order date.

### Sample Data:
```csv
order_id,customer_id,restaurant_name,food_item,quantity,price,delivery_time_mins,order_d
1,201,McDonald's,Burger,2,5.99,30,2023-06-15
2,202,Pizza Hut,Pizza,1,12.99,45,2023-06-16
3,203,KFC,Fried Chicken,3,8.99,25,2023-06-17
4,201,Subway,Sandwich,2,6.50,20,2023-06-17
5,204,Domino's,Pizza,2,11.99,40,2023-06-18
6,205,Starbucks,Coffee,1,4.50,15,2023-06-18
7,202,KFC,Fried Chicken,1,8.99,25,2023-06-19
8,206,McDonald's,Fries,3,2.99,15,2023-06-19
9,207,Burger King,Burger,1,6.99,30,2023-06-20
10,203,Starbucks,Coffee,2,4.50,20,2023-06-20
```

### Exercises:

**Setting up the environment:**
```python
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practice').getOrCreate()

file_name = '/content/sample_data/orders.csv'
df = spark.read.csv(file_name, header=True, inferSchema=True)
df.show()
```

**1. Calculate Total Revenue per Restaurant**
- Group the data by restaurant_name and calculate the total revenue for each restaurant. (Hint: Multiply price by quantity to get total revenue per order.)
```python
total_revenue_per_restaurant = df.groupBy('restaurant_name').agg({'price': 'sum'})
total_revenue_per_restaurant.show()
```

**2. Find the Fastest Delivery**
- Identify the order with the fastest delivery time.
```python
fastest_delivery = df.orderBy('delivery_time_mins').limit(1)
fastest_delivery.show()
```

**3. Calculate Average Delivery Time per Restaurant**
- Group the data by restaurant_name and calculate the average delivery time for each restaurant.
```python
average_delivery_time_per_restaurant = df.groupBy('restaurant_name').agg({'delivery_time_mins': 'avg'})
average_delivery_time_per_restaurant.show()
```

**4. Filter Orders for a Specific Customer**
- Filter the dataset to include only orders placed by a specific customer (e.g., customer_id = 201 ).
```python
specific_customer_orders = df.filter(df.customer_id == 201)
specific_customer_orders.show()
```

**5. Find Orders Where Total Amount Spent is Greater Than $20**
- Filter orders where the total amount spent (price * quantity) is greater than $20.
```python
df = df.withColumn('total_amount', df['price'] * df['quantity'])
orders_greater_than_20 = df[df['total_amount'] > 20]
orders_greater_than_20.show()
```

**6. Calculate the Total Quantity of Each Food Item Sold**
- Group the data by food_item and calculate the total quantity of each food item sold.
```python
total_quantity_per_food_item = df.groupBy('food_item').agg({'quantity': 'sum'})
total_quantity_per_food_item.show()
```

**7. Find the Top 3 Most Popular Restaurants by Number of Orders**
- Identify the top 3 restaurants with the highest number of orders placed.
```python
top_3_popular_restaurants = df.groupBy('restaurant_name').count().orderBy('count', ascending=False).limit(3)
top_3_popular_restaurants.show()
```
**8. Calculate Total Revenue per Day**
- Group the data by order_date and calculate the total revenue for each day.
```python
total_revenue_per_day = df.groupBy('order_d').agg({'price': 'sum'})
total_revenue_per_day.show()
```

**9. Find the Longest Delivery Time for Each Restaurant**
- For each restaurant, find the longest delivery time.
```python
longest_delivery_time_per_restaurant = df.groupBy('restaurant_name').agg({'delivery_time_mins': 'max'})
longest_delivery_time_per_restaurant.show()
```

**10. Create a New Column for Total Order Value**
- Add a new column total_order_value that calculates the total value of each order ( price * quantity ).
```python
df = df.withColumn('total_order_value', df['price'] * df['quantity'])
df.show()
```

## Dataset - 4 : Weather Data
This dataset contains daily weather observations recorded in different cities. Each
row represents the weather data for a specific city on a given day, including the
temperature, humidity, wind speed, and the condition of the day.

### Sample Data:
```csv
date,city,temperature_c,humidity,wind_speed_kph,condition
2023-01-01,New York,5,60,20,Cloudy
2023-01-01,Los Angeles,15,40,10,Sunny
2023-01-01,Chicago,-2,75,25,Snow
2023-01-02,New York,3,65,15,Rain
2023-01-02,Los Angeles,18,35,8,Sunny
2023-01-02,Chicago,-5,80,30,Snow
2023-01-03,New York,6,55,22,Sunny
2023-01-03,Los Angeles,20,38,12,Sunny
2023-01-03,Chicago,-1,70,18,Cloudy
```

### Exercises:

**Setting up the environment:**
```python
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practice').getOrCreate()

file_name = '/content/sample_data/weather.csv'
df = spark.read.csv(file_name, header=True, inferSchema=True)
df.show()
```

**1. Find the Average Temperature for Each City**
- Group the data by city and calculate the average temperature for each city.
```python
avg_temp_df = df.groupBy('city').agg({'temperature_c': 'avg'})
avg_temp_df.show()
```

**2. Filter Days with Temperature Below Freezing**
- Filter the data to show only the days where the temperature was below freezing (below 0°C).
```python
freezing_df = df.filter(df['temperature_c'] < 0)
freezing_df.show()
```

**3. Find the City with the Highest Wind Speed on a Specific Day**
- Find the city with the highest wind speed on a specific day (e.g., 2023-01-02 ).
```python
specific_day_df = df.filter(df['date'] == '2023-01-02')
max_wind_city = specific_day_df.orderBy(specific_day_df['wind_speed_kph'].desc()).first()
print("City with the highest wind speed on 2023-01-02:", max_wind_city['city'])
```

**4. Calculate the Total Number of Days with Rainy Weather**
- Count the number of days where the condition was "Rain."
```python
rainy_days_count = df.filter(df['condition'] == 'Rain').count()
print("Total number of days with rainy weather:", rainy_days_count)
```

**5. Calculate the Average Humidity for Each Weather Condition**
- Group the data by condition and calculate the average humidity for each weather condition (e.g., Sunny, Rainy, Cloudy).
```python
avg_humidity_df = df.groupBy('condition').agg({'humidity': 'avg'})
avg_humidity_df.show()
```

**6. Find the Hottest Day in Each City**
- For each city, find the day with the highest recorded temperature.
```python
max_temp_df = df.groupBy('city').agg({'temperature_c': 'max'})
max_temp_df.show()
```

**7. Identify Cities That Experienced Snow**
- Filter the dataset to show only the cities that experienced "Snow" in the condition .
```python
snow_cities_df = df.filter(df['condition'] == 'Snow')
snow_cities_df.show()
```

**8. Calculate the Average Wind Speed for Days When the Condition was Sunny**
- Filter the dataset for condition = 'Sunny' and calculate the average wind speed on sunny days.
```python
avg_wind_sunny_df = df.filter(df['condition'] == 'Sunny').agg({'wind_speed_kph': 'avg'})
avg_wind_sunny_df.show()
```

**9. Find the Coldest Day Across All Cities**
- Identify the day with the lowest temperature across all cities.
```python
coldest_day = df.orderBy(df['temperature_c'].asc()).first()
print("Coldest day across all cities:", coldest_day['date'])
```

**10. Create a New Column for Wind Chill**
- Add a new column wind_chill that estimates the wind chill based on the formula: [ \text{Wind Chill} = 13.12 + 0.6215 \times \text{Temperature} - 11.37 \times (\text{Wind Speed}^{0.16}) + 0.3965 \times \text{Temperature} \times (\text{Wind Speed}^{0.16}) ]<br>
(Assume wind_speed_kph is the wind speed in kilometers per hour.)
```python
df = df.withColumn('wind_chill', 13.12 + 0.6215 * df['temperature_c'] - 11.37 * (df['wind_speed_kph'] ** 0.16) + 0.3965 * df['temperature_c'] * (df['wind_speed_kph'] ** 0.16))
df.show()
```

## Dataset - 5 : Airline Flight Data
This dataset contains information about flights, including details like the airline,
flight number, departure and arrival times, delays, and the distance traveled.

### Sample Data:
```csv
flight_id,airline,flight_number,origin,destination,departure_time,arrival_time,delay_min,distance,flight_date
1,Delta,DL123,JFK,LAX,08:00,11:00,30,3970,2023-07-01
2,United,UA456,SFO,ORD,09:30,15:00,45,2960,2023-07-01
3,Southwest,SW789,DAL,ATL,06:00,08:30,0,1150,2023-07-01
4,Delta,DL124,LAX,JFK,12:00,20:00,20,3970,2023-07-02
5,American,AA101,MIA,DEN,07:00,10:00,15,2770,2023-07-02
6,United,UA457,ORD,SFO,11:00,14:30,0,2960,2023-07-02
7,JetBlue,JB302,BOS,LAX,06:30,09:45,10,4180,2023-07-03
8,American,AA102,DEN,MIA,11:00,14:00,25,2770,2023-07-03
9,Southwest,SW790,ATL,DAL,09:00,11:00,5,1150,2023-07-03
10,Delta,DL125,JFK,SEA,13:00,17:00,0,3900,2023-07-04
```

### Exercises:

**Setting up the environment:**
```python
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practice').getOrCreate()

file_name = '/content/sample_data/airline.csv'
df = spark.read.csv(file_name, header=True, inferSchema=True)
df.show()
```
**1. Find the Total Distance Traveled by Each Airline**
- Group the data by airline and calculate the total distance traveled for each airline.
```python
from pyspark.sql.functions import sum

total_distance = df.groupBy('airline').agg({'distance': 'sum'})
total_distance.show()
```

**2. Filter Flights with Delays Greater than 30 Minutes**
- Filter the dataset to show only flights where the delay was greater than 30 minutes.
```python
filtered_flights = df.filter(df['delay_min'] > 30)
filtered_flights.show()
```

**3. Find the Flight with the Longest Distance**
- Identify the flight that covered the longest distance.
```python
longest_flight = df.orderBy(df['distance'].desc()).limit(1)
longest_flight.show()
```

**4. Calculate the Average Delay Time for Each Airline**
- Group the data by airline and calculate the average delay time in minutes for each airline.
```python
avg_delay = df.groupBy('airline').agg({'delay_min': 'avg'})
avg_delay.show()
```

**5. Identify Flights That Were Not Delayed**
- Filter the dataset to show only flights with delay_minutes = 0 .
```python
not_delayed_flights = df.filter(df['delay_min'] == 0)
not_delayed_flights.show()
```

**6. Find the Top 3 Most Frequent Routes**
- Group the data by origin and destination to find the top 3 most frequent flight routes.
```python
top_routes = df.groupBy('origin', 'destination').count().orderBy('count', ascending=False).limit(3)
top_routes.show()
```

**7. Calculate the Total Number of Flights per Day**
- Group the data by date and calculate the total number of flights on each day.
```python
flights_per_day = df.groupBy('flight_date').count()
flights_per_day.show()
```

**8. Find the Airline with the Most Flights**
- Identify the airline that operated the most flights.
```python
most_flights_airline = df.groupBy('airline').count().orderBy('count', ascending=False).limit(1)
most_flights_airline.show()
```

**9. Calculate the Average Flight Distance per Day**
- Group the data by date and calculate the average flight distance for each day.
```python
avg_distance_per_day = df.groupBy('flight_date').agg({'distance': 'avg'})
avg_distance_per_day.show()
```

**10. Create a New Column for On-Time Status**
-Add a new column called on_time that indicates whether a flight was on time ( True if delay_minutes = 0 , otherwise False ).
```python
from pyspark.sql.functions import when

df = df.withColumn('on_time', when(df['delay_min'] == 0, True).otherwise(False))
df.show()
```

