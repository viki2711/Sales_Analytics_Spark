from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
from schema import schema
from clean_orders import clean_orders
from parse_orders import parse_order, parse_orders
from helpers import open_json
import pandas as pd

# creating a spark session
spark = (SparkSession.builder.appName("OrdersDF").getOrCreate())

# file path
orders_file_path = "data/orders.txt"
items_file_path = "data/item_data.json"

# Items file
# opening an item_data.json file
items_data = open_json(items_file_path)
# saving json items data to a pandas dataframe
df_items = pd.DataFrame(items_data)

# Orders file
# cleaning the text file
data = clean_orders(orders_file_path)
# parsing cleaned data to the correct data types
parsed_data = parse_orders(parse_order, data)

# creating a data frame with cleaned parsed data and the schema
orders_df = spark.createDataFrame(data=parsed_data, schema=schema)

####--------------------------------------------------------------------------####

# 1. Which user spent the most?
user_df = (orders_df.groupBy("first_name", "last_name", "account_id")
          .agg(sum("price_paid").alias("sum_paid")).orderBy("sum_paid", ascending=False))

user = user_df.first()
user_id = user["account_id"]
user_name = user["first_name"] + " " + user["last_name"]
max_value = round(user["sum_paid"], 2)

print(f'1. User who spent the most is: {user_name}, account_id: {user_id}, value: {max_value}$')

# User that spent the most: Marissa Jefferson, account_id = 143, 105.09$.

####--------------------------------------------------------------------------####

# 2. What was the most expensive item that was purchased by the user who spent the most?
item_df = orders_df.select("first_name", "last_name", "account_id", "item_id", "base_cost")\
         .where(col("account_id") == user_id)\
         .orderBy("base_cost", ascending=False)

item = item_df.first()
item_id = item["item_id"]
# fetching the correct info from pandas dataframe
item_info = df_items[str(item_id)]

print(f'2. The most expensive item that user {user_id} purchased was: '
      f'item_id: {item_id}, cost: {item["base_cost"]}$, '
      f'item info - quality: {item_info["quality"]}, price: {item_info["price"]}$, name: {item_info["itemName"]}')

# The most expensive item that user 143 purchased was: item_id = 2746, cost = 99.99$.

####--------------------------------------------------------------------------####

# 3. How much did Marissa Washington Spend?
selected_users_df = (orders_df.select("first_name", "last_name", "account_id", "price_paid")
          .where((col("first_name") == "Marissa") & (col("last_name") == "Washington")))\
          .groupBy("first_name", "last_name", "account_id")\
          .agg(sum("price_paid").alias("sum_paid"))

selected_users = selected_users_df.collect()

# formatting function
def print_users(selected_users):
    output = f'3. '
    for user in selected_users:
        user_name = user["first_name"] + " " + user["last_name"]
        sum_paid = round(user["sum_paid"], 2)
        output += f'{user_name}, account_id: {user["account_id"]}, spent: {sum_paid}$ | '
    print(output)

print_users(selected_users)
# Marissa Washington account_id = 145 , spent 26.97$.
# Marissa Washington account_id = 125, spent 18.45$.
