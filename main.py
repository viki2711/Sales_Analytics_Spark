from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import min, max, sum, col
from clean_orders import clean_orders
from parse_orders import parse_order, parse_orders

# file path
file = "data/orders.txt"
# cleaning the text file
data = clean_orders(file)
# parsing cleaned data to the correct data types
parsed_data = parse_orders(parse_order, data)

# creating a spark session
spark = (SparkSession.builder.appName("BethesdaProject").getOrCreate())

# creating a schema for building orders data frame
schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("sale", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("base_cost", StringType(), True),
    StructField("price_paid", FloatType(), True)
])

# creating a data frame with cleaned parsed data and the schema
orders_df = spark.createDataFrame(data=parsed_data, schema=schema)
# orders_df.printSchema()
# orders_df.show(10)
# orders_df.orderBy("account_id").show(100)

# 1. Which user spent the most?
(orders_df.groupBy("first_name", "last_name", "account_id")
          .agg(sum("price_paid").alias("sum_paid")).orderBy("sum_paid", ascending=False)).show(1)

# User that spent the most - account_id = 143, Marissa Jefferson.

# 2. What was the most expensive item that was purchased by the user who spent the most?
orders_df.select("first_name", "last_name", "account_id", "item_id", "base_cost")\
         .where(col("account_id") == 143)\
         .orderBy("base_cost", ascending=False).show(1)

# The most expensive item that user 143 purchased was: item_id = 2746, cost = 99.99.

# 3. How much did Marissa Washington Spend?
(orders_df.select("first_name", "last_name", "account_id", "price_paid")
          .where((col("first_name") == "Marissa") & (col("last_name") == "Washington")))\
          .groupBy("first_name", "last_name", "account_id")\
          .agg(sum("price_paid").alias("sum_paid")).show()

# Marissa Washington account_id = 145 , spent 26.969.
# Marissa Washington account_id = 125, spent 18.449.
