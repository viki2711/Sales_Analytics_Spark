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
spark = (SparkSession.builder.appName("TestProject").getOrCreate())

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

# 2. What was the most expensive item that was purchased by the user who spent the most?
orders_df.select("first_name", "last_name", "item_id", "base_cost")\
         .where(col("account_id") == 143)\
         .orderBy("base_cost", ascending=False).show(1)

# 3. How much did Marissa Washington Spend?
orders_df.where((col("first_name") == "Marissa") & (col("last_name") == "Washington"))\
         .orderBy("price_paid", ascending=False).show()