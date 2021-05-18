from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
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

# creating a schema for building a data frame
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
orders_df.show(10)
