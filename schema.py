from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


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