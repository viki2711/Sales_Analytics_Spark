from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from main import spark

# creating a schema for building items data frame
items_schema = StructType([
    StructField("item_id", StringType(), True),
    StructType([
        StructField("quality", IntegerType(), True),
        StructField("price", StringType(), True),
        StructField("itemName", StringType(), True)
    ])
])

# item data file path
items_file_path = "data/item_data.json"

# creating a data frame
# items_df = (spark.read.json(items_file_path, items_schema, multiLine=True))
# items_df.show(10, truncate=False)