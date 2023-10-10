from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define the schema using StructType
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_customer_id", IntegerType(), True),
    StructField("order_status", StringType(), True)
])

# Read the CSV file with the defined schema
df = spark.read.csv("dbfs:/FileStore/root/courses/", schema=schema)

# register
dc = spark.udf.register('date_convert',lambda x: int(x[:10].replace("-", "")))

# can be called using the variable dc
df.select(dc(df.order_date).alias("order_date")).show()
df.where(dc(df.order_date) == 20130725).show()

# for spark SQL, we need to use what is registered. Not the variable
df.createOrReplaceTempView("orders")
spark.sql("""
          SELECT orders.*, date_convert(order_date) FROM orders 
          """).show()
