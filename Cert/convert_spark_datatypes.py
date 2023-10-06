### Passing schema as DataType
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DateType

data = [
    (1, "akshay", 28, True, datetime.datetime(1995, 5, 6)),
    (2, "akshay", 28, True, datetime.datetime(1995, 5, 6)),
    (3, "akshay", 28, True, datetime.datetime(1995, 5, 6)),
    (4, "akshay", 28, True, datetime.datetime(1995, 5, 6))
    ]

# convert list of tuples -> list of rows
from pyspark.sql import Row
users_row_data = [Row(*user) for user in data]

# schema
users_schema = StructType(fields=
                          [
                              StructField("id", IntegerType()),
                              StructField("name", StringType()),
                              StructField("age", IntegerType()),
                              StructField("isCustomer", BooleanType()),
                              StructField("date", DateType())
                          ])

# convert list of rows to Spark Df
spark.createDataFrame(users_row_data, schema=users_schema).show()
