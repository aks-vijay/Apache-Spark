from pyspark.sql.types import datetime

## Converting Special types like array
user_data = [
    {
        "user_id": 1,
        "user_name":"akshay",
        "user_age":28,
        "isCustomer":True,
        "birth_date": datetime.datetime(1995, 5, 6),
        "phone_num":["+1-609-293-4929","+1-609-293-2829"]
    },
    {
        "user_id": 2,
        "user_name":"sankar",
        "user_age":28,
        "isCustomer":False,
        "birth_date": datetime.datetime(1995, 5, 6),
        "phone_num":None
    },
    {
        "user_id": 3,
        "user_name":"sankar",
        "user_age":28,
        "isCustomer":False,
        "birth_date": datetime.datetime(1995, 5, 6),
        "phone_num":["+1-609-293-4929"]
    },
    {
        "user_id": 4,
        "user_name":"sankar",
        "user_age":28,
        "isCustomer":False,
        "birth_date": datetime.datetime(1995, 5, 6),
        "phone_num":["+1-609-293-4929","+1-609-293-2829"]
    }
]

# convert the list of dictionaries with special array type into --> Row
from pyspark.sql import Row
user_data_row = [Row(**user) for user in user_data]
df = spark.createDataFrame(user_data_row)

# phone number data is stored as an array
df.select(df.user_id, df.phone_num).show(truncate=False)

# split the array elements into seperate row object in the DF
from pyspark.sql.functions import explode

# To split into multiple rows -> array data is exploded into multiple rows
# Note: Null value is removed in explode
df \
    .withColumn("phone_number_exploded", explode(df.phone_num)) \
    .drop("phone_num") \
    .show(truncate=False)

# other way of selecting [0] is home number, [1] is mobile number
df \
    .select(df["*"], 
            df.phone_num[0].alias("home_number"),
            df.phone_num[1].alias("mobile_num")) \
    .show(truncate=False)

# split the array elements into seperate row object in the DF
from pyspark.sql.functions import explode_outer

# array data is exploded into multiple rows
# explode outer returns a row even if there's no data
df \
    .withColumn("phone_number_exploded", explode_outer(df.phone_num)) \
    .drop("phone_num") \
    .show(truncate=False)
