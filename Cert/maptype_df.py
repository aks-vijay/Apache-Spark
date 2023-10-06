from pyspark.sql.types import datetime

## Converting Special types like array
user_data = [
    {
        "user_id": 1,
        "user_name":"akshay",
        "user_age":28,
        "isCustomer":True,
        "birth_date": datetime.datetime(1995, 5, 6),
        "phone_num":{"home":"+1-609-293-4929","mobile":"+1-609-293-2829"}
    },
    {
        "user_id": 2,
        "user_name":"sankar",
        "user_age":28,
        "isCustomer":False,
        "birth_date": datetime.datetime(1995, 5, 6),
        "phone_num":{"home":"+1-609-293-4929","mobile":"+1-609-293-2829"}
    },
    {
        "user_id": 3,
        "user_name":"sankar",
        "user_age":28,
        "isCustomer":False,
        "birth_date": datetime.datetime(1995, 5, 6),
        "phone_num":{"home":"+1-609-293-4929","mobile":"+1-609-293-2829"}
    },
    {
        "user_id": 4,
        "user_name":"sankar",
        "user_age":28,
        "isCustomer":False,
        "birth_date": datetime.datetime(1995, 5, 6),
        "phone_num":{"home":"+1-609-293-4929","mobile":"+1-609-293-2829"}
    }
]

# convert list of dictionary to spark DF
from pyspark.sql import Row
df = spark.createDataFrame([Row(**user) for user in user_data])
# phone number is map datatype

# fetch using key of the dictionary
df.select(df["*"], 
          df.phone_num["home"],
          df.phone_num["mobile"]) \
  .show(truncate=False)

# explode into seperate columns
from pyspark.sql.functions import explode
df \
    .select("*", explode(df.phone_num)) \
    .withColumnRenamed("key", "phone_type") \
    .withColumnRenamed("value", "phone_num") \
    .drop(df.phone_num) \
    .show(truncate=False)
