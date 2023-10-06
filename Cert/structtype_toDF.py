from pyspark.sql.types import datetime
from pyspark.sql import Row

## Converting Special types like array
user_data = [
    {
        "user_id": 1,
        "user_name":"akshay",
        "user_age":28,
        "isCustomer":True,
        "birth_date": datetime.datetime(1995, 5, 6),
        "phone_num": Row(mobile="+1-609-293-4929", home="+1-609-293-2829")
    },
    {
        "user_id": 2,
        "user_name":"sankar",
        "user_age":28,
        "isCustomer":False,
        "birth_date": datetime.datetime(1995, 5, 6),
        "phone_num":Row(mobile=None, home=None)
    },
    {
        "user_id": 3,
        "user_name":"sankar",
        "user_age":28,
        "isCustomer":False,
        "birth_date": datetime.datetime(1995, 5, 6),
        "phone_num":Row(mobile="+1-609-293-4929", home="+1-609-293-2829")
    },
    {
        "user_id": 4,
        "user_name":"sankar",
        "user_age":28,
        "isCustomer":False,
        "birth_date": datetime.datetime(1995, 5, 6),
        "phone_num":Row(mobile="+1-609-293-4929", home="+1-609-293-2829")
    }
]

users_df = spark.createDataFrame([Row(**user) for user in user_data])
users_df \
    .select(users_df.user_id, 
            users_df.phone_num["home"].alias("home_number"),
            users_df.phone_num["mobile"].alias("mobile_number")) \
    .show(truncate=False)
