from pyspark.sql.types import datetime
user_data = [
    {
        "user_id": 1,
        "user_name":"akshay",
        "user_age":28,
        "isCustomer":True,
        "birth_date": datetime.datetime(1995, 5, 6)
    },
    {
        "user_id": 1,
        "user_name":"sankar",
        "user_age":28,
        "isCustomer":False,
        "birth_date": datetime.datetime(1995, 5, 6)
    },
    {
        "user_id": 1,
        "user_name":"sankar",
        "user_age":28,
        "isCustomer":False,
        "birth_date": datetime.datetime(1995, 5, 6)
    },
    {
        "user_id": 1,
        "user_name":"sankar",
        "user_age":28,
        "isCustomer":False,
        "birth_date": datetime.datetime(1995, 5, 6)
    }
]

## convert list of dictionary to -> list of rows -> Then to Spark DF
user_data_row = [Row(**user) for user in user_data]
users_df = spark.createDataFrame(user_data_row, 'user_id int, user_name string, user_age int, isCustomer boolean, birth_date date')
