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
        "birth_date": datetime.datetime(1995, 5, 6)
    },
    {
        "user_id": 1,
        "user_name":"sankar",
        "user_age":28,
        "birth_date": datetime.datetime(1995, 5, 6)
    }
]

## convert list of list with missing values to Pandas DF
## list with missing values - Spark DF throws error
import pandas as pd
pandas_df = pd.DataFrame(user_data)

# convert Pandas DF to Spark DF
spark.createDataFrame(pandas_df).show()
