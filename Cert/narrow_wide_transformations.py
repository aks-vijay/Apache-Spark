# Narrow transformations
# Narrow - Transformations that does not involve shuffle. Straight away it can transform -> drop, withcolumn, withcolumnrenamed
# Wide - Transformations that involve shuffle. Groupby

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
        "phone_num": Row(mobile="+1-609-293-4929", home="+1-609-293-2829"),
        "courses":[1,2]
    },
    {
        "user_id": 2,
        "user_name":"sankar",
        "user_age":28,
        "isCustomer":False,
        "birth_date": datetime.datetime(1995, 5, 6),
        "phone_num":Row(mobile=None, home=None),
        "courses":[1,2]
    },
    {
        "user_id": 3,
        "user_name":"sankar",
        "user_age":28,
        "isCustomer":False,
        "birth_date": datetime.datetime(1995, 5, 6),
        "phone_num":Row(mobile="+1-609-293-4929", home="+1-609-293-2829"),
        "courses":[]
    },
    {
        "user_id": 4,
        "user_name":"sankar",
        "user_age":28,
        "isCustomer":False,
        "birth_date": datetime.datetime(1995, 5, 6),
        "phone_num":Row(mobile="+1-609-293-4929", home="+1-609-293-2829"),
        "courses":[]
    }
]

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", False)
import pandas as pd 
users_df = spark.createDataFrame(pd.DataFrame(user_data))

# selectExpr uses SQL style syntax to select
# using spark sql functions in selectExpr
users_df.selectExpr("user_id", "user_name", "user_age * 2 AS age_upd", "concat(user_id, ' ', user_name) AS name").show()

# writing the above in PySpark syntax
users_df.select(users_df.user_id, 
                users_df.user_name, 
                (users_df.user_age * 2).alias("age_upd"), 
                concat(users_df.user_id, lit(" "), users_df.user_name).alias("name")) \
        .show()

# writing in spark.sql syntax
users_df.createOrReplaceTempView("users")

spark.sql('''
          SELECT user_id, 
                user_name, 
                user_age * 2 AS age_upd, 
                concat(user_id, ' ', user_name) AS name 
          FROM users;
          ''') \
    .show()

# For 1st and 3rd method -> Spark uses internal spark SQL so does not have to import any functions
