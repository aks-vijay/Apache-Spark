%run "../01 Create Spark Dataframes/create_dataframe"

# this is the easiest
users_df.selectExpr('user_id', '(user_age + 25) AS user_age_upd').show()

# using pyspark syntax - col, df style syntax
from pyspark.sql.functions import lit, col
users_df.select('user_id', (col('user_age') + lit(25)).alias('age')).show()
users_df.select('user_id', (users_df['user_age'] + lit(25)).alias('age')).show()

# Renaming multiple columns in one shot
# renaming multiple spark dataframe columns
required_columns = ['user_id', 'f_name', 'l_name', 'birth_date']
target_columns = ['user_id', 'user_name', 'user_age', 'user_dob']

# To df accepts varying arguments. Need to use *
# To Df when list is passed it converts to tuple (['user_id', 'user_name', 'user_age', 'user_dob'], )
# To convert to varying argument, need to use * to convert it to ['user_id', 'user_name', 'user_age', 'user_dob']
users_df.select(required_columns).toDF(*target_columns).show()
