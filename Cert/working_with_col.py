%run "../01 Create Spark Dataframes/create_dataframe"

# this will return column object
users_df["user_id"]

# multiple columns will return a dataframe
users_df["user_id", "f_name"]

# using col type functions to fetch individual column object
from pyspark.sql.functions import col
users_df.select(users_df["user_id"], col("f_name"), col("l_name")).show()

# however col will not work inside selectExpr as it expects SQL style syntax
users_df.selectExpr(col("user_id"))

# this will not work since u is added inside the string and accessed outside
# pyspark uses lazy evaluation. Only when action is called it evaluates
users_df.alias("u").select(u["user_id"])

# this will work since u is added inside the string and acccessed inside the string
users_df.alias("u").select("u.user_id").show()

# using selectExpr
users_df.alias("u") \
    .selectExpr("u.f_name", 
            "u.l_name",
            "concat(u.f_name, ', ', u.l_name) AS full_name") \
    .show()

# using SQL style syntax - Dont have to import any functions. Works similar to SelectExpr
users_df.createOrReplaceTempView("users")

spark.sql('''
          SELECT U.f_name, U.l_name, CONCAT(U.f_name, ', ', U.l_name) AS full_name
          FROM users AS U 
          ''') \
    .show()

# col can be converted to an object
# col accepts literal string or list of strings
from pyspark.sql.functions import col
user_id = col('user_id')

# converting list of cols to varying col object. * is important as select accepts varying columns
col = ["user_id","f_name", "l_name"]
users_df.select(*col).show()
users_df.select(user_id).show()
