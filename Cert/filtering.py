users_df.where(users_df.id == 1).show()
users_df.where(users_df["id"] == 1).show()
users_df.filter("id == 1").show()

users_df.createOrReplaceTempView("users")
spark.sql("""
          SELECT * FROM users where id = 1
          """).show()

# to identify is NaN field in numeric fields
from pyspark.sql.functions import isnan
users_df.select(users_df.id, isnan(users_df.id)).show()

users_df.where(
    users_df.customer_from.between("2020-01-01", "2021-01-01")
).show()

users_df.where(users_df.customer_from.isNull()).show()
users_df.where(users_df.customer_from.isNotNull()).show()
# conditions
users_df.where(users_df.customer_from.isNotNull() & users_df.id.isNotNull()).show()

users_df.where(users_df.customer_from.isNotNull() & users_df.id.isNull() == False).show()
#isin
users_df.where(
    users_df.id.isin(1,2)
).show()

# for not in
#isin
users_df.where(
    ~users_df.id.isin(1,2)
).show()

users_df.where(users_df.customer_from.isNotNull() & users_df.id.isNotNull()).show()

users_df.where(
    users_df.customer_from.isNotNull() & 
    ~isnan(users_df.id)  
    ).show()
