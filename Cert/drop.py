from pyspark.sql.functions import col

# drop single column
users_df.drop(users_df.customer_from).show()

# drop multiple column
users_df.drop(users_df.customer_from, users_df.phone_nums).show()

# this will fail because col is an object. Drop accepts only strings or varying list of strings
users_df.drop(users_df.customer_from, users_df.phone_nums, col(users_df.gender)).show()

# using varying list of strings
to_drop = ["id", "f_name", "l_name"]
users_df.drop(*to_drop).show()

# drop duplicates always accepts ONLY LISTS
to_drop = ["f_name", "l_name"]
users_df.dropDuplicates(to_drop).show()

# to drop null, thresh, na.drop
# drop na attributes
# any -> If atleast one is null it will be deleted
# all -> if all are null it will be deleted
users_df.dropna(how="any").show()
users_df.dropna(how="all").show()

## based on subset
users_df.dropna(how="all", subset=["customer_from"]).show()

# na
# attributes which support are drop, fill, replace
# thresh = 9 indicates rows which has less than 9 non null values will be dropped
users_df.na.drop(thresh=9).show()
