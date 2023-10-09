users_df.sort("f_name").show()
users_df.sort(users_df.f_name.desc()).show()
users_df.sort(users_df["f_name"].desc()).show()
users_df.sort(col("f_name").desc()).show()
users_df.sort("customer_from", ascending=True).show()

# ascending the nulls first and then the numbers
users_df.sort(users_df.customer_from.asc_nulls_first()).show()

# ascending the numbers first and then the nulls
users_df.sort(users_df.customer_from.asc_nulls_last()).show()

# descending null first then descending the numbers
users_df.sort(users_df.customer_from.desc_nulls_first()).show()

# descending the numbers first and then the nulls last
users_df.sort(users_df.customer_from.desc_nulls_last()).show()

# composite sorting - Sorting one column and then the next column
users_df.sort(users_df.customer_from, users_df.f_name).show()

# another syntax 1 is ascending and 0 is descending
users_df.sort(['f_name', 'l_name'], ascending = [1,0]).show()
