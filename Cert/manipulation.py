from pyspark.sql.functions import date_format, col, cast

# convert to date format
users_df \
    .select(
        col('user_id'),
        date_format(users_df.birth_date, 'yyyyMMdd').alias("converted_dt")
    ) \
    .printSchema()

# cast the date to int
from pyspark.sql.functions import date_format, col, cast

# convert to date format -> cast('DataType') should be included inside bracket
users_df \
    .select(
        col('user_id'),
        date_format(users_df.birth_date, 'yyyyMMdd').cast('int').alias("conv_dt")
    ) \
    .printSchema()

# formatting using list of columns -> to convert to varying columns
columns = [col('user_id'), 
           date_format('birth_date', 'yyyyMMdd').cast('int').alias('converted_dt')
           ]

users_df.select(*columns).show()

# cast to full_name
from pyspark.sql.functions import concat, col, lit
full_name_columns = [col('f_name'), col('l_name'), concat(col('f_name'), lit(', '), col('l_name')).alias('f_name')]
users_df.select(full_name_columns).show()

# convert birth_dt to numeric
formatted_birth_dt = date_format(col('birth_date'), 'yyyyMMdd') \
                        .cast('numeric').alias('birth_dt')
users_df.select(col('user_id'), formatted_birth_dt).show()
