from pyspark.sql.functions import substring
employees_df.select(employees_df.employee_id, employees_df.phone_num, employees_df.ssn) \
    .withColumn("phonenum_last4", substring(employees_df.phone_num, -4, 4).cast("int")) \
    .withColumn("ssn_last4", substring(employees_df.ssn, 8, 5).cast("int")) \
    .show()
