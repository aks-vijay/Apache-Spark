## get area code, last 4 digit from num, last 4 digit from SSN
from pyspark.sql.functions import lit, concat
employees_df \
    .withColumn("area_code", split(employees_df.phone_num, " ")[0]) \
    .withColumn("phone_num_last4", split(employees_df.phone_num, " ")[3]) \
    .withColumn("ssn_last4", concat(lit("***-***-"), split(employees_df.ssn, " ")[2])) \
    .show()
