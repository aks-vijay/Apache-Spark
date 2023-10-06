users = [
    (1, "akshay", 28, True, datetime.datetime(1995, 5, 6)),
    (2, "akshay", 28, True, datetime.datetime(1995, 5, 6)),
    (3, "akshay", 28, True, datetime.datetime(1995, 5, 6)),
    (4, "akshay", 28, True, datetime.datetime(1995, 5, 6))
    ]

from pyspark.sql import Row
users_row_data = [Row(*user) for user in users]
users_schema = '''
                user_id int, 
                user_name string, 
                age int, 
                isCustomer boolean, 
                dob date
                '''
spark.createDataFrame(users_row_data, schema=users_schema).show()
