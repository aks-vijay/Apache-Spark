## List
names = ['Scott', 'Donald', 'Micky']

from pyspark.sql.types import StringType
spark.createDataFrame(names, StringType()).show()

# list of tuple with one element - syntax
ages_lists = [(21, ), (22, ), (23, ), (24,)]
spark.createDataFrame(ages_lists, 'age int').show()

# list of tuple with two elements - syntax
users_lists = [(1, 'Scott'), (2, 'Mark'), (3, 'Rob'), (4, 'Kevin')]
df = spark.createDataFrame(users_lists, 'user_id int, username string').show()

# dataframe is a collection of row objects
df.show()

# dataframe - Converted to list using Collect()
df.collect()

# varying list of arguments using Row - args
from pyspark.sql import Row
row1 = Row(1, 2)

#Row
# varying list of arguments using Row - args
from pyspark.sql import Row
row1 = Row(1, 2)

# varying list of key word arguments - kwargs
row2 = Row(name="Akshay", age=1)

# list of list
# converting list of lists using a Row
users_lists = [[1, "Mark"], [2, "Mike"], [3, "Blessie"], [4, "John"]]
spark.createDataFrame(users_lists, 'user_id int, user_name string').show()

# varying list of key word arguments - kwargs
row2 = Row(name="Akshay", age=1)

# converting list of list to list of rows
users_row = []
for user in users_lists:
    users_row.append(Row(*user))

# with list comprehension
# newlist = [expression(item) for item in iterable if condition == True]
users_row = [Row(*user) for user in users_lists]
users_row

# converting the list of row to a dataframe
spark.createDataFrame(users_row, 'user_id int, user_name string').show()

### Converting list of tuples directly to spark dataframe
users_lists = [(1, "Scot"), (2, "Tony"), (3, "Leo"), (4, "Harold")]
spark.createDataFrame(users_lists, 'user_id int, user_name string').show()

### converting list of list to list of rows and then to spark dataframe
from pyspark.sql import Row
users_row = [Row(*user) for user in users_lists]
spark.createDataFrame(users_row, 'user_id int, user_name string').show()

### list of dictionary -> Converted directly to Spark DF
users_lists = [
    {"user_id": 1, "user_name": "Mark"},
    {"user_id": 2, "user_name": "Joe"},
    {"user_id": 3, "user_name": "Leo"},
    {"user_id": 4, "user_name": "Das"},
]
spark.createDataFrame(users_lists, 'user_id int, user_name string').show()

### list of dictionary -> Converted to List of Rows -> Converted to Spark Df
from pyspark.sql import Row
users_row = [Row(**user) for user in users_lists]
spark.createDataFrame(users_row, 'user_id int, user_name string').show()
