from pyspark.sql.functions import current_timestamp, udf
from pyspark.sql.types import StringType
import random
import string
import math
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import StringType, DoubleType, FloatType
from config import config



# Define a Python function to extract the timestamp part
def extract_timestamp_part(timestamp):
    # Customize this based on the part you want (e.g., 'yyyy-MM-dd HH:mm:ss')
    return timestamp.strftime('%Y-%m-%d %H:%M:%S')

# Create a UDF from the Python function
timestamp_udf = udf(extract_timestamp_part, StringType())

# Define a Python function to extract a substring
def substring_word(word, start_position, end_position):
    # Using Python string slicing to extract the substring
    return word[start_position - 1:end_position]

# Create a UDF from the Python function
substring_udf = udf(substring_word, StringType())

# Define the UDF function to create STDDEV
def calculate_stddev(values):
    n = len(values)
    avg_value = sum(values) / n
    squared_diff = [(x - avg_value) ** 2 for x in values]
    variance = sum(squared_diff) / n
    std_dev = math.sqrt(variance)
    return std_dev

# Define a UDF to extract the value of 'op_type_API' from the MDVariables section
def op_type_api():
    return config['MDVariables']['op_type_API'] 

# Generates a random string of a given length using ASCII letters
randomString_udf = udf(lambda length: ''.join(random.choice(string.ascii_letters) for _ in range(length)), StringType())

# Calculates the clustered value based on the input and the number of rows
clustered_udf = udf(lambda x, numRows: math.floor(x - 1) / numRows, DoubleType())

# Calculates the clustered value using column operations within the function
clustered2_udf = udf(lambda col, numRows: (col - 1) / numRows, DoubleType())

# Calculates the scattered value based on the input and the number of rows
scattered_udf = udf(lambda x, numRows: abs((x - 1) % numRows) * 1.0, DoubleType())

# Generates a random value based on the seed and the number of rows
randomised_udf = udf(lambda seed, numRows: abs(random.randint(0, numRows) % numRows) * 1.0, DoubleType())

# Generates a random value using column operations and a seed within the function
randomised2_udf = udf(lambda col, numRows: F.abs(F.randn(seed=None) * numRows), DoubleType())

# Pads a string with random characters based on the input and length
padString_udf = udf(lambda x, chars, length: ''.join(random.choice(chars) for _ in range(length - int(x))) + str(x), StringType())

# Pads a string with a sequence of characters based on the column, length, and characters
padString2_udf = udf(lambda col, length, chars: F.concat_ws("", F.expr("sequence(1, {})".format(length)), col.cast("string")), StringType())

# Pads a string with a single character for a given length
padSingleChar_udf = udf(lambda chars, length: ''.join(chars for _ in range(length)), StringType())

# Prints each element in a list
println_udf = udf(lambda lst: [print(ll[0]) for ll in lst], StringType())

# Register the stddev UDF
stddev_udf = udf(calculate_stddev, FloatType())
# example, calculate the standard deviation of "RANDOMISED" column
# std_udf = df.agg(stddev_udf(collect_list("RANDOMISED"))).alias("stddev").collect()[0][0]

# Register the UDF
op_type_api_udf = udf(op_type_api, StringType())

 
