import random
import string
import math
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Generates a random string of a given length using ASCII letters
def randomString(length):
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

# Calculates the clustered value based on the input and the number of rows
def clustered(x, numRows):
    return math.floor(x - 1) / numRows

# Calculates the clustered value using column operations within the function
def clustered2(col, numRows):
    return (col - 1) / numRows

# Calculates the scattered value based on the input and the number of rows
def scattered(x, numRows):
    return abs((x - 1) % numRows) * 1.0

# Generates a random value based on the seed and the number of rows
def randomised(seed, numRows):
    random.seed(seed)
    return abs(random.randint(0, numRows) % numRows) * 1.0

# Generates a random value using column operations and a seed within the function
def randomised2(col, numRows):
    return F.abs(F.randn(seed=None) * numRows)

# Pads a string with random characters based on the input and length
def padString(x, chars, length):
    n = int(x) + 1
    result_str = ''.join(random.choice(chars) for i in range(length - n)) + str(x)
    return result_str

# Pads a string with a sequence of characters based on the column, length, and characters
def padString2(col, length, chars):
    result_str = F.concat_ws(
        "",
        F.expr("sequence(1, {})".format(length)),
        col.cast("string")
    )
    return result_str

# Pads a string with a single character for a given length
def padSingleChar(chars, length):
    result_str = ''.join(chars for i in range(length))
    return result_str

# Prints each element in a list
def println(lst):
    for ll in lst:
        print(ll[0])


