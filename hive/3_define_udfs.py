'''
Definin UDFs

@author: Puneetha B M
'''
    
def create_dataframe_ex(sc, spark):
    headers = ("id", "name", "age", "emp_email")
    data = [
         (1, "Puneetha" , 26, "Puneetha <puneetha@gmail.com>"),
         (2, "Bhoomika" , 23, "Bhoomika <bhoomika@gmail.com>"),
         (3, "James" , 21, "James <james@gmail.com>"),
         (4, "Joe" , 19, "Joe <joe@gmail.com>"),
         (5, "Conor Ryan" , 20, "Conor, Ryan <conor.ryan@gmail.com>"),
         (6, "Darragh" , 26, "Darragh <darragh@gmail.com>"),
         (7, "Alan", 31, "Alan <alan@yahoo.com>"),
         (8, "Amit" , 32, "Amit <amit@yahoo.co.in>"),
         (9, "Smitha" , 28, "Smitha <smitha@gmail.com>"),
         (10, "Alex" , 30, "Alex <alex@gmail.com>"),
         (11, "Denis", 29, "Denis <denis@gmail.co.in>"),
         (12, "Michal" , 34, "Michal <michal@gmail.com>"),
         (13, "John Mathew", 27, "John Mathew <jon.mathew@gmail.com>"),
         (14, "Jim Parker", 29, "Jim Parker <jim.parker@gmail.com>"),
         (15, "Sophia Ran", 25, "Sophia Ran <sophia.ran@gmail.com>"),
         (16, "Wendi Blake", 29, "Wendi Blake <wendi.blake@gmail.com>"),
         (17, "Stephan Lai", 32, "Stephan Lai <stephan.lai@yahoo.com>"),
         (18, "Fay Van Damme", 22, "Fay Van Damme <fay_van.damme@gmail.com>"),
         (19, "Brevin Dice", 24, "Brevin Dice <brevin.dice@gmail.com>"),
         (20, "Regina Oleveria", 37, "Regina Oleveria <regina_oleveria@yahoo.com>"),
         (21, "Rajat", 21, "Rajat <rajat@gmail.com>"),
         (22, "Sheetal", 32, "Sheetal <sheetal@gmail.com>"),
         (23, "James" , 21, "James <james@gmail.com>")
     ]
    
    df = spark.createDataFrame(data, headers)    
    return df  


def extract_email_address(variable):
    """
    Extract all email addresses from an array
    Remove aliases: Ex: ABC XYZ <abc-xyz@gmail.com>   -> abc-xyz@gmail.com
    Note: The regex used here covers almost all generic email addresses and has been created in accordance with RFC-5322.
    However, it isn't perfect, thus for a more robust regex, more work on below regex will be required.
    :param variable: A string or an array of strings
    :return: Array of strings, email addresses only. If no valid email IDs are present, it returns an empty array or list.
    """
    import re
    try:
        patt = r"[\w!#$%&*+/=?^_`{|}~-]+(?:\.[\w!#$%&*+/=?^_`{|}~-]+)*@(?:[\w](?:[\w-]*[\w])?\.)+[\w](?:[\w-]*[\w])?"
        return_value = re.findall(patt, str(variable).upper())
    except Exception as e:
        print(e)
        return_value = variable
    return return_value

def dedup(variable):
    """
    Remove duplicates from a list of strings
    :param variable: Array of strings
    :return: Array of strings, unique values only.
    """
    try:
        if isinstance(variable, list):
            return_value = sorted(list(set([x.upper() for x in variable])))
        else:
            return_value = [variable]
    except Exception as e:
        print(e)
        return_value = [variable]
    return return_value

def retain_only_alphanumeric_chars(x, separator=" "):
    """
    Remove all characters except alphanumeric characters ([A-Za-z0-9])
    :param variable: string
    :return: alphanumeric string
    """
    import re
    try:
        return_value = re.sub(r'[^A-Za-z0-9]', separator, x)
    except Exception as e:
        print(e)
        return_value = ""
    return return_value


if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .getOrCreate()

    sc = spark.sparkContext
    df = create_dataframe_ex(sc, spark)
    df.show(25, False)
    
    print("Create Spark - UDF mapping")
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType, IntegerType, ArrayType, LongType
    
    #udf_name = udf(python_function , return_type)
    udf_extract_email_address = udf(extract_email_address, ArrayType(StringType()))
    udf_dedup  = udf(dedup, ArrayType(StringType()))
    udf_retain_only_alphanumeric_chars = udf(retain_only_alphanumeric_chars, StringType())    
    
    print("Using UDFs")
    from pyspark.sql.functions import col
    df = df.withColumn("email_address", udf_extract_email_address(col("emp_email")))
    df = df.withColumn("email_alpha_chars_only", udf_retain_only_alphanumeric_chars(col("emp_email")))
    df.show(25, False)
    
    
    

    