'''
monotonically_increasing_id

@author: Puneetha B M
'''

def create_dataframe_ex(sc, spark):
    headers = ("name", "age")
    data = [
         ("Puneetha" , 26),
         ("Bhoomika" , 23),
         ("James" , 21),
         ("Joe" , 19),
         ("Conor Ryan" , 20),
         ("Darragh" , 26),
         ("Alan", 31),
         ("Amit" , 32),
         ("Smitha" , 28),
         ("Alex" , 30),
         ("Denis", 29),
         ("Michal" , 34),
         ("John Mathew", 27),
         ("Jim Parker", 29),
         ("Sophia Ran", 25),
         ("Wendi Blake", 29),
         ("Stephan Lai", 32),
         ("Fay Van Damme", 22),
         ("Brevin Dice", 24),
         ("Regina Oleveria", 37),
         ("Rajat", 21),
         ("Sheetal", 32),
         ("James" , 21)
     ]
    
    df = spark.createDataFrame(data, headers)    
    return df

def monotonically_increasing_id_ex(sc, spark):
    """
    A column that generates monotonically increasing 64-bit integers.
    
    The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive. The current implementation puts the partition ID in the upper 31 bits, and the record number within each partition in the lower 33 bits. The assumption is that the data frame has less than 1 billion partitions, and each partition has less than 8 billion records.

    As an example, consider a DataFrame with two partitions, each with 3 records. This expression would return the following IDs: 0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.
    """
    print("monotonically_increasing_id - example")
    df = create_dataframe_ex(sc, spark)
    
    print("Dataframe before")
    df.show(25, False)
    
    print("Dataframe after")
    from pyspark.sql.functions import monotonically_increasing_id
    df = df.withColumn("id", monotonically_increasing_id())
    df.show(25, False)
    
if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .getOrCreate()

    sc = spark.sparkContext
    
    monotonically_increasing_id_ex(sc, spark)
    
