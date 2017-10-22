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
    