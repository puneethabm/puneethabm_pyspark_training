'''
Hive - queries

@author: Puneetha B M
'''


def create_dataframe_ex(sc, spark):
    headers = ("id", "name", "age")
    data = [
         (1, "Puneetha" , 26),
         (2, "Bhoomika" , 23),
         (3, "James" , 21),
         (4, "Joe" , 19),
         (5, "Conor Ryan" , 20),
         (6, "Darragh" , 26),
         (7, "Alan", 31),
         (8, "Amit" , 32),
         (9, "Smitha" , 28),
         (10, "Alex" , 30),
         (11, "Denis", 29),
         (12, "Michal" , 34),
         (13, "John Mathew", 27),
         (14, "Jim Parker", 29),
         (15, "Sophia Ran", 25),
         (16, "Wendi Blake", 29),
         (17, "Stephan Lai", 32),
         (18, "Fay Van Damme", 22),
         (19, "Brevin Dice", 24),
         (20, "Regina Oleveria", 37),
         (21, "Rajat", 21),
         (22, "Sheetal", 32),
         (23, "James" , 21)
     ]
    
    df = spark.createDataFrame(data, headers)    
    return df



def create_temp_table_ex(sc, spark):
    df = create_dataframe_ex(sc, spark)
    df.show(25, False)
    
    print("Creating temp table view - example")    
    tempTableName = "my_temp_table"
    print("Temp table view name={0}".format(tempTableName))
    df.createOrReplaceTempView(tempTableName)
    
    print("New we can use dataframe similar to hive tables")        
    df = spark.sql("SELECT * FROM {0} WHERE age=25".format(tempTableName))
    df.show(25, False)
    
    print("Dropping temp view={0}".format(tempTableName))
    spark.catalog.dropTempView(tempTableName)
            
if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .getOrCreate()

    sc = spark.sparkContext
    
    create_temp_table_ex(sc, spark)
    
