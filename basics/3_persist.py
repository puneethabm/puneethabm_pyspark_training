'''
Persist/Unpersist

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
         (23, "James" , 21),
         (23, "James" , 21)
     ]
    
    df = spark.createDataFrame(data, headers)    
    return df

def persist_ex(sc, spark):
    df = create_dataframe_ex(sc, spark)
    print("Persist - example")
    print("""Persist storage options:
            StorageLevel.DISK_ONLY 
                StorageLevel.DISK_ONLY_2 
                StorageLevel.MEMORY_ONLY 
                StorageLevel.MEMORY_ONLY_2 
                StorageLevel.MEMORY_AND_DISK 
                StorageLevel.MEMORY_AND_DISK_2 
                StorageLevel.OFF_HEAP
            """)
    
    print("Default Persist")
    df.persist() 
    
    from pyspark.storagelevel import StorageLevel
    print("Persist with StorageLevel")
    df.persist(StorageLevel.MEMORY_AND_DISK_2) 
    print("Get the RDD's current storage level:{0}".format(df.rdd.getStorageLevel()))
        
    print("Do all operations with dataframe...")    
    df.show(25, False)
    
    print("unpersist dataframe - to clean space")
    df.unpersist()
    
if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .getOrCreate()
        
    sc = spark.sparkContext    
    
    persist_ex(sc, spark)     