'''
RDDs

@author: Puneetha B M
'''

def rdd_read_parallelize_ex(sc, spark):
    data1 = [
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
    
    rdd1 = sc.parallelize(data1)
    print(rdd1.take(5))
    
        
if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .getOrCreate()
        
    sc = spark.sparkContext
    
    rdd_read_parallelize_ex(sc, spark)
    
    
    