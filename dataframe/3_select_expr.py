'''
Select - expr

@author: Puneetha B M
'''

def selectExpr_ex(sc, spark):
    print("select Expr - example")
    df = spark.range(200).selectExpr("id % 10 as a", "id % 30 as b", "rand() as c", "rand() as d")
    df.show(25, False)
    
if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .getOrCreate()
 
    sc = spark.sparkContext
 
    selectExpr_ex(sc, spark)