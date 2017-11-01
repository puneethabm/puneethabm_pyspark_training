'''
Instantiate spark

@author: Puneetha B M
'''

def get_spark_instance():
    print("Instantiate Spark") 
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .getOrCreate()
        
    sc = spark.sparkContext  
    return sc, spark

def set_spark_properties_instantiate():   
    print("Instantiate Spark with custom properties") 
    from pyspark.sql import SparkSession
    from pyspark.conf import SparkConf
    configuration = SparkConf()\
                    .set("spark.yarn.keytab", "puneetha.keytab")\
                    .set("spark.yarn.principal", "puneetha@PUNEETHA.COM")\
                    .set("spark.yarn.stagingDir", "/user/cloudera/tmp/staging/spark")
    spark = SparkSession.builder.config(conf=configuration).getOrCreate()
    print("Get all spark settings from spark instance")
    print(sc._conf.getAll())
    return spark
 
def get_or_set_spark_setting(sc, spark):
    print("Get all spark settings:")
    print(sc._conf.getAll())
    
    print("Set spark settings")
    sc._conf.set("spark.sql.parquet.compression.codec", "snappy")
    
    print("Get spark settings: {0}".format(sc._conf.get("spark.sql.parquet.compression.codec")))
    
if __name__ == '__main__':
    sc, spark = get_spark_instance()
    
    spark = set_spark_properties_instantiate()
    
    get_or_set_spark_setting(sc, spark)
