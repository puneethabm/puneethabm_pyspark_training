'''
Utilities - General

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

def spark_version(sc, spark):
    print("---------------------------------------------------------------")
    print("Print Spark version:" , sc.version)
    print("---------------------------------------------------------------")
   
def python_version(sc, spark):
    print("---------------------------------------------------------------")
    print("Print Python version:" , sc.pythonVer)
    print("---------------------------------------------------------------")
    
def print_master(sc, spark):
    print("---------------------------------------------------------------")
    print("Print Master:" , sc.master)
    print("---------------------------------------------------------------")
    
def print_appName(sc, spark):
    print("---------------------------------------------------------------")
    print("Print appName:" , sc.appName)
    print("---------------------------------------------------------------")
    
def print_application_id(sc, spark):
    print("---------------------------------------------------------------")
    print("ApplicationID:" , sc.applicationId)
    print("---------------------------------------------------------------")
    
def print_sparkHome(sc, spark):
    print("---------------------------------------------------------------")
    print("Print sparkHome:" , sc.sparkHome)
    print("---------------------------------------------------------------")

def print_sparkUser(sc, spark):
    print("---------------------------------------------------------------")
    print("Print sparkUser:" , sc.sparkUser())
    print("---------------------------------------------------------------")
    
def print_defaultParallelism (sc, spark):
    print("---------------------------------------------------------------")
    print("Print default level of parallelism  :" , sc.defaultParallelism)
    print("---------------------------------------------------------------")
    
    
def print_defaultMinPartitions(sc, spark):
    print("---------------------------------------------------------------")
    print("Print Default minimum number of partitions for RDDs:" , sc.defaultMinPartitions)
    print("---------------------------------------------------------------")

def get_job_time(sc, spark):
    print("---------------------------------------------------------------")
    print("Print job time")
    print("---------------------------------------------------------------")
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    
    start_time = datetime.now()
    print("Start time: {0}".format(start_time.strftime('%Y/%m/%d %H:%M:%S')))
    
    end_time = datetime.now()
    print("End time: {0}".format(end_time.strftime('%Y/%m/%d %H:%M:%S')))
    
    diff = relativedelta(end_time, start_time)
    print("Time taken is {0} days {1} hours {2} minutes {3} seconds".format(diff.days, diff.hours, diff.minutes, diff.seconds))
    

def set_log_level(sc, spark):
    print("How to set log level")
    sc.setLogLevel('ERROR')

def get_or_set_spark_setting(sc, spark):
    print("Get all spark settings:")
    print(sc._conf.getAll())
    
    print("Set spark settings")
    sc._conf.set("spark.sql.parquet.compression.codec", "snappy")
    
    print("Get spark settings: {0}".format(sc._conf.get("spark.sql.parquet.compression.codec")))

    
def invoke_python_file(sc, spark):
    print("---------------------------------------------------------------")
    print("Invoke Python file. Run other python script inside pyspark")
    print("---------------------------------------------------------------")
    print('exec(open("/home/cloudera/bin/hive_udfs_mapping.py").read())')
#     exec(open("/home/cloudera/bin/hive_udfs_mapping.py").read())

def add_egg_zip_file_ex(sc, spark):
    print("---------------------------------------------------------------")
    print("Add a .py or .zip dependency file.The 'path' passed can be either a local file (or) a file in HDFS")
    print("---------------------------------------------------------------")
    print('sc.addPyFile("/home/cloudera/bin/custom_python_framework.egg")')
#     sc.addPyFile("/home/cloudera/bin/custom_python_framework.egg")
  
def get_element_type(input):
    from pyspark.sql import DataFrame
    from pyspark.rdd import RDD

    if isinstance(input, RDD):
        return "RDD"
    if isinstance(input, DataFrame):
        return "DataFrame"
    return "Unknown"


if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .getOrCreate()
        
    sc = spark.sparkContext    
    
    spark_version(sc, spark)
    
    python_version(sc, spark)
    
    print_master(sc, spark)
    
    print_appName(sc, spark)
    
    print_application_id(sc, spark)    
    
    print_sparkHome(sc, spark)
    
    print_sparkUser(sc, spark)
    
    print_defaultParallelism (sc, spark)
    
    print_defaultMinPartitions(sc, spark)  

    get_job_time(sc, spark)
    
    set_log_level(sc, spark)
    
    get_or_set_spark_setting(sc, spark)
    
    invoke_python_file(sc, spark)

    df = create_dataframe_ex(sc, spark)
    print("Checking whether the element is a dataframe: {0}".format(get_element_type(df)))
    print("Print type of dataframe element: {0}".format(type(df).__name__))
    
    distData = sc.parallelize([1, 2, 3, 4, 5])
#     distData.setName("distData")
    distData = distData.persist()
    print("Checking whether the element is a RDD: {0}".format(get_element_type(distData)))
    print("Print type of RDD element: {0}".format(type(distData).__name__))
    
    