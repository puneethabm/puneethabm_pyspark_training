'''
RDDs
A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable, partitioned collection of elements that can be operated on in parallel.

@author: Puneetha B M
'''

# location_prefix = "/tmp/"
location_prefix = ""

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

def rdd_getStorageLevel_ex(sc, spark):
    distRdd = sc.parallelize([1, 2, 3, 4, 5])
    
    from pyspark.storagelevel import StorageLevel
    print("Persist with StorageLevel")
    distRdd = distRdd.persist(StorageLevel.MEMORY_AND_DISK_2)
    
    print("Get the RDD's current storage level:{0}".format(distRdd.getStorageLevel()))
     
def rdd_getCheckpointFile_ex(sc, spark):
    distRdd = sc.parallelize([1, 2, 3, 4, 5])
    
    checkpoint_dir = location_prefix + "my_local_check_point"
    print("Set checkpoint directory={0}".format(checkpoint_dir))
     
    sc.setCheckpointDir(checkpoint_dir)
#     distRdd = distRdd.checkpoint(True)
      
    print("Get the name of the file to which this RDD was checkpointed:{0}".format(distRdd.getCheckpointFile()))
      
    print("Do all operations with Rdd Operations...")    
    print(distRdd.take(5))
    
def list_rdds(sc, spark):
    print("List all RDDs")
    from pyspark import RDD
    return [k for (k, v) in globals().items() if isinstance(v, RDD)]

def list_persistent_rdds(sc, spark):
    print("List Persistent RDDs")
    from pyspark import RDD
    return [id for (id, rdd) in spark.sparkContext._jsc.getPersistentRDDs().items()]

def unpersist_all_rdds(sc, spark):
    print("Unpersist all RDDs")
    for (id, rdd) in spark.sparkContext._jsc.getPersistentRDDs().items():
        rdd.unpersist()


def create_empty_rdd(sc, spark):
    myRdd = sc.emptyRDD()
    
    print("Print empty RDD:{0}".format(myRdd.collect()))

   
def read_rdd_newAPIHadoopFile_ex(sc, spark):
    """
        Read a 'new API' Hadoop InputFormat with arbitrary key and value class from HDFS,
        a local file system (available on all nodes), or any Hadoop-supported file system URI.
        The mechanism is the same as for sc.sequenceFile.
    """
    hdfs_path = "raw_data/csv_sample"
    inputFormatClass = "org.apache.hadoop.mapreduce.lib.input.TextInputFormat"
    keyClass = "org.apache.hadoop.io.Text"
    valueClass = "org.apache.hadoop.io.LongWritable"
    myRdd = sc.newAPIHadoopFile(hdfs_path
                        , inputFormatClass
                        , keyClass
                        , valueClass
                        )
    print("Reading from newAPIHadoopFile:{0}".format(myRdd.take(5)))
  
# TODO: need to fix this. Its not working  
# def read_rdd_newAPIHadoopRDD_ex(sc, spark):
#     """
#         Read a 'new API' Hadoop InputFormat with arbitrary key and value class, from an arbitrary
#         Hadoop configuration, which is passed in as a Python dict.
#         This will be converted into a Configuration in Java.
#         The mechanism is the same as for sc.sequenceFile.
#     """
#     data = [1, 2, 3, 4, 5]
#     hdfs_path = "location_csv_table1"
#     
#     inputFormatClass = "org.apache.hadoop.mapreduce.lib.input.TextInputFormat"
#     keyClass = "org.apache.hadoop.io.Text"
#     valueClass = "org.apache.hadoop.io.LongWritable"    
#     conf = {"input" : hdfs_path}
#     
#     # Connect to database, instead of below:
#     myRdd = sc.newAPIHadoopRDD( inputFormatClass
#                         , keyClass
#                         , valueClass
#                         , conf=conf
#                         )
# 
#     print("Reading from newAPIHadoopRDD:{0}".format(myRdd.take(5)))


def runJob_ex(sc, spark):
    """
        Executes the given partitionFunc on the specified set of partitions,
        returning the result as an array of elements.
    
        If 'partitions' is not specified, this will run over all partitions.
    """
    myRDD = sc.parallelize(range(6), 3)
    print("Full RDD={0}".format(myRDD.collect()))
    myList = sc.runJob(myRDD, lambda part: [x * x for x in part])
    print("runJob - example 1 - All partitions={0}".format(myList))

    myRDD = sc.parallelize(range(6), 3)
    print("Full RDD={0}".format(myRDD.collect()))
    myList = sc.runJob(myRDD, lambda part: [x * x for x in part], [0, 2], True)        
    print("runJob - example 2 - Specific partitions i.e. partitions 0 and 2 and skip partition 1 ={0}".format(myList))
    
if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .getOrCreate()

    sc = spark.sparkContext    
    
    distData = sc.parallelize([1, 2, 3, 4, 5])
    distData = distData.persist()    
   
    distData1 = sc.parallelize([1, 2, 3, 4, 5])
    print(list_rdds(sc, spark))
    
    print(list_persistent_rdds(sc, spark))
    
    unpersist_all_rdds(sc, spark)
    print(list_persistent_rdds(sc, spark))
        
    rdd_getStorageLevel_ex(sc, spark)
    
    rdd_getCheckpointFile_ex(sc, spark)
        
    create_empty_rdd(sc, spark)
    
    read_rdd_newAPIHadoopFile_ex(sc, spark)
    
#     read_rdd_newAPIHadoopRDD_ex(sc, spark)

    runJob_ex(sc, spark)
    
    
    