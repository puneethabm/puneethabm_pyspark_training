'''
Checkpoints

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

def delete_hdfs_directory(sc, spark, input_hdfs_path):
    """
    Delete HDFS directory
    """
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
 
    fs = FileSystem.get(Configuration())
    if(fs.exists(Path(input_hdfs_path))):
            print("Deleting HDFS location=({0}) - STARTED ".format(input_hdfs_path))
            fs.delete(Path(input_hdfs_path), True)
            print("Deleting HDFS location=({0}) - DONE ".format(input_hdfs_path))
            
def create_checkpoint_ex(sc, spark):
    df = create_dataframe_ex(sc, spark)
    print("Creating checkpoint - example")
    
    checkpoint_dir = location_prefix + "my_local_check_point"
    print("Set checkpoint directory={0}".format(checkpoint_dir))
    
    sc.setCheckpointDir(checkpoint_dir)
    df = df.checkpoint(True)
       
    print("Check whether dataframe is checkpointed:{0}".format(df.rdd.isCheckpointed()))
    
    print("Get the name of the file to which this RDD was checkpointed:{0}".format(df.rdd.getCheckpointFile()))
     
    print("Do all operations with dataframe...")    
    df.show(25, False)
    
    print("Deleting the checkpoint")
    delete_hdfs_directory(sc, spark, checkpoint_dir)
    
    
if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .getOrCreate()
        
    sc = spark.sparkContext    
    
    create_checkpoint_ex(sc, spark)     