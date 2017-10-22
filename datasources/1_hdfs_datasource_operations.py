'''
HDFS - Data sources

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

def create_dataframe_ex_partition(sc, spark):
    headers = ("id", "name", "age", "year_of_join", "month_of_join")
    data = [
         (1, "Puneetha" , 26, 2007, 10),
         (2, "Bloomika" , 23, 2009, 12),
         (3, "James" , 21, 2015, 9),
         (4, "Joe" , 19, 2007 , 8),
         (5, "Conor Ryan" , 20, 2001, 7),
         (6, "Darragh" , 26, 2009, 4),
         (7, "Alan", 31, 2011, 1),
         (8, "Amit" , 32, 2007, 2),
         (9, "Smitha" , 28, 2013, 7),
         (10, "Alex" , 30, 2012, 9),
         (11, "Denis", 29, 2007, 7),
         (12, "Michal" , 34, 2007, 10),
         (13, "John Mathew", 27, 2010, 11),
         (14, "Jim Parker", 29, 2008, 8),
         (15, "Sophia Ran", 25, 2006, 5),
         (16, "Wendi Blake", 29, 2005, 1),
         (17, "Stephan Lai", 32, 2003, 9),
         (18, "Fay Van Damme", 22, 2001, 4),
         (19, "Brevin Dice", 24, 2002, 8),
         (20, "Regina Oleveria", 37, 2010, 11),
         (21, "Rajat", 21, 2007, 10),
         (22, "Sheetal", 32, 2002, 12),
         (23, "James" , 21, 2007, 9)
     ]
    
    df = spark.createDataFrame(data, headers)    
    return df

def read_from_hdfs_location(sc, spark):
    print("Read HDFS Location - CSV")
    df_csv = spark.read.format("csv")\
            .option("delimiter", "|")\
            .load(location_prefix + 'location_spark_created/csv_sample1/*')
    df_csv.show(5, False)
    
    print("Read HDFS Location - Parquet")
    df_parquet = spark.read.format("parquet")\
            .load(location_prefix + 'location_spark_created/parquet_sample1/*')
    df_parquet.show(5, False)
     
    print("Read HDFS Location - JSON")
    df_json = spark.read.format("json")\
            .load(location_prefix + 'location_spark_created/json_sample1/*')
    df_json.show(5, False)
    
    
def write_to_hdfs_location(sc, spark):
    df = create_dataframe_ex(sc, spark)
    print(".coalesce(1) is used to write output to 1 single file")
    
    print("Write HDFS Location - CSV")
    df.coalesce(1).write.format("csv") \
            .mode('overwrite') \
            .option("delimiter", "|") \
            .save(location_prefix + 'location_spark_created/csv_sample1/')
    
    print("Write HDFS Location - Parquet")
    df.coalesce(1).write.format("parquet") \
            .mode('overwrite') \
            .save(location_prefix + 'location_spark_created/parquet_sample1/')

    print("Write HDFS Location - Json")
    df.coalesce(1).write.format("json") \
            .mode('overwrite') \
            .save(location_prefix + 'location_spark_created/json_sample1/')


def write_to_hdfs_partition_location(sc, spark):
    df = create_dataframe_ex_partition(sc, spark)
    
    print("Write HDFS Location Partition - CSV")
    df.write.format("csv") \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .partitionBy("year_of_join", "month_of_join") \
            .save(location_prefix + 'location_spark_created/csv_partition_sample')
    
    print("Write HDFS Location Partition - Parquet")
    df.write.format("parquet") \
            .mode("overwrite") \
            .partitionBy("year_of_join", "month_of_join") \
            .save(location_prefix + 'location_spark_created/parquet_partition_sample')

    print("Write HDFS Location Partition - Json")
    df.write.format("json") \
            .mode("overwrite") \
            .partitionBy("year_of_join", "month_of_join") \
            .save(location_prefix + 'location_spark_created/parquet_partition_sample')


def add_input_file_name_to_df_ex(sc, spark):
    df = spark.read.format("csv")\
            .option("delimiter", "|")\
            .load(location_prefix + 'location_spark_created/csv_sample1/*')
    
    from pyspark.sql.functions import input_file_name
    print("Adding source input filename/location")
    df = df.withColumn("source_filename", input_file_name())
    df.show(25, False)
    
def read_whole_file_content_to_df(sc, spark):
    print("Read Whole file into a dataframe")
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType, IntegerType, ArrayType, LongType
    
    rdd_full = sc.wholeTextFiles(location_prefix + "raw_data/wordcount_input/*")
    
    schema = StructType([
                     StructField("source_filename", StringType(), True)
                     ,StructField("whole_file_content", StringType(), True)
                     ])
     
    file_content_df = spark.createDataFrame(rdd_full, schema)
    print("Whole file content - dtypes = {0}".format(file_content_df.dtypes))
    file_content_df.show(5, False)


def get_filenames_hdfs_directory(sc, spark, input_hdfs_path, recursive=True):
    """
    List HDFS directory
    """    
    print("Getting filenames HDFS location=({0})".format(input_hdfs_path))
    
    Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration    
    
    fs = FileSystem.get(Configuration())
    
    status = fs.globStatus(Path(input_hdfs_path))
    filenames = []
    
    for fileStatus in status:
        filenames.append(fileStatus.getPath())
    
    print("Filenames HDFS location=({0}) are = {1} "
            .format(input_hdfs_path, ", ".join([str(a) for a in filenames])))

    return filenames

def get_number_of_files(sc, spark, input_hdfs_path):
    """
    Get number of files matching the pattern
    """    
    filenames = get_filenames_hdfs_directory(sc, spark, input_hdfs_path, True)
    filenames = list([str(a) for a in filenames])
 
    numer_of_files = len(filenames) 
    print("Number of input files={0} for location={1}".format(numer_of_files , input_hdfs_path))
    
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
                
if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .getOrCreate()
 
    sc = spark.sparkContext
 
    print("Datasource operations")
    write_to_hdfs_location(sc, spark)
       
    write_to_hdfs_partition_location(sc, spark)
      
    read_from_hdfs_location(sc, spark)
     
    add_input_file_name_to_df_ex(sc, spark)
      
    read_whole_file_content_to_df(sc, spark)
  
    input_hdfs_path = location_prefix + "raw_data/wordcount_input/*"
    get_filenames_hdfs_directory(sc, spark, input_hdfs_path)

    input_hdfs_path = location_prefix + "raw_data/wordcount_input/*"
    get_number_of_files(sc, spark, input_hdfs_path)
      
    input_hdfs_path = location_prefix + "location_spark_created/csv_partition_sample"
    delete_hdfs_directory(sc, spark, input_hdfs_path)
    