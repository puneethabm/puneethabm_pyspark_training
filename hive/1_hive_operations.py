'''
Hive - operations

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


def write_to_hive(sc, spark):
    """
    Template:
        mode = "append", "overwrite"  -> default "overwrite"
        storage_format =  "csv", "parquet", "json"
        
    In production:
    hdfs_location = "hdfs://nameservices-logical-name/project/project_name/table_name"
    table_name = "default"
    """
    df = create_dataframe_ex(sc, spark)
        
    print("Write to Hive - CSV")    
    hdfs_location = location_prefix + "location_csv_table1"
    table_name = "table_csv_name1"
    df.write.mode("overwrite") \
            .format("csv") \
            .option("delimiter", "|") \
            .option("path", hdfs_location) \
            .saveAsTable(table_name)

    print("Write to Hive - Parquet")   
    hdfs_location = location_prefix + "location_parquet_table1"
    table_name = "table_parquet_name1"
    df.write.mode("overwrite") \
            .format("parquet") \
            .option("path", hdfs_location) \
            .saveAsTable(table_name)
            
    print("Write to Hive - JSON")    
    hdfs_location = location_prefix + "location_json_table1"
    table_name = "table_json_name1"
    df.write.mode("overwrite") \
            .format("json") \
            .option("path", hdfs_location) \
            .saveAsTable(table_name)

def write_to_partitions_hive(sc, spark):
    df = create_dataframe_ex_partition(sc, spark)
    
    print("Write to Hive Partition - CSV")    
    hdfs_location = location_prefix + "location_csv_partition_table1"
    table_name = "table_csv_partition_name1"
    df.write.mode("overwrite") \
            .format("csv") \
            .option("delimiter", "|") \
            .partitionBy("year_of_join", "month_of_join") \
            .option("path", hdfs_location) \
            .saveAsTable(table_name)

    print("Write to Hive - Partition Parquet")    
    hdfs_location = location_prefix + "location_parquet_partition_table1"
    table_name = "table_parquet_partition_name1"
    df.write.mode("overwrite") \
            .format("parquet") \
            .partitionBy("year_of_join", "month_of_join") \
            .option("path", hdfs_location) \
            .saveAsTable(table_name)
            
    print("Write to Hive Partition - JSON")    
    hdfs_location = location_prefix + "location_json_partition_table1"
    table_name = "table_json_partition_name1"
    df.write.mode("overwrite") \
            .format("json") \
            .partitionBy("year_of_join", "month_of_join") \
            .option("path", hdfs_location) \
            .saveAsTable(table_name)


def read_from_hive(sc, spark):
    print("Read from Hive - CSV")  
    table_name = "table_csv_name1"      
    df_csv = spark.read.format("csv") \
            .option("delimiter", "|") \
            .table( table_name)
    df_csv.show(5, False)   
    
    print("Read from Hive - Parquet")  
    table_name = "table_parquet_name1"    
    df_parquet = spark.read.format("parquet") \
            .table( table_name)
    df_parquet.show(5, False)
    
    
    print("Read from Hive - JSON")  
    table_name = "table_json_name1"    
    df_json = spark.read.format("json") \
            .table( table_name)
    df_json.show(5, False)

def read_from_hive_with_partitions(sc, spark):
    print("Read from Hive Partition - CSV")  
    table_name = "table_csv_partition_name1"    
    df_csv = spark.read.format("csv") \
            .option("delimiter", "|") \
            .table( table_name)
    df_csv.show(5, False)   
    
    print("Read from Hive Partition - Parquet")  
    table_name = "table_parquet_partition_name1"    
    df_parquet = spark.read.format("parquet") \
            .table( table_name)
    df_parquet.show(5, False)
    
    
    print("Read from Hive Partition - JSON")  
    table_name = "table_json_partition_name1"    
    df_json = spark.read.format("json") \
            .table( table_name)
    df_json.show(5, False)


if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .getOrCreate()

    sc = spark.sparkContext
 
    print("Hive - operations")
    write_to_hive(sc, spark)
    
    read_from_hive(sc, spark)
     
    write_to_partitions_hive(sc, spark)
     
    read_from_hive_with_partitions(sc, spark)
    
    
    
    