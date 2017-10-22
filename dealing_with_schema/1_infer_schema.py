'''
HDFS - Data sources

@author: Puneetha B M
'''

# location_prefix = "/tmp/"
location_prefix = ""

def read_from_hdfs_location_with_schema(sc, spark):
    print("Read HDFS Location - Load with schema - CSV")
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    schema = StructType([
                     StructField("id", StringType(), True)
                     ,StructField("name", StringType(), True)
                     ])

    df_csv = spark.read.format("csv")\
            .option("delimiter", "\t")\
            .schema(schema)\
            .load(location_prefix + 'raw_data/csv_sample/*')
    df_csv.show(5, False)
    
                
if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .getOrCreate()
 
    sc = spark.sparkContext
 
    print("Dealing with schemas")
    read_from_hdfs_location_with_schema(sc, spark)
