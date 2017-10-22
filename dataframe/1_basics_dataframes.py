'''
Main - test script

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


def print_dataframe_ex(sc, spark):
    df = create_dataframe_ex(sc, spark)
    
    print("Prints first 20 rows by default and truncates the characters. By default truncating is set to True") 
    df.show()

    print("Prints first 20 rows by default. We set truncate to False so that the characters are not truncated") 
    df.show(truncate=False)

    print("Prints first 20 rows by default. We set truncate to 4. it will only display first 4 characters in each column") 
    df.show(truncate=4)
    
    print("Prints first 5 rows. We set it to False so that the characters are not truncated") 
    df.show(5, False)    
    
    print("Prints all rows. We set it to False so that the characters are not truncated") 
    df.show(df.count(), False)

def print_number_of_partitions_df_ex(sc, spark):
    df = create_dataframe_ex(sc, spark)
    
    print("To get number of partitions of the dataframe")
    print(df.rdd.getNumPartitions())
    
def convert_dataframe_to_rdd_ex(sc, spark):
    df = create_dataframe_ex(sc, spark)
    
    print("Convert dataframe to RDD")
    df.rdd
    
def print_schema_of_dataframe(sc, spark):
    df = create_dataframe_ex(sc, spark)
    
    print("Print schema of the dataframe - printSchema")
    df.printSchema()

    print("Print schema of the dataframe - dtypes")
    print(df.dtypes)
    
    print("Print schema of the dataframe - columns")
    print(df.columns)

    print("Print schema of the dataframe - describe - This evaluates full dataset. Don't use this, unless its necessary!")
    print(df.describe())
    
    
def select_dataframe_ex(sc, spark):
    from pyspark.sql.functions import col
    
    print("Select specific columns to display")
    df = create_dataframe_ex(sc, spark)
    df.select(col("name")).show(5, False)
    
    df.select(col("name") , col("age")).show(5, False)

    print("Print without using col function")
    df.select("name").show(5, False)


def filter_ex(sc, spark):
    df = create_dataframe_ex(sc, spark)
    
    from pyspark.sql.functions import col
    
    print("Print filtered results - example - 1")
    df.filter(col("age") > 21).show()
    
    print("Print filtered results - example - 2")
    df_fil = df.filter(df.age> 21)
    df_fil.show()
    
    print("Print filtered results - example - 3")
    df_fil = df.filter("name RLIKE '.*Puneetha.*' OR name RLIKE '.*James.*'")
    df_fil.show(5, False)
    
    print("Print filtered results - between - example - 1")
    df_fil = df.filter(col("age").between(int(19) , int(23)))
    df_fil.show(5, False)
    
    print("Print filtered results - between - example - 2")
    df_fil = df.filter(df.age.between(int(19) , int(23)))
    df_fil.show(5, False)

    
def group_by_and_agg_ex(sc, spark):
    df = create_dataframe_ex(sc, spark)
    
    from pyspark.sql.functions import col, count
    print("Print group by results - example - 1")
    df_grp = df.groupby(col("age")).count()
    df_grp.show(5, False)
    
    print("Print group by results - example - 2")
    df_grp = df.groupby("name", "age").agg(count('*').alias("record_count"))
    df_grp = df_grp.sort(col("record_count").desc())
    df_grp.show(25, False)

def distinct_ex(sc, spark):
    df = create_dataframe_ex(sc, spark)
    
    from pyspark.sql.functions import col
    print("Print distinct by results  - example 1")
    
    df_distinct = df.select(col("name"), col("age")).distinct()
    df_distinct.show(25, False)
    
    print("Print distinct by results  - example 2")
    df_distinct = df.select(df.name, df.age).distinct()
    df_distinct.show(25, False)
    
    print("Print distinct by results  - example 3")
    df_distinct = df.dropDuplicates()
    df_distinct.show(25, False)
 
def sort_by_order_by_ex(sc, spark):
    df = create_dataframe_ex(sc, spark)
    from pyspark.sql.functions import col
    
    print("Sort by results  - example 1 - asc")
    df_sort = df.sort(col("id"))
    df_sort.show(25, False)
    
    print("Sort by results  - example 2 - desc")
    df_sort = df.sort(col("id").desc())
    df_sort.show(25, False)
    
    print("Order by results  - example 1")
    df_sort = df.orderBy(col("id"))
    df_sort.show(25, False)


def drop_columns_dataframe_ex(sc, spark):
    df = create_dataframe_ex(sc, spark)
    
    print("Dropping columns")
    columns_to_drop = ["id", "age"]
    df_sel = df.select([column for column in df.columns if column not in columns_to_drop])
    df_sel.show(25, False)

def columns_to_keep_dataframe_ex(sc, spark):
    df = create_dataframe_ex(sc, spark)
    
    print("Select specific column list/ Keep specific columns")
    columns_to_keep = ["id", "name"]
    df_sel = df.select([column for column in df.columns if column in columns_to_keep])
    df_sel.show(25, False)

def add_static_field_to_df_ex(sc, spark):
    df = create_dataframe_ex(sc, spark)
    
    print("Add static information to the dataframe")
    from pyspark.sql.functions import lit
    df = df.withColumn("employee_batch_number", lit("1"))
    df.show(25, False)
    
def manipulate_multiple_columns(sc, spark):
    df = create_dataframe_ex(sc, spark)
    
    print("Manipulate a few columns in the one go")
    print("In this example. We are pre-fixing 'mycompany_employee_' for all 2 new columns")
    from pyspark.sql.functions import col, lit, concat
    from functools import reduce
    names = ["one", "two"]
    new_df = reduce(lambda new_df, idx: new_df.withColumn("id_inc_" + names[idx], concat(lit("mycompany_employee_") , col("name"))), 
     range(len(names)), 
     df)
    new_df.show(25, False)

def debugging_dataframe(sc, spark):
    df = create_dataframe_ex(sc, spark)
    
    print("Print schema of the dataframe - explain")
    print(df.explain())
    

if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .getOrCreate()
 
    sc = spark.sparkContext
 
    print("Dataframe operations")
    print_dataframe_ex(sc, spark)
            
    print_schema_of_dataframe(sc, spark)
            
    print_number_of_partitions_df_ex(sc, spark)
        
    convert_dataframe_to_rdd_ex(sc, spark)
       
    debugging_dataframe(sc, spark)
             
    select_dataframe_ex(sc, spark)
             
    filter_ex(sc, spark)
            
    group_by_and_agg_ex(sc, spark)
           
    distinct_ex(sc, spark)
      
    sort_by_order_by_ex(sc, spark)
           
    drop_columns_dataframe_ex(sc, spark)
       
    columns_to_keep_dataframe_ex(sc, spark)
     
    add_static_field_to_df_ex(sc, spark)
     
    manipulate_multiple_columns(sc, spark)