'''
Dataframes - Joins

@author: Puneetha B M
'''

def employee_dataframe_ex(sc, spark):
    headers = ("id", "name", "age" , "department_id")
    data = [
         (1, "Puneetha" , 26 , 1),
         (2, "Bhoomika" , 23 , 2),
         (3, "James" , 21 , 3),
         (4, "Joe" , 19, 4),
         (5, "Conor Ryan" , 20, 4),
         (6, "Darragh" , 26, 3),
         (7, "Alan", 31, 2),
         (8, "Amit" , 32, 1),
         (9, "Smitha" , 28, 1),
         (10, "Alex" , 30, 2),
         (11, "Denis", 29, 3),
         (12, "Michal" , 34, 4),
         (13, "John Mathew", 27, 2),
         (14, "Jim Parker", 29, 2),
         (15, "Sophia Ran", 25, 4),
         (16, "Wendi Blake", 29, 1),
         (17, "Stephan Lai", 32, 3),
         (18, "Fay Van Damme", 22, 2),
         (19, "Brevin Dice", 24, 2),
         (20, "Regina Oleveria", 37, 4),
         (21, "Rajat", 21, 1),
         (22, "Sheetal", 32, 2),
         (23, "James" , 21, 3),
         (23, "James" , 21, 4)
     ]
    
    df = spark.createDataFrame(data, headers)    
    return df

def department_dataframe_ex(sc, spark):
    headers = ("department_id", "name")
    data = [
         (1, "Depatment-A"),
         (2, "Depatment-B"),
         (3, "Depatment-C"),
         (4, "Depatment-D"),
         (5, "Department-E")
     ]
    
    df = spark.createDataFrame(data, headers)    
    return df

def join_ex(sc, spark):
    df_emp = employee_dataframe_ex(sc, spark)
    df_dept = department_dataframe_ex(sc, spark)
    
    print("Joins - example -- left outer join")    
    df_join = df_emp.join(df_dept, [df_emp.department_id == df_dept.department_id],
                         'left_outer')
    df_join.show(25, False)
    
    print("Joins - example -- inner join")    
    df_join = df_emp.join(df_dept, [df_emp.department_id == df_dept.department_id],
                         'inner')
    df_join.show(25, False)
    
    print("Joins - example -- right outer join")    
    df_join = df_emp.join(df_dept, [df_emp.department_id == df_dept.department_id],
                         'right_outer')
    df_join.show(25, False)
    
if __name__ == '__main__':
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .getOrCreate()
 
    sc = spark.sparkContext
 
    join_ex(sc, spark)
    
    