from pyspark.sql import SparkSession

 

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate() 
             
			 
    df_csv = spark.read.option('header',True).csv('users.csv')

    df_csv.show()

    df_csv.printSchema()

    df_csv.filter('name like "%ia"').show()

