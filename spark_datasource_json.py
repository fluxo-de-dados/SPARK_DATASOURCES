from pyspark.sql import SparkSession

 

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate() 
             
			 
    df_json = spark.read.json('users.json')

    df_json.show()

    df_json.printSchema()

    df_json.filter('name like "%ia"').show()

