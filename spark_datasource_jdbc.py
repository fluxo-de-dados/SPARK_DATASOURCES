from pyspark.sql import SparkSession

 

if __name__ == '__main__':
	spark = (SparkSession.builder .config('spark.jars.packages','org.postgresql:postgresql:42.6.0') .getOrCreate() )

	df_jdbc = spark.read \
    .jdbc(url="jdbc:postgresql://localhost:5432/postgres", 
          table="public.users",
          properties={"user": "postgres", "password": "postgres",'driver':'org.postgresql.Driver'}
          )

	df_jdbc.show()

	df_jdbc.printSchema()

	df_jdbc.show()

