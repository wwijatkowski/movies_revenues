from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VerifySpark").getOrCreate()
print("Spark session created successfully!")
spark.stop()