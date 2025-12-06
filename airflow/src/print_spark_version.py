from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestFromAirflow").getOrCreate()

df = spark.range(0, 10)
print("Spark version:", spark.version)
print("Row count:", df.count())

spark.stop()
