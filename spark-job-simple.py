import pyspark
import sys

# Below code is Spark 2+

spark = pyspark.sql.SparkSession.builder.appName('test').getOrCreate()
print('Spark version: {}'.format(spark.version))
rdd = spark.range(10)
print(rdd.collect())