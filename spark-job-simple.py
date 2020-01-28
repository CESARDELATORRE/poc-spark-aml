import pyspark
import sys

from azureml.core.run import Run

# initialize logger
run = Run.get_context()

# Below code is Spark 2+

spark = pyspark.sql.SparkSession.builder.appName('test').getOrCreate()

# print runtime versions
print('****************')
print('Python version: {}'.format(sys.version))
print('Spark version: {}'.format(spark.version))
print('****************')

# log versions
run.log('PythonVersion', sys.version)
run.log('SparkVersion', spark.version)

rdd = spark.range(10)
print(rdd.collect())