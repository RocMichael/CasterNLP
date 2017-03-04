from pyspark import SparkConf, SparkContext

APP_NAME = ""
MASTER_URL = "local[*]"

conf = SparkConf().setAppName(APP_NAME)
conf = conf.setMaster(MASTER_URL)
spark_context = SparkContext(conf=conf)

num = spark_context.parallelize([1, 2, 3, 4]).count()
print(num)
