from pyspark import SparkConf, SparkContext

APP_NAME = ""
MASTER_URL = "local[*]"

conf = SparkConf().setAppName(APP_NAME)
conf = conf.setMaster(MASTER_URL)
spark_context = SparkContext(conf=conf)
