from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession, Row

APP_NAME = "NaaS"
MASTER_URL = "local[*]"

conf = SparkConf().setAppName(APP_NAME)
conf = conf.setMaster(MASTER_URL)
spark_context = SparkContext(conf=conf)

sql_context = SQLContext(spark_context)
spark_session = SparkSession(spark_context)

if __name__ == '__main__':
    num = spark_context.parallelize([1, 2, 3, 4]).count()
    print(num)