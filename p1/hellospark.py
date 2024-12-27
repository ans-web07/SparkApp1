from pyspark import SparkConf
from pyspark.shell import spark
from pyspark.sql import *
from lib.logger import Log4J
from p1.lib.utils import get_spark_app_config

#local multithreaded JVM with 3 threads    = local[3] below
#app_name in logger.py goes from appName set here
if __name__ == "__main__":
    #obtaining a spark session and setting configurations
    '''spark = SparkSession.builder\
            .appName("Hello Spark")\
            .master("local[3]")\
            .getOrCreate()'''

    #Another way for configuring spark session
    '''config = SparkConf()
    config.set("spark.app.name","Hello Spark")
    config.set("spark.master","local[3]")
    spark = SparkSession.builder\
            .config(conf=config)\
            .getOrCreate()'''

    #3rd way for configuring spark session
    config1= get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=config1) \
        .getOrCreate()

    logger = Log4J(spark)
    logger.info("Starting hellospark")

    #Reading all spark configs
    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())

    logger.info("Finished hellospark")
    spark.stop()
