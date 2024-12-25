

from pyspark.sql import *
from lib.logger import Log4J
#local multithreaded JVM with 3 threads    = local[3] below
#app_name in logger.py goes from appName set here
if __name__ == "__main__":
    #obtaining a spark session
    spark = SparkSession.builder\
            .appName("Hello Spark")\
            .master("local[3]")\
            .getOrCreate()

    logger = Log4J(spark)
    logger.info("Starting hellospark")

    logger.info("Finished hellospark")
    spark.stop()
