import configparser

from pyspark import  SparkConf

'''This function will read all the configs from the spark.conf file and set them to spark_conf object
and return the ready to use spark_conf'''

def get_spark_app_config():
    spark_conf= SparkConf()
    config= configparser.ConfigParser()
    config.read("spark.conf")

    for(key,val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key,val)
    return spark_conf