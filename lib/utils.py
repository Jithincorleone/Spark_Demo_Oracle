from pyspark.sql import *
from pyspark.conf import SparkConf
import configparser


def get_spark_confg():

    sparkConf = SparkConf()
    config = configparser.ConfigParser()
    config.read("conf/spark.conf")
    for (key, value) in config.items("Spark_Config"):
        print(key+"::"+value)
        sparkConf.set(key, value)
    return sparkConf

def get_db_connection_detials():
    conf = {}
    config = configparser.ConfigParser()
    config.read("conf/db.conf")
    for (key, value) in config.items("connection_settings"):
        conf[key] = value

    return conf

def get_input_file_details():
    conf = {}
    config = configparser.ConfigParser()
    config.read("conf/file.conf")

    for(key,value) in config.items("file_path"):
        conf[key] = value

    return conf