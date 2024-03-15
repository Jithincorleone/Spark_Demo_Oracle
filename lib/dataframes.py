from pyspark.sql import *
from pyspark.conf import SparkConf
from pyspark.sql.types import *

def get_spark_session(sparkconf):

    return SparkSession.builder \
           .config(conf=sparkconf) \
           .getOrCreate()

def read_input_table(db_conf, spark):

    return spark.read \
               .format("jdbc") \
               .option("url", db_conf["conn_url"]) \
               .option("driver", db_conf["driver"]) \
               .option("dbtable", db_conf["db_table"]) \
               .option("user", db_conf["user"]) \
               .option("password", db_conf["password"]) \
               .load()

def get_raw_files_df(spark, file_conf):

    acc_schema = StructType(
        [StructField("load_date", DateType()),
            StructField("active_ind", IntegerType()),
            StructField("account_id", LongType()),
            StructField("source_sys", StringType()),
            StructField("account_start_date", TimestampType()),
            StructField("legal_title_1", StringType()),
            StructField("legal_title_2", StringType()),
            StructField("tax_id_type", StringType()),
            StructField("tax_id", StringType()),
            StructField("branch_code", StringType()),
            StructField("country", StringType())])

    party_schema = StructType(
        [StructField("load_date", DateType()),
            StructField("account_id", LongType()),
            StructField("party_id", LongType()),
            StructField("relation_type", StringType()),
            StructField("relation_start_date", TimestampType())])

    address_schema = StructType(
        [StructField("load_date", DateType()),
         StructField("party_id", LongType()),
         StructField("address_line1", StringType()),
         StructField("address_line_2", StringType()),
         StructField("city", StringType()),
         StructField("postal_code", IntegerType()),
         StructField("country_of_address", StringType()),
         StructField("address_start_date", DateType())])

    acc_df = spark.read \
             .format("csv") \
             .schema(acc_schema) \
             .option("header", "true") \
             .option("inferSchema", "true") \
             .load(file_conf["accounts_file"])

    parties_df = spark.read \
                 .format("csv") \
                 .schema(party_schema) \
                 .option("header", "true") \
                 .option("inferSchema", "true") \
                 .load(file_conf["parties_file"])

    party_add_df = spark.read \
                   .format("csv") \
                   .schema(address_schema) \
                   .option("header", "true") \
                   .option("inferSchema", "true") \
                   .load(file_conf["party_add_file"])

    return acc_df, parties_df, party_add_df

def write_db_table(df, db_conf, table_name):

         df.write \
        .format("jdbc") \
        .option("url", db_conf["conn_url"]) \
        .option("driver", db_conf["driver"]) \
        .option("dbtable", table_name) \
        .option("user", db_conf["user"]) \
        .option("password", db_conf["password"]) \
        .mode("overwrite")\
        .save()