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
        .mode("overwrite") \
        .save()


def join_df(acc_df, parties_df, party_add_df, db_conf, table_name):

    parties_renamed_df = parties_df.withColumnRenamed("account_id", "renamed_account_id")

    join_expr = acc_df.account_id == parties_renamed_df.renamed_account_id
    interim_df = acc_df.join(parties_renamed_df, join_expr, "inner") \
        .select("active_ind", "renamed_account_id", "source_sys", "account_start_date", "LEGAL_TITLE_1",
                "LEGAL_TITLE_2", "tax_id_type", "tax_id", "branch_code", "country", "party_id")

    join_expr = interim_df.party_id == party_add_df.party_id
    final_df = interim_df.join(party_add_df, join_expr, "inner") \
        .select("active_ind", "renamed_account_id", "source_sys", "account_start_date", "LEGAL_TITLE_1",
                "LEGAL_TITLE_2", "tax_id_type", "tax_id", "branch_code", "country",
                "address_line1", "address_line_2", "city", "postal_code", "country_of_address",
                "address_start_date"
                )

    final_df.write \
        .format("jdbc") \
        .option("url", db_conf["conn_url"]) \
        .option("driver", db_conf["driver"]) \
        .option("dbtable", table_name) \
        .option("user", db_conf["user"]) \
        .option("password", db_conf["password"]) \
        .mode("overwrite") \
        .save()


def create_spark_tables(spark, acc_df, parties_df, party_add_df, db_conf, table_name):

    acc_df.createOrReplaceTempView("acc_df_tbl")
    parties_df.createOrReplaceTempView("parties_df_tbl")
    party_add_df.createOrReplaceTempView("party_add_df_tbl")

    spark_sql = """
    select a.active_ind, a.account_id, a.source_sys,
                               a.account_start_date, a.LEGAL_TITLE_1,
                               a.LEGAL_TITLE_2, a.tax_id_type,a.tax_id, a.branch_code, a.country,
                               c.address_line1, c.address_line_2, c.city, c.postal_code,
                               c.country_of_address, c.address_start_date
                               from acc_df_tbl a,
                                    parties_df_tbl b,
                                    party_add_df_tbl c
                               where a.account_id = b.account_id
                               and b.party_id = c.party_id"""

    acc_df_result = spark.sql(spark_sql)
#    acc_df_result.show()

    acc_df_result.write \
        .format("jdbc") \
        .option("url", db_conf["conn_url"]) \
        .option("driver", db_conf["driver"]) \
        .option("dbtable", table_name) \
        .option("user", db_conf["user"]) \
        .option("password", db_conf["password"]) \
        .mode("overwrite") \
        .save()