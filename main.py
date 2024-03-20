from pyspark.sql import *
from lib.utils import get_spark_confg, get_db_connection_detials, get_input_file_details
from lib.logger import Log4j
from lib.dataframes import *

if __name__ == "__main__":

    sparkconf = get_spark_confg()
    db_conf = get_db_connection_detials()
    file_conf = get_input_file_details()
    spark = get_spark_session(sparkconf)

    logger = Log4j(spark)
    logger.info("spark Started")

    acc_df, parties_df, party_add_df = get_raw_files_df(spark, file_conf)

    write_db_table(acc_df, db_conf, db_conf["acc_table"])
    write_db_table(parties_df, db_conf, db_conf["party_table"])
    write_db_table(party_add_df, db_conf, db_conf["address_table"])
#    join_df(acc_df, parties_df, party_add_df, db_conf, db_conf["join_table"])
    create_spark_tables(spark, acc_df, parties_df, party_add_df, db_conf, db_conf["join_table"])

    spark.stop()
    logger.info("spark stopped")
