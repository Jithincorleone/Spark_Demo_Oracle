class Log4j:

    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        root_class ="Oracle_Spark"
        app_name = spark.sparkContext.getConf().get("spark.app.name")
        self.logger =log4j.LogManager.getLogger(root_class+"."+app_name)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def warn(self, message):
        self.logger.warn(message)

    def debug(self, message):
        self.logger.debug(message)
