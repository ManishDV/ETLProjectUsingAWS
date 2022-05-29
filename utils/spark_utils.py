from .Logger import Logger
from pyspark.sql import SparkSession
import os


class SparkUtils:

    def __init__(self):
        self.logger = Logger.get_logger(conf_file_path='utils/logging.conf', logger_name='module')

    def get_spark_session(self, app_name):
        master = os.getenv('ENVIRON')

        self.logger.info(f'Running Spark Application On {master}')

        master = 'yarn' if master == 'PROD' else 'local[3]'

        self.logger.info(f"Running Spark Application Using '{master}' as master")

        return SparkSession.builder.appName(app_name).master(master).config('package', 'org.apache.hadoop:hadoop-aws:2.10.1,com.amazonaws:aws-java-sdk:1.12.228').getOrCreate()

