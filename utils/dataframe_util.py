from pyspark.sql.functions import year, substring, month, dayofmonth
from .Logger import Logger
import os


class DataframeUtil:

    def __init__(self, spark):
        self.spark = spark
        self.logger = Logger.get_logger(conf_file_path='./utils/logging.conf')

    def get_df_from_data(self, bucket):
        self.logger.info(f'Creating df for data from file location {bucket}')
        return self.spark.read.format('json').load(bucket)

    def process_data(self, raw_df):
        self.logger.info(f'Processing data')
        return raw_df.withColumn('year', year(substring('created_at', 0, 10))) \
            .withColumn('month', month(substring('created_at', 0, 10))) \
            .withColumn('day', dayofmonth(substring('created_at', 0, 10)))

    def write_processed_data_to_destination(self, df, destination):
        if destination == 'local':
            self.logger.info(f'Saving output df to location : hdfs:///data/output')
            df.write.partitionBy('year', 'month', 'day').mode('overwrite').parquet('/data/output/')
        else:
            path = 's3://manishpractice/demo/output/'
            self.logger.info(f'Saving output df to location : {path}/data/output')
            df.write.partitionBy('year', 'month', 'day').mode('overwrite').parquet(path)
