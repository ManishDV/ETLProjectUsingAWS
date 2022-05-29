import os
from pathlib import Path

from utils.spark_utils import SparkUtils
from utils.Logger import Logger
from utils.data_util import DataUtils
from utils.dataframe_util import DataframeUtil


def main():
    spark_utils = SparkUtils()
    parent_path = Path(__file__).parent
    logging_config = parent_path / 'utils/logging.conf'
    create_log_file_if_not_exists(parent_path)

    logger = Logger.get_logger(conf_file_path=logging_config)

    app_name = 'ETLApp'
    spark = spark_utils.get_spark_session(app_name)
    data_utils = DataUtils()
    data_utils.download_and_upload_data_to_s3('2022-05-27-{0..23}.json.gz')

    env = os.getenv('ENVIRON')
    bucket_name = 's3a://manishpractice/demo/*' if env == 'PROD' else 'file:///' + str(parent_path) + '/data/input_data/'
    destination = 'local' if env == 'DEV' else 's3'
    dataframe_util = DataframeUtil(spark)

    df = dataframe_util.get_df_from_data(bucket_name)

    df = dataframe_util.process_data(df)

    dataframe_util.write_processed_data_to_destination(df, destination)


def create_log_file_if_not_exists(parent_path):
    os.makedirs(os.path.dirname(parent_path / 'logs/ETLApp.log'), exist_ok=True)


if __name__ == '__main__':
    main()
