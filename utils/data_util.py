import requests
import boto3
from .Logger import Logger
import os


class DataUtils:

    def __init__(self):
        self.logger = Logger.get_logger('./utils/logging.conf')
        self.basic_url = 'https://data.gharchive.org/'

    def download_and_upload_data_to_s3(self, file_name):
        environ = os.getenv('ENVIRON')
        if '{' in file_name:
            base_file = file_name[:10]

            for i in range(0, 1):
                file_name_extracted = base_file + '-' + str(i) + '.json.gz'
                if environ == 'PROD':
                    self.download_and_upload_to_s3(file_name_extracted)
                else:
                    self.download_and_save_to_local(file_name_extracted)
        else:
            if environ == 'PROD':
                self.download_and_upload_to_s3(file_name)
            else:
                self.download_and_save_to_local(file_name)

    def download_and_upload_to_s3(self, file_name):
        data = requests.get(self.basic_url + file_name)
        if data.status_code == 200:
            s3_client = boto3.client('s3', region_name='us-east-1')
            s3_client.put_object(Body=data.content, Bucket='manishpractice', Key='demo/' + file_name)
            self.logger.info(f'Processed file {file_name}')
        else:
            self.generate_error_and_exit(f'Error downloading file {file_name}')

    def generate_error_and_exit(self, message):
        self.logger.error(message)
        exit(0)

    def download_and_save_to_local(self, file_name):
        data = requests.get(self.basic_url + file_name)
        if data.status_code == 200:
            open('./data/input_data/' + file_name, 'wb').write(data.content)
        else:
            self.generate_error_and_exit(f'Error downloading file {file_name}')
