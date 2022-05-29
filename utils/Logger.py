import logging
import logging.config


class Logger:
    conf_file_path = 'logging.conf'
    logger_name = 'module'

    @staticmethod
    def get_logger(conf_file_path='logging.conf', logger_name=None):
        logging.config.fileConfig(conf_file_path)
        logger = logging.getLogger()
        return logger

    def set_conf_file_path(self, file_path):
        self.conf_file_path = file_path
