from logs.application_logging import logger
from utils import raw_validation

class train_validation:
    def __init__(self, path):
        self.log_writer = logger.AppLogger()
        self.file_object = open("logs/training_logs/training_main_log.txt", 'a+')
        self.raw_data = raw_validation.raw_data_validation(path)

    def train_validation(self):
        self.log_writer.log(self.file_object, "Starting Validation of Files !!!")
        length_of_datestamp, length_of_timestamp, column_name, no_of_columns = self.raw_data.values_from_schema()