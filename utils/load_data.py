import pandas as pd 

class get_data:
    def __init__(self, file_object, logger_object):
        self.logger = logger_object
        self.file_object = file_object
        self.training_file = 

    def extract_data(self):
        self.logger.log(self.file_object, 'Entered the extract_data method of the load_data class')
        try:
            self.data = pd.read_csv(self.training_file)
            self.logger.log(self.file_object, 'Data Load Successful.Exited the get_data method of the Data_Getter class')
            return self.data
        except Exception as e:
            self.logger.log(self.file_object, 'Exception occured in get_data method of the Data_Getter class. Exception message: ' + str(e))
            self.logger.log(self.file_object, 'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise Exception
