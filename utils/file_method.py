import os
import shutil
import pickle 

class file_operations:
    def __init__(self, file_object, logger_object):
        self.logger = logger_object
        self.file_object = file_object
        self.model_directory = 'models/'

    def save_model(self, model, filename):
        self.logger.log(self.file_object, 'Entered the save_model method of the file_operations class')
        try:
            path = os.path.join(self.model_directory, filename)
            if os.path.isdir(path):
                shutil.rmtree(self.model_directory)
            os.mkdir(self.model_directory)
            with open(path + '/' + filename + '.sav', 'wb') as file:
                pickle.dump(model, file)
            self.logger.log(self.file_object, 'Model File '+filename+' saved. Exited the save_model method of the Model_Finder class')
            return 'success'

        except Exception as e:
            self.logger.log(self.file_object, 'Exception occured in save_model method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger.log(self.file_object, 'Model File ' + filename + ' could not be saved. Exited the save_model method of the Model_Finder class')
            raise Exception()
            
