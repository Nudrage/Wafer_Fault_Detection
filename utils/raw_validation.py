import json
from logs.application_logging import logger

class raw_data_validation:
    def __init__(self, path):
        self.schema_path = 'metadata/training_schema.json'
        self.logger = logger.AppLogger()

    def values_from_schema(self):
        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
            pattern = dic['SampleFileName']
            length_of_datestamp = dic['LengthOfDateStampInFile']
            length_of_timestamp = dic['LengthOfTimeStampInFile']
            column_name = dic['ColName']
            no_of_columns = dic['NumberofColumns']

            file = open("logs/training_logs/raw_validation_logs.txt", 'a+')
            message = "LengthOfDateStampInFile:: " + length_of_datestamp + "\t" + "LengthOfTimeStampInFile:: " + length_of_timestamp +"\t " + "NumberofColumns:: " + no_of_columns + "\n"
            self.logger.log(file, message)
            file.close()
        
        except ValueError:
            file.open("logs/training_logs/raw_validation_logs.txt", 'a+')
            self.logger.log(file, "ValueError: Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file.open("logs/training_logs/raw_validation_logs.txt", 'a+')
            self.logger.log(file, "KeyError: Key value error incorrect key passed")
            file.close()
            raise KeyError
        
        except Exception as e:
            file.open("logs/training_logs/raw_validation_logs.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e
        
        return length_of_datestamp, length_of_timestamp, column_name, no_of_columns
    


    