import os
import json
import shutil
import regex as re
import pandas as pd
from datetime import datetime

from logs.application_logging import logger
# flake8: noqa: E501

class RawDataValidator:
    def __init__(self, path):
        """
        Initializes the RawDataValidator object with the provided path.

        Parameters:
            path: str - The directory path where the data is stored.

        Returns:
            None
        """
        self.schema_path = 'metadata/training_schema.json'
        self.logger = logger.AppLogger()
        self.batch_directory = path

    def values_from_schema(self):
        """
        Reads the schema file and retrieves the necessary values from it.

        Returns:
            length_of_datestamp (str): The length of the date stamp in the file.
            length_of_timestamp (str): The length of the time stamp in the file.
            column_name (str): The name of the column.
            no_of_columns (str): The number of columns in the file.

        Raises:
            ValueError: If the value is not found inside the schema file.
            KeyError: If an incorrect key is passed.
            Exception: If any other exception occurs.
        """
        try:
            with open(self.schema_path, 'r') as schema_file:
                schema_data = json.load(schema_file)
            pattern = schema_data['SampleFileName']
            length_of_datestamp = schema_data['LengthOfDateStampInFile']
            length_of_timestamp = schema_data['LengthOfTimeStampInFile']
            column_name = schema_data['ColName']
            no_of_columns = schema_data['NumberofColumns']

            file = open("logs/training_logs/raw_validation_logs.txt", 'a+')
            message = "LengthOfDateStampInFile:: " + length_of_datestamp + "\t" + "LengthOfTimeStampInFile:: " + length_of_timestamp +"\t " + "NumberofColumns:: " + no_of_columns + "\n"
            self.logger.log(file, message)
            file.close()

        except ValueError:
            with open("logs/training_logs/raw_validation_logs.txt", 'a+') as file:
                self.logger.log(file, "ValueError: Value not found inside schema_training.json")
            raise ValueError

        except KeyError:
            with open("logs/training_logs/raw_validation_logs.txt", 'a+') as file:
                self.logger.log(file, "KeyError: Key value error incorrect key passed inside schema_training.json")
            raise KeyError
        
        except Exception as e:
            with open("logs/training_logs/raw_validation_logs.txt", 'a+') as file:
                self.logger.log(file, str(e))
            raise Exception
        
        return length_of_datestamp, length_of_timestamp, column_name, no_of_columns

    def manual_regex_creation(self):
        """
        Creates a regular expression pattern for validating file names.
        Returns:
            str: The regular expression pattern for validating file names.
        """
        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def validate_file_name(self, regex, length_of_datastamp, length_of_timestamp):
        """
        This function deletes the existing bad data folder and the existing good data folder, and create new folder directories.
        It iterates over the files in the batch directory and checks if each file name matches the given regular expression pattern.
        If a file name matches the pattern, it splits the file name at the dot and at the underscore.It checks if the length of the 
        datastamp and the length of the timestamp are as expected. If they are, it copies the file to the 'good_data_raw' folder in 
        the 'training_data' directory. If the length of the datastamp or the length of the timestamp is not as expected, 
        it copies the file to the 'bad_data_raw' folder in the 'training_data' directory. If a file name does not match the pattern, 
        it copies the file to the 'bad_data_raw' folder in the 'training_data' directory. Any errors that occur during the validation 
        process are logged and an exception is raised.

        Args:
            regex (str): The regular expression pattern to match against the file names.
            length_of_datastamp (int): The expected length of the datastamp in the file name.
            length_of_timestamp (int): The expected length of the timestamp in the file name.

        Returns:
            None

        Raises:
            Exception: If an error occurs during the validation process.
        """
        self.delete_existing_bad_data_folder()
        self.delete_existing_good_data_folder()

        self.create_directory_for_data_folder()
        files = [file for file in os.listdir(self.batch_directory)]
        try:
            with open('logs/file_validation_logs.txt', 'a+') as log_file:
                self.logger.log(log_file, 'File name validation started')
                for file_name in files:
                    if (re.match(regex, file_name)):
                        split_at_dot = re.split('.csv', file_name)
                        split_at_dot = re.split('_', split_at_dot[0])
                        if len(split_at_dot[1] == length_of_datastamp):
                            if len(split_at_dot[2] == length_of_timestamp):
                                shutil.copy('data/training_batch_files' + file_name, 'data/training_data/good_data_raw')
                                self.logger.log(log_file, 'Valid file name. File moved to good_data_raw folder: ', file_name)

                            else:
                                shutil.copy('data/training_batch_files' + file_name, 'data/training_data/bad_data_raw')
                                self.logger.log(log_file, 'Invalid file name. File moved to bad_data_raw folder: ', file_name)
                        else:
                            shutil.copy('data/training_batch_files' + file_name, 'data/training_data/bad_data_raw')
                            self.logger.log(log_file, 'Invalid file name. File moved to bad_data_raw folder: ', file_name)
                    else:
                        shutil.copy('data/training_batch_files' + file_name, 'data/training_data/bad_data_raw')
                        self.logger.log(log_file, 'Invalid file name. File moved to bad_data_raw folder: ', file_name)
        except Exception as e:
            with open('logs/training_logs/file_validation_logs.txt', 'a+') as file:
                self.logger.log(file, "Error occured while validating FileName: ", e)
            raise Exception


    def delete_existing_bad_data_folder(self):
        """
        Deletes the existing bad data folder if it exists.
        This function checks if the bad data folder exists in the specified path.
        If it does, the function deletes the folder and logs a message indicating that
        the bad data directory was deleted before starting the validation process.

        Parameters:
            self (object): The instance of the class.

        Returns:
            None

        Raises:
            OSError: If there is an error while deleting the bad data directory.
        """
        try:
            path = "data/training_data/"
            if os.path.isdir(path + 'bad_data_raw/'):
                shutil.rmtree(path + 'bad_data_raw/')
                with open('logs/training_logs/general_logs.txt', 'a+') as file:
                    self.logger.log(file, "Bad Raw Directory deleted before starting validation")
        except OSError as s:
            with open('logs/training_logs/general_logs.txt', 'a+') as file:
                self.logger.log(file, "Error while Deleting Bad data Directory: ", s)
            raise OSError

    def delete_existing_good_data_folder(self):
        """
        Deletes the existing good data folder if it exists.
        This function checks if the good data folder exists in the specified path. 
        If it does, the function deletes the folder and logs a message indicating that the good data directory was deleted before starting the validation process.

        Parameters:
            self (object): The instance of the class.

        Returns:
            None

        Raises:
            OSError: If there is an error while deleting the good data directory.
        """
        try:
            path = "data/training_data/"
            if os.path.isdir(path + 'good_data_raw/'):
                shutil.rmtree(path + 'good_data_raw/')
                with open('logs/training_logs/general_logs.txt', 'a+') as file:
                    self.logger.log(file, "Good Raw Directory deleted before starting validation")
        except OSError as s:
            with open('logs/training_logs/general_logs.txt', 'a+') as file:
                self.logger.log(file, "Error while Deleting Good data Directory: ", s)
            raise OSError

    def create_directory_for_data_folder(self):
        """
        Creates directories for the 'good_data_raw' and 'bad_data_raw' folders in the 'training_data' directory.
        This function checks if the 'good_data_raw' and 'bad_data_raw' directories exist in the 'training_data' directory. 
        If they do not exist, the function creates them using the `os.mkdir()` function.

        Parameters:
            self (object): The instance of the class.

        Returns:
            None

        Raises:
            OSError: If there is an error while creating the directories.
        """
        try:
            path = os.path.join('training_data/', '/good_data_raw/')
            if not os.path.isdir(path):
                os.mkdir(path)
            path = os.path.join('training_data/', '/bad_data_raw/')
            if not os.path.isdir(path):
                os.mkdir(path)
        except OSError as s:
            with open('logs/training_logs/general_logs.txt', 'a+') as file:
                self.logger.log(file, 'Error while creating Directory', s)
            raise OSError

    def validate_column_length(self, no_of_columns):
        """
        Validates the column length of the CSV files in the 'good_data_raw' directory.
        Moves the files with invalid column length to the 'bad_data_raw' directory.

        Parameters:
            no_of_columns (int): The expected number of columns in the CSV files.

        Returns:
            None

        Raises:
            OSError: If there is an error while moving the file.
            Exception: If there is an unexpected error while moving the file.
        """
        try:
            # Open the log file for appending
            with open('logs/training_logs/file_validation_logs.txt', 'a+') as log_file:
                self.logger.log(log_file, 'Column validation started')
                for file in os.listdir('data/training_data/good_data_raw'):
                    csv_data = pd.read_csv('data/training_data/good_data_raw/' + file)

                    if csv_data.shape[1] == no_of_columns:
                        pass
                    else:
                        shutil.move('data/training_data/good_data_raw/' + file, 'data/training_data/bad_data_raw')
                        self.logger.log(log_file, 'Invalid column length for the File, file moved to Bad data folder: ', file)

                self.logger.log(log_file, 'Column length Validation Completed.')

        except OSError as s:
            with open('logs/training_logs/file_validation_logs.txt', 'a+') as log_file:
                self.logger.log(log_file, 'Error occured while moving the file: ', s)
            raise OSError

        except Exception as e:
            with open('logs/training_logs/file_validation_logs.txt', 'a+') as log_file:
                self.logger.log(log_file, 'Error occured while moving the file: ', e)
            raise Exception
        

    def validate_missing_values(self):
        """
        Validates the CSV files in the 'good_data_raw' directory and moves files with missing values to the 'bad_data_raw' directory.
        Renames the 'Unnamed: 0' column to 'Wafer' and saves the validated files in the 'Training_Raw_files_validated/Good_Raw' directory.

        Raises:
            OSError: If there is an error while moving the file.
            Exception: If there is an unexpected error while moving the file.
        """
        try:
            with open('logs/training_logs/file_validation_logs.txt', 'a+') as log_file:
                log_file.write('Missing Values Validation Started\n')

                for file_name in os.listdir('data/training_data/good_data_raw/'):
                    file_path = 'data/training_data/good_data_raw/' + file_name
                    csv_data = pd.read_csv(file_path)
                    if csv_data.isnull().values.any():
                        shutil.move(file_path, 'data/training_data/bad_data_raw')
                        log_file.write(f'Invalid Column Length for the file: {file_name}, file moved to bad_data_raw folder\n')
                    else:
                        csv_data.rename(columns={'Unnamed: 0': 'Wafer'}, inplace=True)
                        validated_file_path = 'Training_Raw_files_validated/Good_Raw/' + file_name
                        csv_data.to_csv(validated_file_path, index=False)

        except OSError as e:
            with open('logs/training_logs/file_validation_logs.txt', 'a+') as log_file:
                log_file.write(f'Error Occured while moving the file: {e}')
            raise OSError
        except Exception as e:
            with open('logs/training_logs/file_validation_logs.txt', 'a+') as log_file:
                log_file.write(f'Error Occured while moving the file: {e}')
            raise Exception


    def move_bad_files_to_archive(self):
        """
        Moves all files from the 'Bad_Raw' directory located at 'Training_Raw_files_validated/Bad_Raw/' to a new directory named 'BadData_[current_date]_[current_time]' located at 'TrainingArchiveBadData/'. 
        If the 'Bad_Raw' directory does not exist, the function does nothing. 
        If the destination directory does not exist, it is created. 
        If a file with the same name already exists in the destination directory, it is not moved. 
        After moving all files, a log message is written to 'Training_Logs/GeneralLog.txt' indicating that the bad files were moved to archive. 
        If the 'Bad_Raw' directory exists after moving the files, it is deleted. 
        If the deletion is successful, a log message is written to 'Training_Logs/GeneralLog.txt' indicating that the 'Bad_Raw' directory was deleted successfully. 
        If an error occurs during the process, an exception is raised and a log message is written to 'Training_Logs/GeneralLog.txt' indicating the error.
        Parameters:
            self (object): The instance of the class.
        Raises:
            Exception: If an error occurs during the process.
        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            source = 'Training_Raw_files_validated/Bad_Raw/'
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'TrainingArchiveBadData/BadData_' + str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
                file.close()
        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e
