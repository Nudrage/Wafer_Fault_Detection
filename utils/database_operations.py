import sqlite3
import os 
import shutil
import csv
from logs.application_logging import logger

class DatabaseOperations:
    def __init__(self):
        self.path = 'data/training_database/'
        self.logger = logger.AppLogger()
        self.good_file_path =
        self.bad_file_path =

    def database_connection(self, database_name):
        """
        Establishes a connection to the specified database.

        Parameters:
            database_name (str): Name of the database.

        Returns:
            sqlite3.Connection: Connection object for the database.

        Raises:
            ConnectionError: If an error occurs while connecting to the database.
        """
        try:
            # Create a connection to the database
            conn = sqlite3.connect(self.path + database_name + '.db')
            with open('logs/training_logs/database_operation_logs.txt', 'a+') as file:
                self.logger.log(file, "Opened %s database successfully", database_name)

        except ConnectionError:
            with open('logs/training_logs/database_operation_logs.txt', 'a+') as file:
                self.logger.log(file, "Error while connecting to database: %s", ConnectionError)
            raise ConnectionError

        # Return the connection object
        return conn


    def create_table_db(self, database_name, column_names):
        """
        Creates a table named 'Good_Raw_Data' in the given database if it doesn't exist.
        Adds new columns to the table if they don't already exist.

        Parameters:
            database_name (str): Name of the database.
            column_names (dict): Dictionary of column names and their data types.

        Raises:
            Exception: If an error occurs while creating the table.
        """
        try:
            # Establish database connection
            connection = self.database_connection(database_name)
            conn = connection.cursor()

            # Check if 'Good_Raw_Data' table exists
            conn.execute('SELECT count(name) FROM sqlite_master WHERE type = "table" AND name = "Good_Raw_Data"')
            if conn.fetchone()[0] == 1:
                # Close the connection if table exists
                connection.close()

                # Log success message
                with open('logs/training_logs/database_operation_log.txt', 'a+') as file:
                    self.logger.log(file, "Tables created successfully!!")
                with open('logs/training_logs/database_connection_log.txt', 'a+') as file:
                    self.logger.log(file, "Closed %s database successfully", database_name)
            else:
                # Add new columns to the table
                for key, type in column_names.items():
                    try:
                        connection.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key, dataType=type))
                    except:
                        connection.execute('CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))

                # Close the connection
                connection.close()

                # Log success message
                with open('logs/training_logs/database_operation_log.txt', 'a+') as file:
                    self.logger.log(file, "Tables created successfully!!")
                with open('logs/training_logs/database_connection_log.txt', 'a+') as file:
                    self.logger.log(file, "Closed %s database successfully", database_name)

        except Exception as e:
            # Log error message
            with open('logs/training_logs/database_operation_log.txt', 'a+') as file:
                self.logger.log(file, "Error while creating table: %s", e)

            # Close the connection and raise an exception
            connection.close()
            with open('logs/training_logs/database_connection_log.txt', 'a+') as file:
                self.logger.log(file, "Closed %s database successfully", database_name)
            raise Exception()

                
    def insert_into_table(self, database):
        """
        This function inserts data from CSV files into the 'Good_Raw_Data' table in the specified database.
        
        Args:
            database (str): The name of the database.
        """
        # Establish database connection
        conn = self.database_connection(database)
        good_file_path = self.good_file_path
        bad_file_path = self.bad_file_path
        
        log_file = open('logs/training_logs/database_insertion_log.txt', 'a+')
        onlyfiles = [f for f in os.listdir(good_file_path)]
        for file in onlyfiles:
            try:
                with open(good_file_path + "/" + file, "r") as f:
                    next(f)
                    reader = csv.reader(f, delimiter="\n")

                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values = list_))
                                self.logger.log(log_file, " %s: File loaded successfully!!", file)
                                conn.commit()
                            except Exception
                                raise Exception
            except Exception as e:
                # Rollback the transaction
                conn.rollback()
                self.logger.log(log_file, "Error while creating table: %s", e)
                shutil.move(good_file_path + "/" + file, bad_file_path)
                self.logger.log(log_file, "File Moved successfully %s", file)
            
        # Close the database connection
        conn.close()
        log_file.close()


    def selecting_data_from_table_into_csv(self, database):
        self.fileFromDb = 'Training_FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("Training_Logs/ExportToCsv.txt", 'a+')
        try:
            conn = self.dataBaseConnection(Database)
            sqlSelect = "SELECT *  FROM Good_Raw_Data"
            cursor = conn.cursor()

            cursor.execute(sqlSelect)

            results = cursor.fetchall()
            # Get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            #Make the CSV ouput directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open CSV file for writing.
            csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')

            # Add the headers and data to the CSV file.
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file, "File exported successfully!!!")
            log_file.close()

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" %e)
            log_file.close()
