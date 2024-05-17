from datetime import datetime

class AppLogger:
    def __init__(self):
        pass

    def log(self, file_object, log_message):
        """
        This method logs the given log_message to the provided file_object with timestamp.
        
        Args:
            file_object: The file object to write the log to.
            log_message: The message to be logged.
        """
        self.current_datetime = datetime.now()
        self.current_date = self.current_datetime.date()
        self.current_time = self.current_datetime.strftime("%H:%M:%S")
        file_object.write(str(self.current_date) + "/" + str(self.current_time) + "\t\t" + log_message + "\n")
