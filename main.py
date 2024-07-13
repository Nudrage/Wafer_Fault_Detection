# from wsgiref.simple_server import simple_server
import os
from flask import Flask, Response
from flask_cors import CORS
import flask_monitoringdashboard as dashboard

from utils import data_preprocessing, training_model

app = Flask(__name__)  # The Flask application object used to serve requests.
dashboard.blind(app)  # Initialize the Flask-MonitoringDashboard plugin.
CORS(app)  # Enable CORS support for all endpoints.


# Define a function that will be used as a route in the Flask application.
# This function is responsible for handling the training route.
def training_route_client():
    """
    Function to handle the training route in the Flask application.
    It creates an instance of the train_validation class and calls its
    train_validation method. It also creates an instance of the train_model
    class and calls its model_training method.

    Returns:
        A Flask Response object with a success message if no exceptions occur,
        otherwise, a Response object with an error message.
    """
    try:
        # if request.json['folder_path'] is not None:
        #     path = request.json['folder_path']
        path = r'data\training_files'
        train_val_obj = data_preprocessing.data_validation(path)
        train_val_obj.raw_data_validation()

        train_model_obj = training_model.train_model()
        train_model_obj.model_training()

    except (ValueError, KeyError, Exception) as e:
        return Response(f"Error occurred: {e.__class__.__name__}")

    return Response("Training successfull!!")


port = int(os.get("PORT", 5000))
# The port number to listen on, or 5000 by default.
if __name__ == "__main__":
    # httpd = simple_server.make_server(
    #     "0.0.0.0", port, app
    # )  # The WSGI server object used to serve requests
    # httpd.serve_forever()
    training_route_client()
