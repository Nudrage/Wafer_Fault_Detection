"""
Main entry point for the Wafer Fault Detection server.

This file starts a Flask server using the Flask application defined in this file. The server is started on all network interfaces with a random port number by default, or the port number specified in the PORT environment variable if it is set.
"""

from wsgiref import simple_server
import os
from flask import Flask
import flask_monitoringdashboard as dashboard
from flask_cors import CORS

app = Flask(__name__)       # The Flask application object used to serve requests.
dashboard.blind(app)        # Initialize the Flask-MonitoringDashboard plugin.
CORS(app)                   # Enable CORS support for all endpoints.


port = int(os.get("PORT", 5000))    # The port number to listen on, or 5000 by default.
if __name__ == "__main__":
    httpd = simple_server.make_server("0.0.0.0", port, app)     # The WSGI server object used to serve requests
    httpd.serve_forever()
