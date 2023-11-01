import os
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse
import time
from kubernetes import client, config
import threading
from server import SimpleHTTPRequestHandler
from helper import createRegister, updateRegister, getDefinedMinute
from datetime import datetime, timedelta, timezone
import psycopg2
import psycopg2.extras as extras
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(filename='server.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Load the in-cluster configuration
config.load_incluster_config()

# Create a Kubernetes API client
api = client.BatchV1Api()

def main():
    load_dotenv()
    
    connection = psycopg2.connect(
    host=os.environ['POSTGRES_HOST'],
    port=os.environ['POSTGRES_PORT'],
    database=os.environ['POSTGRES_DB'],
    user=os.environ['POSTGRES_USER'],
    password=os.environ['POSTGRES_PASSWORD']
)

    # Step 1 Record in the register table the current time interval i.e. now and (now - 20) minutes
    start_dateTime = datetime.now(timezone.utc)
    interval = os.environ['SURVEY_INTERVAL_MINUTES']
    offset = os.environ['INTERVAL_OFFSET']
    minute = getDefinedMinute(interval, offset, start_dateTime)
    new_time = start_dateTime.replace(second=0, microsecond=0, minute=minute)           
    end_dateTime = new_time - timedelta(minutes=interval)
    createRegister(connection, start_dateTime, end_dateTime, logging)

    # Step 2 Check in the register table the previous time interval completed otherwise signal error
    
    # Step 3 Pull down list of files (not necessarily files themselves)
    
    # Step 4 Create Kubernetes ZKValidators
    host = '0.0.0.0'  # Listen on all available network interfaces
    port = 8080  # The port you want to listen on
    server = HTTPServer((host, port), SimpleHTTPRequestHandler)
    print(f"Server started on {host}:{port}")

    # Log server startup
    logging.info(f"Server started on {host}:{port}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
        print("\nServer stopped.")
        logging.info("Server stopped.")
    # Step 5 Update table at end to say done.
    updateRegister(connection, start_dateTime, logging)
if __name__ == '__main__':
    main()