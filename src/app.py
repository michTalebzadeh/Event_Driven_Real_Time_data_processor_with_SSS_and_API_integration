from flask import Flask, jsonify, make_response
from requests.exceptions import RequestException
import socket
import requests
import logging
import json
import threading
import time


host = socket.gethostname()
print(f"\n\n hostt is {host}\n\n")
port = 8999
endpoint='/api/data'
api_url = f'http://{host}:{port}{endpoint}'

app = Flask(__name__)
"""
Below defines a route in Flask application. When a GET request is made to the endpoint /api/data,
the get_api_data function is executed. Inside this function, the data is accessed 
within a critical section protected by the data_lock. It returns the JSON representation of the data.
"""

@app.route(f'{endpoint}', methods=['GET'])
def get_api_data():
    # Serve data at the /api/data endpoint
    with data_lock:
        return jsonify(data)

# Variable to store the data and timestamp for the last file read
data = []
'''
A Lock (short for Locking) is a synchronization primitive used to ensure that only one thread accesses a shared resource at a time.
It helps prevent data corruption and race conditions in multithreaded or multiprocess environments.
'''
last_read_timestamp = 0
data_lock = threading.Lock()

def read_file_periodically():
    global data, last_read_timestamp
    while True:
        try:
            current_time = time.time()

            # Check if enough time has passed since the last file read
            if current_time - last_read_timestamp > 10:  # 10 seconds
                with open('/var/tmp/prices.json', 'r', encoding='utf-8') as file:
                    content = file.read()
                    if content.strip():  # Check if the file is not empty
                        with data_lock:
                            data = json.loads(content)
                            last_read_timestamp = current_time  # Update the timestamp
                    else:
                        with data_lock:
                            data = []  # Set data to an empty list
                            last_read_timestamp = current_time  # Update the timestamp
            else:
                # If not enough time has passed, sleep for a shorter duration
                time.sleep(5)

            # Make the API call to get data
            response = requests.get(api_url)

            # Check the response status code
            response.raise_for_status()

            # Successful API call, update data
            with data_lock:
                data = response.json()

        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON: {e}")
            with data_lock:
                data = []  # Set data to an empty list in case of decoding error
                last_read_timestamp = current_time  # Update the timestamp
        except requests.RequestException as e:
            logging.error(f"Error making API call: {e}")
            with data_lock:
                data = []  # Set data to an empty list
                last_read_timestamp = current_time  # Update the timestamp
        except requests.HTTPError as e:
            logging.error(f"HTTP error during API call: {e.response.status_code}, {e.response.text}")
            with data_lock:
                data = []  # Set data to an empty list
                last_read_timestamp = current_time  # Update the timestamp
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            with data_lock:
                data = []  # Set data to an empty list
                last_read_timestamp = current_time  # Update the timestamp
"""
This block is typical for running a Flask application. Here is what each part does:

if __name__ == '__main__':: This checks if the script is being run directly (not imported as a module).
It ensures that the following code is executed only when the script is the main program.
host = socket.gethostname(): Gets the hostname of the machine. This is used as the host for the Flask application, allowing external connections.
port = 8999: Specifies the port on which the Flask application will run.
thread = threading.Thread(target=read_file_periodically): Creates a new thread that runs 
the read_file_periodically function in the background. This thread is set as a daemon, meaning it will exit when the main program exits.
thread.start(): Starts the background thread.
app.run(debug=True, host=host, port=port): Runs the Flask application. 
The debug=True option enables debugging mode, and the host and port parameters determine where the app is accessible.
"""
if __name__ == '__main__':

    # Start the background thread to periodically read the file and update data
    thread = threading.Thread(target=read_file_periodically)
    thread.daemon = True
    thread.start()

    # Run the Flask app
    app.run(debug=True, host=host, port=port)

