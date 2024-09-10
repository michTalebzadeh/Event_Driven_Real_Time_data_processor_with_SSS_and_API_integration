import sys
# Adding necessary paths
sys.path.append('/home/hduser/dba/bin/python/Event_Driven_Real_Time_data_processor_with_SSS_and_API_integration/')
sys.path.append('/home/hduser/dba/bin/python/Event_Driven_Real_Time_data_processor_with_SSS_and_API_integration/conf')
sys.path.append('/home/hduser/dba/bin/python/Event_Driven_Real_Time_data_processor_with_SSS_and_API_integration/othermisc')
sys.path.append('/home/hduser/dba/bin/python/Event_Driven_Real_Time_data_processor_with_SSS_and_API_integration/src')
sys.path.append('/home/hduser/dba/bin/python/Event_Driven_Real_Time_data_processor_with_SSS_and_API_integration/udfs')
sys.path.append('/home/hduser/.local/lib/python3.9/site-packages')

# Importing required modules
from config import config
from udfs import udf_functions as udfs
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round, current_timestamp, lit, udf
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.window import Window
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf

import json
import datetime
import time
import uuid
import schedule

from flask import Flask
from flask_restful import Resource, Api
from flask.signals import got_request_exception
from pyspark.sql.streaming import DataStreamWriter
import socket

import logging
import pyspark

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
appName = "sampleAPIRead"

import threading
import requests
from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamWriter

# Set up logging configuration
logging.basicConfig(level=logging.INFO)

appName = "EventDrivenRealTimeDataProcessorWithAP"

app = Flask(__name__)
api = Api(app)

# Set up Spark session and context
spark_session = SparkSession.builder.appName(appName).getOrCreate()
spark_context = spark_session.sparkContext

numRows = 100
processingTime = 5
checkpoint_path = "file:///ssd/hduser/randomdata/chkpt"

class StartStreamingResource(Resource):
    def __init__(self, spark_session, spark_context, streaming_dataframe, processing_time, checkpoint_path):
        self.spark_session = spark_session
        self.spark_context = spark_context
        self.streaming_dataframe = streaming_dataframe
        self.processing_time = processing_time
        self.checkpoint_path = checkpoint_path
        self.query = None
        super().__init__()

# Set the log level to ERROR to reduce verbosity
spark_context.setLogLevel("ERROR")

# Define schema and other constants
data_schema = StructType([
    StructField("rowkey", StringType()),
    StructField("ticker", StringType()),
    StructField("timeissued", StringType()),
    StructField("price", FloatType())
])

# Streaming DataFrame Creation
streaming_dataframe = spark_session.readStream.format("rate") \
    .option("rowsPerSecond", 100) \
    .option("subscribe", "rate") \
    .option("failOnDataLoss", "false") \
    .option("includeHeaders", "true") \
    .option("startingOffsets", "latest") \
    .load()

# Define external event trigger setup
def listen_for_external_event():
    # Your logic to listen for an external event, e.g., Kafka consumer, file watcher, HTTP server, etc.
    return True

# API Data Retrieval
def get_api_data():
    try:
        response = requests.get("http://rhes75:8999/api/data")
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None

def process_data(batch_df, batchId):
    if len(batch_df.take(1)) > 0:
        # Check for external event trigger
        if listen_for_external_event():
            # Assuming 'data' is a list of dictionaries obtained from the API in each batch
            api_data = get_api_data()
            if api_data:
                dfAPI = spark_session.createDataFrame(api_data, schema=data_schema)
                dfAPI = dfAPI \
                    .withColumn("op_type", lit("GET")) \
                    .withColumn("op_time", current_timestamp())

                dfAPI.show(10, False)
                dfAPI.createOrReplaceTempView("tmp_view")
                try:
                    """
                        When working with Spark Structured Streaming,
                        each streaming query runs in its own separate Spark session to ensure isolation and avoid conflicts
                        between different queries. You should use df.sparkSession to access the Spark session
                        associated with the DataFrame (df). This way, you ensure that you are working with the correct 
                        Spark session associated with the streaming query.
                    """
                    rows = dfAPI.sparkSession.sql("SELECT COUNT(1) FROM tmp_view").collect()[0][0]
                    print(f"Number of rows: {rows}")
                except Exception as e:
                    logging.error(f"Error counting rows: {e}")
            else:
                logging.warning("Error getting API data.")
        else:
            logging.info("No external trigger received.")
    else:
        logging.warning("DataFrame is empty")


# Generate a unique query name by appending a timestamp
query_name = f"{appName}_{int(time.time())}"
logging.info(query_name)

# Initialize result_query as a global variable
result_query = None

class StartStreamingResource(Resource):
    def __init__(self, spark_session, spark_context, streaming_dataframe, processing_time, checkpoint_path):
        self.spark_session = spark_session
        self.spark_context = spark_context
        self.streaming_dataframe = streaming_dataframe
        self.processing_time = processing_time
        self.checkpoint_path = checkpoint_path
        self.query = None
        super().__init__()

    def get(self):
        global result_query  # Use the global variable
        # Start the streaming process or perform other actions
        result_query = (
            streaming_dataframe.writeStream
                .outputMode('append')
                .option("truncate", "false")
                .foreachBatch(lambda df, batchId: process_data(df, batchId))
                .trigger(processingTime=f'{processing_time} seconds')
                .option('checkpointLocation', checkpoint_path)
                .queryName(f"{query_name}")
                .start()
        )
        return {'status': 'Streaming started.'}
    
    def post(self):
        # Implement logic for handling POST requests
        return {'status': 'POST request handled.'}

class StopStreamingResource(Resource):
    def get(self):
        global result_query  # Use the global variable
        if result_query is not None:
            try:
                result_query.stop()
                return {'status': 'Streaming stopped.'}
            except Exception as e:
                return {'status': f'Error stopping streaming: {str(e)}'}
        else:
            return {'status': 'Streaming not active.'}


# Create instances of Spark-related objects
spark_session = SparkSession.builder.appName(appName).getOrCreate()
spark_context = spark_session.sparkContext
streaming_dataframe = streaming_dataframe
processing_time = 5
checkpoint_path = "file:///ssd/hduser/randomdata/chkpt"
checkpoint_path = checkpoint_path

api.add_resource(
    StartStreamingResource,
    '/start_streaming',
    resource_class_args=(spark_session, spark_context, streaming_dataframe, processing_time, checkpoint_path),
    resource_class_kwargs={}
)

api.add_resource(
    StopStreamingResource,
    '/stop_streaming'
)

if __name__ == '__main__':
    # Explicitly set the hostname and port
    host = '0.0.0.0'  # Allow external connections
    port = 7999
    # Run the Flask app in a separate thread
    flask_thread = threading.Thread(target=app.run, kwargs={'debug': False, 'host': host, 'port': port})
    flask_thread.start()

# Wait for the termination of the streaming query
spark_session.streams.awaitAnyTermination()
