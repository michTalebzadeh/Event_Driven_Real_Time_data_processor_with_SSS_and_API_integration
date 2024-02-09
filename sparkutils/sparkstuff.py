# Importing necessary libraries
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

from pyspark.sql import SQLContext, HiveContext
from src.config import config


def hivecontext():
  return HiveContext(sparkcontext())

def spark_session_local(appName):
    return SparkSession.builder \
        .master('local[1]') \
        .appName(appName) \
        .enableHiveSupport() \
        .getOrCreate()


# Function to create a Spark session

def spark_session(appName):
    """   
    Create a Spark session with Hive support.

    Args:
        appName (str): The name of the Spark application.

    Returns:
        SparkSession: The Spark session.
    """
    
def spark_session(appName, existing_spark_session=None):
    if existing_spark_session:
        return existing_spark_session
    else:
        return SparkSession.builder \
            .appName(appName) \
            .enableHiveSupport() \
            .getOrCreate()
    
# Function to get or create a Spark context
def sparkcontext():

    """
    Get or create a Spark context.

    Returns:
        SparkContext: The Spark context.
    """
    return SparkContext.getOrCreate()
    

# Function to create a Hive context
def hivecontext():
    """
    Create a Hive context.

    Returns:
        HiveContext: The Hive context.
    """
    return HiveContext(sparkcontext())

# Function to create a local Spark session
def spark_session_local(appName):
    """
    Create a local Spark session.

    Args:
        appName (str): The name of the Spark application.

    Returns:
        SparkSession: The local Spark session.
    """
    return SparkSession.builder \
        .master('local[1]') \
        .appName(appName) \
        .enableHiveSupport() \
        .getOrCreate()

# Function to set Spark configurations for Hive
def setSparkConfHive(spark):
    """
    Set Spark configurations for Hive.

    Args:
        spark (SparkSession): The Spark session.

    Returns:
        SparkSession: The Spark session with updated configurations.
    """
    try:
        # Set Hive-related configurations
        spark.conf.set("hive.exec.dynamic.partition", "true")
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        spark.conf.set("spark.sql.orc.filterPushdown", "true")
        spark.conf.set("hive.msck.path.validation", "ignore")
        spark.conf.set("hive.metastore.authorization.storage.checks", "false")
        spark.conf.set("hive.metastore.client.connect.retry.delay", "5s")
        spark.conf.set("hive.metastore.client.socket.timeout", "1800s")
        spark.conf.set("hive.metastore.connect.retries", "12")
        spark.conf.set("hive.metastore.execute.setugi", "false")
        spark.conf.set("hive.metastore.failure.retries", "12")
        spark.conf.set("hive.metastore.schema.verification", "false")
        spark.conf.set("hive.metastore.schema.verification.record.version", "false")
        spark.conf.set("hive.metastore.server.max.threads", "100000")
        spark.conf.set("hive.metastore.authorization.storage.checks", "/usr/hive/warehouse")
        spark.conf.set("hive.stats.autogather", "true")
        spark.conf.set("hive.metastore.disallow.incompatible.col.type.changes", "false")
        spark.conf.set("set hive.resultset.use.unique.column.names", "false")
        spark.conf.set("hive.metastore.uris", "thrift://rhes75:9083")
        return spark
    except Exception as e:
        print(f"{str(e)}, quitting!")
        sys.exit(1)

# Function to set Spark configurations for BigQuery
def setSparkConfBQ(spark):
    """
    Set Spark configurations for BigQuery.

    Args:
        spark (SparkSession): The Spark session.

    Returns:
        SparkSession: The Spark session with updated configurations.
    """
    try:
        # Set BigQuery-related configurations
        spark.conf.set("BigQueryParentProjectId", config['GCPVariables']['projectId'])
        spark.conf.set("BigQueryDatasetLocation", config['GCPVariables']['datasetLocation'])
        spark.conf.set("google.cloud.auth.service.account.enable", "true")
        spark.conf.set("fs.gs.project.id", config['GCPVariables']['projectId'])
        spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        spark.conf.set("temporaryGcsBucket", config['GCPVariables']['tmp_bucket'])
        spark.conf.set("spark.sql.streaming.checkpointLocation", config['GCPVariables']['tmp_bucket'])
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled","true")
        return spark
    except Exception as e:
        print(f"{str(e)}, quitting!")
        sys.exit(1)

# Function to set Spark configurations for Redis
def setSparkConfRedis(spark):
    """
    Set Spark configurations for Redis.

    Args:
        spark (SparkSession): The Spark session.

    Returns:
        SparkSession: The Spark session with updated configurations.
    """
    try:
        spark.conf.set("spark.redis.host", config['RedisVariables']['redisHost'])
        spark.conf.set("spark.redis.port", config['RedisVariables']['redisPort'])
        spark.conf.set("spark.redis.auth", config['RedisVariables']['Redis_password'])
        spark.conf.set("spark.redis.db", config['RedisVariables']['redisDB'])
        return spark
    except Exception as e:
        print(f"{str(e)}, quitting!")
        sys.exit(1)

# Function to set Spark configurations for streaming
def setSparkConfStreaming(spark):
    """
    Set Spark configurations for streaming.

    Args:
        spark (SparkSession): The Spark session.

    Returns:
        SparkSession: The Spark session with updated configurations.
    """
    try:
        spark.conf.set("sparkDefaultParllelism", config['MDVariables']['sparkDefaultParallelism'])
        spark.conf.set("sparkSerializer", config['MDVariables']['sparkSerializer'])
        spark.conf.set("sparkNetworkTimeOut", config['MDVariables']['sparkNetworkTimeOut'])
        spark.conf.set("sparkStreamingUiRetainedBatches", config['MDVariables']['sparkStreamingUiRetainedBatches'])
        spark.conf.set("sparkWorkerUiRetainedDrivers",  config['MDVariables']['sparkWorkerUiRetainedDrivers'])
        spark.conf.set("sparkWorkerUiRetainedExecutors", config['MDVariables']['sparkWorkerUiRetainedExecutors'])
        spark.conf.set("sparkWorkerUiRetainedStages", config['MDVariables']['sparkWorkerUiRetainedStages'])
        spark.conf.set("sparkUiRetainedJobs", config['MDVariables']['sparkUiRetainedJobs'])
        spark.conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
        spark.conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
        spark.conf.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
        spark.conf.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true")
        spark.conf.set("spark.streaming.backpressure.enabled", "true")
        spark.conf.set("spark.streaming.receiver.maxRate", config['MDVariables']['sparkStreamingReceiverMaxRate'])
        spark.conf.set("spark.streaming.kafka.maxRatePerPartition", config['MDVariables']['sparkStreamingKafkaMaxRatePerPartition'])
        spark.conf.set("spark.streaming.backpressure.pid.minRate", config['MDVariables']['sparkStreamingBackpressurePidMinRate'])
       # Add RocksDB configurations here
        spark.conf.set("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelog", "true")
        spark.conf.set("spark.sql.streaming.stateStore.rocksdb.writeBufferSizeMB", "64")  # Example configuration
        # More RocksDB configurations as needed, e.g., compaction settings
        spark.conf.set("spark.sql.streaming.stateStore.rocksdb.compaction.style", "level")
        spark.conf.set("spark.sql.streaming.stateStore.rocksdb.compaction.level.targetFileSizeBase", "67108864")
        return spark
    except Exception as e:
        print(f"{str(e)}, quitting!")
        sys.exit(1)

# Function to load data from a BigQuery table
def loadTableFromBQ(spark, dataset, tableName):
    """
    Load data from a BigQuery table into a Spark DataFrame.

    Args:
        spark (SparkSession): The Spark session.
        dataset (str): The name of the BigQuery dataset.
        tableName (str): The name of the BigQuery table.

    Returns:
        DataFrame: The Spark DataFrame containing the loaded data.
    """
    try:
        read_df = spark.read. \
            format("bigquery"). \
            option("credentialsFile", config['GCPVariables']['jsonKeyFile']). \
            option("dataset", dataset). \
            option("table", tableName). \
            load()
        return read_df
    except Exception as e:
        print(f"{str(e)}, quitting!")
        sys.exit(1)

# Function to write data to a BigQuery table
def writeTableToBQ(dataFrame, mode, dataset, tableName):
    """
    Write data to a BigQuery table from a Spark DataFrame.

    Args:
        dataFrame (DataFrame): The Spark DataFrame containing the data to be written.
        mode (str): The write mode (e.g., 'overwrite', 'append').
        dataset (str): The name of the BigQuery dataset.
        tableName (str): The name of the BigQuery table.
    """
    try:
        dataFrame. \
            write. \
            format("bigquery"). \
            option("credentialsFile", config['GCPVariables']['jsonKeyFile']). \
            mode(mode). \
            option("dataset", dataset). \
            option("table", tableName). \
            save()
    except Exception as e:
        print(f"{str(e)}, quitting!")
        sys.exit(1)

# Function to load data from a JDBC table
def loadTableFromJDBC(spark, url, tableName, user, password, driver, fetchsize):
    """
    Load data from a JDBC table into a Spark DataFrame.

    Args:
        spark (SparkSession): The Spark session.
        url (str): The JDBC connection URL.
        tableName (str): The name of the table to read from.
        user (str): The username for the JDBC connection.
        password (str): The password for the JDBC connection.
        driver (str): The JDBC driver class name.
        fetchsize (int): The number of rows to fetch per round trip.

    Returns:
        DataFrame: The Spark DataFrame containing the loaded data.
    """
    try:
       read_df = spark.read. \
            format("jdbc"). \
            option("url", url). \
            option("dbtable", tableName). \
            option("user", user). \
            option("password", password). \
            option("driver", driver). \
            option("fetchsize", fetchsize). \
            load()
       return read_df
    except Exception as e:
        print(f"{str(e)}, quitting!")
        sys.exit(1)

# Function to write data to a JDBC table
def writeTableWithJDBC(dataFrame, url, tableName, user, password, driver, mode):
    """
    Write data to a JDBC table from a Spark DataFrame.

    Args:
        dataFrame (DataFrame): The Spark DataFrame containing the data to be written.
        url (str): The JDBC connection URL.
        tableName (str): The name of the table to write to.
        user (str): The username for the JDBC connection.
        password (str): The password for the JDBC connection.
        driver (str): The JDBC driver class name.
        mode (str): The write mode (e.g., 'overwrite', 'append').
    """
    try:
        dataFrame. \
            write. \
            format("jdbc"). \
            option("url", url). \
            option("dbtable", tableName). \
            option("user", user). \
            option("password", password). \
            option("driver", driver). \
            mode(mode). \
            save()
    except Exception as e:
        print(f"{str(e)}, quitting!")
        sys.exit(1)

# Function to load data from a Redis table
def loadTableFromRedis(spark, tableName, keyColumn):
    """
    Load data from a Redis table into a Spark DataFrame.

    Args:
        spark (SparkSession): The Spark session.
        tableName (str): The Redis table name.
        keyColumn (str): The name of the column to be used as the key.

    Returns:
        DataFrame: The Spark DataFrame containing the loaded data.
    """
    try:
       read_df = spark.read. \
            format("org.apache.spark.sql.redis"). \
            option("table", tableName). \
            option("key.column", keyColumn). \
            option("infer.schema", True). \
            load()
       return read_df
    except Exception as e:
        print(f"{str(e)}, quitting!")
        sys.exit(1)

# Function to write data to a Redis table
def writeTableToRedis(dataFrame, tableName, keyColumn, mode):
    """
    Write data to a Redis table from a Spark DataFrame.

    Args:
        dataFrame (DataFrame): The Spark DataFrame containing the data to be written.
        tableName (str): The Redis table name.
        keyColumn (str): The name of the column to be used as the key.
        mode (str): The write mode (e.g., 'overwrite', 'append').
    """
    try:
        dataFrame. \
            write. \
            format("org.apache.spark.sql.redis"). \
            option("table", tableName). \
            option("key.column", keyColumn). \
            mode(mode). \
            save()
    except Exception as e:
        print(f"{str(e)}, quitting!")
        sys.exit(1)



