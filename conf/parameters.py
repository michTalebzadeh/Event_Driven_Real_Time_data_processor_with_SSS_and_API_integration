# define font dictionary
font = {'family': 'serif',
        'color':  'darkred',
        'weight': 'normal',
        'size': 10,
        }
# define font dictionary
font_small = {'family': 'serif',
        'color':  'darkred',
        'weight': 'normal',
        'size': 7,
        }
# Hive variables
DB = "pycharm"
tableName = "randomDataPy"
fullyQualifiedTableName = DB + '.' + tableName
tempView = "tmp"
settings = [
      ("hive.exec.dynamic.partition", "true"),
      ("hive.exec.dynamic.partition.mode", "nonstrict"),
      ("spark.sql.orc.filterPushdown", "true"),
      ("hive.msck.path.validation", "ignore"),
      ("spark.sql.caseSensitive", "true"),
      ("spark.speculation", "false"),
      ("hive.metastore.authorization.storage.checks", "false"),
      ("hive.metastore.client.connect.retry.delay", "5s"),
      ("hive.metastore.client.socket.timeout", "1800s"),
      ("hive.metastore.connect.retries", "12"),
      ("hive.metastore.execute.setugi", "false"),
      ("hive.metastore.failure.retries", "12"),
      ("hive.metastore.schema.verification", "false"),
      ("hive.metastore.schema.verification.record.version", "false"),
      ("hive.metastore.server.max.threads", "100000"),
      ("hive.metastore.authorization.storage.checks", "/apps/hive/warehouse"),
      ("hive.stats.autogather", "true")
]
rowsToGenerate = 10

# oracle variables
driverName = "oracle.jdbc.OracleDriver"
_username = "scratchpad"
_password = "oracle"
_dbschema = "SCRATCHPAD"
_dbtable = "DUMMY"
dump_dir = "d:/temp/"
filename = 'DUMMY.csv'
oracleHost = 'rhes564'
oraclePort = '1521'
oracleDB = 'mydb12'
url= "jdbc:oracle:thin:@"+oracleHost+":"+oraclePort+":"+oracleDB
serviceName = oracleDB + '.mich.local'
DB2 = "oraclehadoop"
table2 = "sales"

# aerospike variables
dbHost = "rhes75"
dbPort = 3000
dbConnection = "mich"
namespace = "test"
dbPassword = "aerospike"
dbSet = "oracletoaerospike2"
dbKey = "ID"

# GCP variables
projectId = 'axial-glow-224522'
datasetLocation = "europe-west2"
bucketname = 'etcbucket'
sourceDataset = "staging"
sourceTable = "ukhouseprices"
inputTable = sourceDataset+"."+sourceTable
fullyQualifiedInputTableId = projectId+":"+inputTable
targetDataset = "ds"
targetTable = "summary"
outputTable = targetDataset+"."+targetTable
fullyQualifiedoutputTableId = projectId+":"+outputTable
tmp_bucket = "tmp_storage_bucket/tmp"
jsonKeyFile = "/home/hduser/GCPFirstProject-d75f1b3a9817.json"

# GCP table schema
col_names = ['ID', 'CLUSTERED', 'SCATTERED','RANDOMISED', 'RANDOM_STRING', 'SMALL_VC', 'PADDING']
col_types = ['FLOAT', 'FLOAT', 'FLOAT', 'FLOAT', 'STRING', 'STRING', 'STRING']
col_modes = ['REQUIRED', 'NULLABLE', 'NULLABLE', 'NULLABLE', 'NULLABLE', 'NULLABLE', 'NULLABLE']

# DS stuff
DSDB = "DS"
regionname = "Kensington and Chelsea"
Boston_csvlocation="hdfs://rhes75:9000/ds/Boston.csv"
London_csvLocation="hdfs://rhes75:9000/ds/UK-HPI-full-file-2020-01.csv"