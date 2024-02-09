import yaml
with open("/home/hduser/dba/bin/python/Event_Driven_Real_Time_data_processor_with_SSS_and_API_integration/conf/config.yml", 'r') as file:
  #config: dict = yaml.load(file.read(), Loader=yaml.FullLoader)
  config: dict = yaml.safe_load(file)
  #print(config.keys())
  #print(config['hiveVariables'].items())
  hive_url = "jdbc:hive2://" + config['hiveVariables']['hiveHost'] + ':' + config['hiveVariables']['hivePort'] + '/default'
  oracle_url = "jdbc:oracle:thin:@" + config['OracleVariables']['oracleHost'] + ":" + config['OracleVariables']['oraclePort'] + ":" + config['OracleVariables']['oracleDB']
  mysql_url = "jdbc:mysql://"+config['MysqlVariables']['MysqlHost']+"/"+config['MysqlVariables']['dbschema']


with open("/home/hduser/dba/bin/python/Event_Driven_Real_Time_data_processor_with_SSS_and_API_integration/conf/config_test.yml", 'r') as file:
  #ctest: dict = yaml.load(file.read(), Loader=yaml.FullLoader)
  ctest: dict = yaml.safe_load(file)
  test_url = "jdbc:mysql://"+ctest['statics']['host']+"/"+ctest['statics']['dbschema']






