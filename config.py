# config.py
SOURCE_INPUT_TYPE = "CSV" # Give INPUT_TYPE type as either 'CSV' or DB, IF INPUT_TYPE is CSV then CSV_FILES value will be considered else Source_Table to store the data into Target Table
DATABASE = {
    'driver': 'mysql',
    'host': 'localhost',
    'username': 'root',
    'password': 'Shiva@73',
    'database': 'sampletest',
    'port': 3306,
    'Target_table_name': 'Targettable'
}

LOG_FILE = 'mylog.txt'
CSV_FILES = ['file1.csv','file2.csv'] # Sample CSV_FILES ['file1.csv','file2.csv','file3.csv']
SOURCE_DATABASE = {
    'driver': 'mysql',
    'host': 'localhost',
    'username': 'root',
    'password': 'Shiva@73',
    'database': 'sampletest',
    'port': 3306,
    'Source_Table': 'sourcetable'
}
