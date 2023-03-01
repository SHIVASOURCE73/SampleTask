import pandas as pd
import mysql.connector
import logging
import config
# Configure logging
try:
    LOG_FILE_PATH = config.LOG_FILE
except AttributeError:
    LOG_FILE_PATH = 'Mylog.log'

logging.basicConfig(filename=LOG_FILE_PATH, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# MySQL database connection parameters
try:
    DB_config = {
        'user': config.DATABASE['username'],
        'password': config.DATABASE['password'],
        'host': config.DATABASE['host'],
        'port': config.DATABASE['port'],
        'database': config.DATABASE['database']
    }
except KeyError as e:
    print(f"Error: {e} is not defined in config module.")
# Function to read data from the CSV file

def read_data(file_name):
    try:
        # Read CSV file using pandas
        df = pd.read_csv(file_name)
        return df
    except Exception as e:
        logging.error(f"Error while reading data from {file_name}: {str(e)}")
        
def db_read_data(Source_Table):

    # establish a connection to the MySQL database
    cnx = mysql.connector.connect(user=config.SOURCE_DATABASE['username'], password=config.SOURCE_DATABASE['password'],
                                  host=config.SOURCE_DATABASE['host'],
                                  database=config.SOURCE_DATABASE['database'],port = config.SOURCE_DATABASE['port'])

    # define the SQL query to retrieve data from the table
    query = 'SELECT * FROM '+Source_Table

    # use pandas.read_sql() function to execute the query and create a dataframe
    df = pd.read_sql(query, con=cnx)

    # close the database connection
    cnx.close()

    # display the dataframe
    print(df)
    return df


# Function to check if a table exists in the database
def table_exists(cursor, table_name):
    try:
        # Build query to check if the table exists
        query = f"SHOW TABLES LIKE '{table_name}'"
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            return True
        else:
            return False
    except Exception as e:
        logging.error(f"Error while checking if table exists: {str(e)}")

# Function to create a table in the database
def create_table(cursor, table_name):
    try:
        # Build query to create the table
        query = f"CREATE TABLE {table_name} (Field1 VARCHAR(255), Field2 VARCHAR(255), Field3 VARCHAR(255), Field4 VARCHAR(255), Field5 VARCHAR(255), UID CHAR(1))"
        cursor.execute(query)
    except Exception as e:
        logging.error(f"Error while creating table: {str(e)}")

# Function to check if a record already exists in the target table
def record_exists(cursor, table_name, primary_key_values):
    try:
        # Build query to check if the record already exists
        query = f"SELECT * FROM {table_name} WHERE Field1 = '{primary_key_values[0]}' AND Field2 = '{primary_key_values[1]}'"
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            return True
        else:
            return False
    except Exception as e:
        logging.error(f"Error while checking if record exists: {str(e)}")

# Function to insert a new record in the target table
def insert_record(cursor, table_name, row):
    try:
        # Build query to insert the new record
        query = f"INSERT INTO {table_name} (Field1, Field2, Field3, Field4, Field5, UID) VALUES ('{row[0]}', '{row[1]}', '{row[2]}', '{row[3]}', '{row[4]}', 'I')"
        cursor.execute(query)
    except Exception as e:
        logging.error(f"Error while inserting new record: {str(e)}")

# Function to update an existing record in the target table
def update_record(cursor, table_name, row):
    try:
        # Build query to update the existing record
        query = f"UPDATE {table_name} SET Field3 = '{row[2]}', Field4 = '{row[3]}', Field5 = '{row[4]}', UID = 'U' WHERE Field1 = '{row[0]}' AND Field2 = '{row[1]}'"
        cursor.execute(query)
    except Exception as e:
        logging.error(f"Error while updating record: {str(e)}")



# Function to mark 'D' for all records in the target table
def delete_record(cursor, table_name):
    try:
        # Construct DELETE query
        query = f"UPDATE {table_name} SET UID='D' WHERE Field1 <>'-1' AND Field2<>'-1'"

        # Execute query
        cursor.execute(query)
    except Exception as e:
        logging.error(f"Error while deleting record: {str(e)}")


# Function to store data in the target table
def pre_store_data( table_name):
    try:
        # Connect to MySQL database
        cnx = mysql.connector.connect(**DB_config)
        cursor = cnx.cursor()

        # Check if table exists
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()
        if not result:
            # If table doesn't exist, create it
            create_query = f"CREATE TABLE {table_name} (Field1 VARCHAR(255), Field2 VARCHAR(255), Field3 VARCHAR(255), Field4 VARCHAR(255), Field5 VARCHAR(255), UID CHAR(1))"
            cursor.execute(create_query)
        delete_record(cursor, table_name)
        
        # Commit changes and close connection
        cnx.commit()
        cursor.close()
        cnx.close()
    except Exception as e:
        logging.error(f"Error while Pre storing in {table_name}: {str(e)}")


# Function to store data in the target table
def store_data(data, table_name):
    try:
        # Connect to MySQL database
        cnx = mysql.connector.connect(**DB_config)
        cursor = cnx.cursor()

        
        # Loop through each row in the data
        for index, row in data.iterrows():
            # Check if the record already exists in the target table
            primary_key_values = [str(row[0])]
            for i in range(1, len(row)):
                primary_key_values.append(str(row[i]))
            if record_exists(cursor, table_name, primary_key_values):
                # If the record already exists, update it
                update_record(cursor, table_name, row)
            else:
                # If the record doesn't exist, insert it
                insert_record(cursor, table_name, row)
        
        # Commit changes and close connection
        cnx.commit()
        cursor.close()
        cnx.close()
    except Exception as e:
        logging.error(f"Error while storing data in {table_name}: {str(e)}")

if __name__ == '__main__':
    try:
        # Store data in target table
        # Target_table_name = 'SAMPLETABLE'
        try:
            Target_table_name = config.DATABASE['Target_table_name']
        except AttributeError:
            Target_table_name = 'sampletable'
        pre_store_data(Target_table_name)
        
        try:
            Source_Input_Type = config.SOURCE_INPUT_TYPE
        except AttributeError:
            Source_Input_Type = 'CSV'
        
        if Source_Input_Type == 'CSV':
            # Read data from CSV file
            try:
                CSV_FILES = config.CSV_FILES
            except AttributeError:
                CSV_FILES = ['file1.csv']
            for file_name in CSV_FILES:
                data = read_data(file_name)
                store_data(data, Target_table_name)
        else:
            try:
                Source_Table = config.SOURCE_DATABASE['Source_Table']
            except AttributeError:
                Source_Table = 'source_table'
            data = db_read_data(Source_Table)
            store_data(data, Target_table_name)
            
        
        # store_data(data, Target_table_name)
    except Exception as e:
        logging.error(f"Error while running the program: {str(e)}")

