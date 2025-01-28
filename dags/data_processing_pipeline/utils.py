import pandas as pd
import os
import sqlite3
from sqlite3 import Error
from data_processing_pipeline.constants import *


def load_data(file_path):
    """
    Description: Load the data
    input: file path
    output : Dataframe
    """
    if "test" in file_path:
        return pd.read_parquet(file_path)
    return pd.read_parquet(file_path, index_col=[0])


def check_if_table_has_value(cnx, table_name):
    """
    Description: Checks whether the table in the database connection has any values
    input: database connection and table name
    output: boolean
    """
    # cnx = sqlite3.connect(db_path+db_file_name)
    check_table = pd.read_sql(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';",
        cnx,
    ).shape[0]
    return check_table == 1


def build_dbs():
    """
    This function checks if the db file with specified name is present
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates
    the db file with the given name at the given path.


    INPUTS
        db_file_name : Name of the database file 'utils_output.db'
        db_path : path where the db file should be '


    OUTPUT
    The function returns the following under the conditions:
        1. If the file exsists at the specified path
                prints 'DB Already Exsists' and returns 'DB Exsists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db
                file at the specified path with the specified name and
                once the db file is created prints 'New DB Created' and
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    """
    if os.path.isfile(DB_PATH + DB_FILE_NAME):
        print("DB Already Exsist")
        print(os.getcwd())
        return "DB Exsist"
    else:
        print("Creating Database")
        """ create a database connection to a SQLite database """
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
            print("New DB Created")
            return "DB created"
        except Error as e:
            print(e)
            return "Error creating DB " + DB_PATH + DB_FILE_NAME
        # closing the connection once the database is created
        finally:
            if conn:
                conn.close()


def load_data_into_db():
    """
    Thie function loads the data present in datadirectiry into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' with 0.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        data_directory : path of the directory where 'leadscoring.csv'
                        file is present


    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    """
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)

        if not check_if_table_has_value(conn, "loaded_data"):
            _extracted_from_load_data_into_db(conn)
        else:
            print("loaded_data already populated")
    except Exception as e:
        print(f"Exception thrown in load_data_into_db : {e}")
    finally:
        if conn:
            conn.close()


def _extracted_from_load_data_into_db(conn):
    print("Loading data from " + TRAIN_X_PATH)
    X = load_data(TRAIN_X_PATH)
    y = load_data(TRAIN_Y_PATH)

    print("Processing Data.....")
    df = pd.concat([X, y], axis=1)

    print("Storing processed df to loaded_data table")
    df.to_sql(name="loaded_data", con=conn, if_exists="replace", index=False)
