# Import necessary modules
##############################################################################


import pandas as pd
from data_processing_pipeline.constants import *
from data_processing_pipeline.utils import *
from data_processing_pipeline.schema import *
import sqlite3

###############################################################################
# Define function to validate raw data's schema
# ##############################################################################


def raw_data_schema_check():
    """
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        raw_data_schema : schema of raw data in the form oa list/tuple as present
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints
        'Raw datas schema is in line with the schema present in schema.py'
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'


    SAMPLE USAGE
        raw_data_schema_check
    """
    try:
        X = load_data(TRAIN_X_PATH)
        y = load_data(TRAIN_Y_PATH)

        print("Processing Data.....")
        df = pd.concat([X, y], axis=1)
        if sorted(raw_data_schema) == sorted(df.columns):
            print("Raw datas schema is in line with the schema present in schema.py")
        else:
            print(
                "Raw datas schema is NOT in line with the schema present in schema.py"
            )
    except Exception as e:
        print(f"Exception thrown in raw_data_schema_check : {e}")


###############################################################################
# Define function to validate model's input schema
# ##############################################################################


def model_input_schema_check():
    """
    This function check if all the columns mentioned in model_input_schema in
    schema.py are present in table named in 'model_input' in db file.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        raw_data_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'

    SAMPLE USAGE
        raw_data_schema_check
    """
    try:
        cnx = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        df = pd.read_sql("select * from model_input", cnx)
        cnx.close()
        if sorted(model_input_schema) == sorted(df.columns):
            print("Models input schema is in line with the schema present in schema.py")
        else:
            print(
                "Models input schema is NOT in line with the schema present in schema.py"
            )
    except Exception as e:
        print(f"Exception thrown in model_input_schema_check : {e}")
