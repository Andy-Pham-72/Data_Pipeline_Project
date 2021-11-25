#from configparser import Error
import pandas as pd
import mysql.connector
import json

import logging
from logging.handlers import RotatingFileHandler

def setup_logger():
    """function creates log files"""
    MAX_BYTES = 10000000 # Maximum size for a log file

    # The name should be unique, so you can get in in other places
    logger = logging.getLogger(__name__) 
    logger.setLevel(logging.INFO) # the level should be the lowest level set in handlers
    
    # log format
    log_format = logging.Formatter('[%(levelname)s] %(asctime)s - %(message)s')

    # info logger
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_format)
    stream_handler.setLevel(logging.INFO)
    logger.addHandler(stream_handler)

    info_handler = RotatingFileHandler('info.log', maxBytes=MAX_BYTES)
    info_handler.setFormatter(log_format)
    info_handler.setLevel(logging.INFO)
    logger.addHandler(info_handler)

    # error logger
    error_handler = RotatingFileHandler('error.log', maxBytes=MAX_BYTES)
    error_handler.setFormatter(log_format)
    error_handler.setLevel(logging.ERROR)
    logger.addHandler(error_handler)
    return logger

logger = setup_logger()

# open config json file
f = open(input("Type your config file directory:")) # data/config.json

# load json object
config = json.load(f)

# input csv file directory
file_path_csv= input("Type your csv file directory:")

def get_db_connection():
    """ function connects to MySQL server"""
    global connection
    connection = None
    try:
        connection = mysql.connector.connect(**config)
        logger.info('Successfully connected to MySQL Server')
    except Exception as err:
        print("Error while connecting to database for job tracker\nPlease try again!\n", err)
        # log
        logger.error("Error while connecting to database for job tracker: {}".format(err))
        # call again
        get_db_connection()

    return connection

def load_third_party(connection, file_path_csv):

    cursor = connection.cursor()
    # [Iterate through the CSV file and execute insert statement]
    
    try:
        csv_file = pd.read_csv(file_path_csv, header= None, index_col = False, delimiter= ',')
        for i, row in csv_file.iterrows():
            my_sql_query = """INSERT INTO ticketsale (  ticket_id ,
                                                        trans_date ,
                                                        event_id ,
                                                        event_name ,
                                                        event_date ,
                                                        event_type ,
                                                        event_city ,
                                                        customer_id,
                                                        price,
                                                        num_tickets )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
            cursor.execute(my_sql_query, tuple(row))
            print("Record inserted")
            connection.commit()
        logger.info('Successfully insert data to ticketsale table')
    except TypeError as err:
        print("\n*************\nInvalid!\n*************\nPlease try again!\n")
        # Log
        logger.error("Invalid. An error message: {}".format(err))
        
    except ValueError as err:
        print("\n*************\nInvalid input!\n*************\nPlease try again!\n")
        # Log
        logger.error("Invalid input. An error message: {}".format(err))
        
    except Exception as err:
        print("Error while connecting to database for job tracker\nPlease try again!\n", err)
        # log
        logger.error("Error while connecting to database for job tracker: {}".format(err))
        
    return 

def query_popular_tickets(connection):
    """function runs the popular ticket query"""
    
    sql_statement = 'SELECT event_name \
                    FROM ticketsales.ticketsale \
                    ORDER BY num_tickets DESC ,trans_date ASC \
                    LIMIT 3;'
    try:
        cursor = connection.cursor()
        cursor.execute(sql_statement)
        records = cursor.fetchall()
        cursor.close()
        if len(records) < 1:
            logger.info('Successfully implement "the most popular tickets" query. But no updated output.')
        else: 
            print('Here are the most popular tickets in the past month:\n{}'.\
                format('\n'.join(["- " + records[i][0] for i in range(0, len(records))])))
            logger.info('Successfully implement "the most popular tickets" query')
    except Exception as err:
        print("Error while connecting to database for job tracker\nPlease try again!\n", err)
        # log
        logger.error("Error while connecting to database for job tracker: {}".format(err))

if __name__ == '__main__':

    connection = get_db_connection()

    try:
        load_third_party(connection, file_path_csv)
    except TypeError as err:
        print("\n*************\nInvalid!\n*************\nPlease try again!\n")
        # Log
        logger.error("Invalid. An error message: {}".format(err))

    query_popular_tickets(connection)

