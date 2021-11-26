import pandas as pd
import mysql.connector
import json
# import module
from setup_logger import logger

# open config json file
f = open("config/config.json") # data/config.json

# load json object
config = json.load(f)

class SqlFuncs:
    """
    class SqlFuncs is created to access the essential mysql.connector's functions
    """

    def __init__(self):
        self._conn = mysql.connector.connect(**config)  # use your password here
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def execute(self, sql, params=None):
        """Implements execute function"""
        self.cursor.execute(sql, params or ())

    def commit(self):
        """Implements commit function"""
        self.connection.commit()

    def fetchall(self):
        """Implements fetchall function"""
        return self.cursor.fetchall()

    def fetchone(self):
        """Implement fetchone function"""
        return self.cursor.fetchone()

    def close(self, commit=True):
        """Implement close function"""
        if commit:
            self.commit()
        self.connection.close()

class ticket_sale(SqlFuncs):

    def __init__(self):
        super().__init__()
    
    def load_third_party(self):
        '''
        function that loads data from ticket sale csv file and insert into the "ticketsale" database
        '''

        try:
            # csv file directory
            self.file_path_csv = "data/third_party_sales.csv"
            csv_file = pd.read_csv(self.file_path_csv, header= None, index_col = False, delimiter= ',')
            # Iterate through the CSV file and execute insert statement
            for i, row in csv_file.iterrows():
                my_sql_query = """ INSERT INTO ticketsale ( ticket_id ,
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
                self.execute(my_sql_query, tuple(row))
                print(f"Record inserted #{i}")
                self.commit()
            logger.info('Successfully insert data to ticketsale table')
        except AttributeError as err:
            # Log
            logger.error("Invalid. An error message: {}".format(err))
        except TypeError as err:
            # Log
            logger.error("Invalid. An error message: {}".format(err))
        except ValueError as err:
            # Log
            logger.error("Invalid input. An error message: {}".format(err))
        except Exception as err:
            # log
            logger.error("Error while connecting to database for job tracker: {}".format(err))

    def query_popular_tickets(self):
        """Function runs the popular ticket query and give an output with the first 3 result as example below:
            " Here are the most popular tickets in the past month:
              - Washington Spirits vs Sky Blue FC
              - The North American International Auto Show
              - Christmas Spectacular 
            "
        """
        # sql query
        sql_statement = 'SELECT event_name \
                        FROM ticketsales.ticketsale \
                        ORDER BY num_tickets DESC ,trans_date ASC \
                        LIMIT 3;'
        try:
            self.execute(sql_statement)
            records = self.fetchall()
            self.close()
            if len(records) < 1:
                logger.info('Successfully implemented "the most popular tickets" query. But no updated output.')
            else: 
                print('\nHere are the most popular tickets in the past month:\n{}'.\
                    format('\n'.join(["- " + records[i][0] for i in range(0, len(records))])))
                logger.info('Successfully implemented "the most popular tickets" query')
        except Exception as err:
            # log
            logger.error("Error while connecting to database for job tracker: {}".format(err))

if __name__ == '__main__':

    connection = ticket_sale()

    connection.load_third_party()

    connection.query_popular_tickets()
