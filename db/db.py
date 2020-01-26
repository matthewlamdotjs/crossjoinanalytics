import psycopg2
import os

def call_proc(procname, arguments):

    # grab credentials
    try:
        DB_URL = os.environ['CJ_DB_URL']
        DB_PORT = os.environ['CJ_DB_PORT']
        DB_USER = os.environ['CJ_DB_UN']
        DB_PASS = os.environ['CJ_DB_PW']
    except:
        print('Missing credentials. Please set environment variables appropriately.')
        return 0

    try:
        connection = psycopg2.connect(user = DB_USER,
                                    password = DB_PASS,query_string
                                    host = DB_URL,
                                    port = DB_PORT,
                                    database = 'postgres')
        connection.autocommit = True
        cursor = connection.cursor()

        
        query_string = ('CALL "'+ procname +'"(\''+
            symbol +'\',\''+
            key +'\',\''+
            ppd['2. high'] +'\',\''+
            ppd['3. low'] +'\',\''+
            ppd['1. open'] +'\',\''+
            ppd['4. close'] +'\');')
            
        cursor.execute()
        

    except (Exception, psycopg2.Error) as error :
        print ('Error while connecting to PostgreSQL', error)
    finally:
        #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')
            return 1
        else:
            return 0

        