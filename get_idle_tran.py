import psycopg2
from psycopg2 import Error
import logging_loki
import smtplib
import logging
from email.mime.text import MIMEText
from pathlib import Path
import common
import datetime

def get_idle_in_tran(connection):

    query = "select * from pg_stat_activity where state != 'idle' and  query_start <  now() - interval '10 minutes' and pid<>pg_backend_pid();"
    message =""
    status=""
    try:
        cursor = connection.cursor()
        conn_detail = connection.get_dsn_parameters()
        # Executing a SQL query
        cursor.execute(query)
        # Fetch result
        record = cursor.fetchone()
        if (record == None ):
            message = " no long process (10 mn)"
            status ='OK'
            print(message)
            #send_mail2(message,conn_detail)
        else:
            print ("process has been found")
            message = "Running process  - ", record, "\n"
            #common.send_mail1(message, conn_detail)
            #print (record[16]) # status active
            #print(record[20])  # query
            status = 'WAR'
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            #print("PostgreSQL connection is closed")
            return message,status,conn_detail

def get_active_conn(connection):
    query = "call monitoring.ins_running_statements();commit;"
    message =""
    status=""
    try:
        cursor = connection.cursor()
        rowcount = cursor.rowcount
        conn_detail = connection.get_dsn_parameters()
        # Executing a SQL query
        cursor.execute(query)       
        #print ("executing query " + query)
        common.logging_info(str(datetime.datetime.now()) + " active connections done with " + str(rowcount) + " rows" )
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection is not None):
            cursor.close()
            connection.close()

def get_db_size(connection,dbname):
    query = "SELECT pg_size_pretty( pg_database_size(%s));", (dbname)
    query = "insert into monitoring.db_size SELECT now(), 'ce-emp-prd', (pg_database_size('{}'));".format(dbname)    
    message =""
    status=""
    cursor = connection.cursor()
    try:
        # Executing a SQL query
        cursor.execute(query)
        
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)     

def run_vacuum_analyze(connection):
    query = "vacuum analyze;"
    connection.autocommit = True
    cursor = connection.cursor()
    
    try:
        cursor.execute(query) 
        
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while running vaccuum analyze ", error)
        common.logging_info(str(datetime.datetime.now()) + " Error while running vaccuum analyze")
    
def get_table_space_detail(connection):
    query = "call monitoring.set_table_details();commit;"
    message =""
    status=""
    try:
        cursor = connection.cursor()
        #conn_detail = connection.get_dsn_parameters()
        cursor.execute(query) 
        rowcount = cursor.rowcount
        common.logging_info(str(datetime.datetime.now()) + "audit table details done   with " + str(rowcount) + " rows" )
        
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL", error)
        common.logging_info(str(datetime.datetime.now()) + "Error running audit table details done")


def send_metric_to_graphana():
    handler = logging_loki.LokiHandler(
            url="https://metrics.cockpit.fr-par.scw.cloud/api/v1/push",
            tags={"job": "metric_from_python"},
            auth=("api_key", "26VdClopfloSKBdNKtK0xj8rkco3UZSZo1sncu82CLgTumOCTYcyRcO-HmpUv5Xt"),
            version="1",
        )
    logger = logging.getLogger("my-first-python-metric")
    logger.addHandler(handler)
    logger.error(
            "Logging a python error with Scaleway cockpit example",
            extra={"tags": {"service": "my-service"}},
               )
