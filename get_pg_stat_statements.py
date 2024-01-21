import psycopg2
from psycopg2 import Error
import smtplib , datetime, common
import logging
from pathlib import Path
import get_idle_tran
import pandas as pd
from IPython.display import HTML

def mng_reset_pg_stat_statements(connection):
    d = d = datetime.datetime.now()
    day = d.strftime("%d")
    if (day == 1):
        common.logging_info(str(datetime.datetime.now()) + " reset of pg_stat_statements as day {}".format(day))
        reset_pg_stat_statements(connection)
    else:
        common.logging_info(str(datetime.datetime.now()) + "  No reset of pg_stat_statements as day {}".format(day))
        return
          
    
def reset_pg_stat_statements(connection):
    query = "select pg_stat_statements_reset();"
    try:
        cursor = connection.cursor()
        conn_detail = connection.get_dsn_parameters()
        # Executing a SQL query
        cursor.execute(query)
        # Fetch result
        record = cursor.fetchone()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL in reset_pg_stat_statements", error)
    finally:
        return

def get_pg_stat_statements(connection ):
    query = "SELECT  substring(query, 1, 50) AS query,round(total_exec_time::numeric, 2) AS total_time,calls,round(mean_exec_time::numeric, 2) AS mean,round((100 * total_exec_time /sum(total_exec_time::numeric) OVER ())::numeric, 2) AS percentage_cpu FROM    pg_stat_statements where query not like '%insu%' ORDER BY total_time DESC LIMIT 1000;"
    message =""
    status=""
    pd.set_option('colheader_justify', 'center')
    html_string = '''
    <html>
      <head> 
      <title>PG_STAT_STATEMENTS REPORT</title>
      <style>
      .mystyle {{
        font-size: 10pt; 
        font-family: Verdana;
        border-collapse: collapse; 
        border: 1px solid silver;
      }}

      .mystyle td, th {{
        padding: 5px;
      }}

      .mystyle tr:nth-child(even) {{
        background: #E0E0E0;
      }}

      .mystyle tr:hover {{
        background: silver;
        cursor: pointer;
      }}
      </style>
      </head>
      <body>
        <h4>Output #1</h4>
        {table_1}
      </body>
    </html>
    '''
    query2 = 'select pg_stat_statements_reset();'
    conn_param=""
    df_1= pd.DataFrame()
    try:
        cursor = connection.cursor()
        print("PostgreSQL server information")
        conn_param=connection.get_dsn_parameters(), "\n"
        # Executing a SQL query
        df_1 = pd.read_sql_query(query,connection)
        with open('myhtml2.html', 'w') as f:
            f.write(html_string.format(table_1=df_1.to_html(classes='mystyle')))
        f.close()
        #cursor2 = connection.cursor()
        #cursor.execute(query2)
        #connection.close()
    except (Exception, Error) as error:
        print("Error while executing pg_stat_statements query", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
            return (df_1, conn_param)