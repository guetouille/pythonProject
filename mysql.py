import pymysql
import subprocess
import time
from pymysql import Error
from time import gmtime, strftime 

def backup_mysql():
    # Define database connection details
    host = "51.159.27.98"
    user = "gperez"
    password = "Letouille0101!!"
    database = "ce-prod"
    backup_file = "db_short_"
    dir = "/root/gp/backup/"
    port = 7347
    db_file_name= backup_file + time.strftime("%Y%m%d%I%M") + ".dmp"
    string_file = dir + backup_file
    # Establish connection with the MySQL database
    connection = pymysql.connect(host=host, user=user, password=password, database=database, port=port)
        
    # Execute the mysqldump command
    #command = f"mysqldump -u {user} --password='{password}' -h {host} -P {port} {database} --ignore-table=ce-prod.ce_login_attempts --ignore-table=ce-prod.search --skip-lock-tables> /root/gp/backup/'{db_file_name}'"
    command = f"mysqldump -u {user} --password='{password}' -h {host} -P {port} {database} --routines --no-data --skip-lock-tables> /root/gp/backup/'{db_file_name}'"
    command2 = f"mysqldump -u {user} --password='{password}' -h {host} -P {port} {database} --no-create-info --ignore-table=ce-prod.ce_login_attempts --ignore-table=ce-prod.search --ignore-table=ce-prod.ce_user_refresh_tokens --skip-lock-tables>> /root/gp/backup/'{db_file_name}'"
    #print (command)
    subprocess.run(command, shell=True)
    subprocess.run(command2, shell=True)

    command = f"gzip /root/gp/backup/{db_file_name}"
    print (command)
    subprocess.run(command, shell=True)
    # Close the database connection
    connection.close()
  
    return db_file_name + ".gz"


def check_process():
         
         host = "51.159.27.98"
         user = "gperez"
         password = "Letouille0101!!"
         database = "ce-prod"
         #backup_file = "backup_short.dmp"
         port: int  = 7347
         db_file_name= "db_short_" + time.strftime("%Y%m%d%I%M") + ".dmp"
         outparam = ""
         result = ""
         databaseConnection = pymysql.connect(host=host, user=user, password=password, db=database,port=port, cursorclass= pymysql.cursors.DictCursor)
         try:
            cursorObject = databaseConnection.cursor()
            # parms = (row['Host Name'], row["OS Type"], row['Host Description'])

            # resultArgs = cursor.callproc('update_servers', inOutParams)
            cursorObject.execute("call mon_get_process2()")
            
            for row in cursorObject.fetchall():
             
             print( row )
             result = row["lastalertTime"]
             
         except (Exception, pymysql.Error) as error:
            print("Error while connecting to database mysql ", error)
         finally:
            databaseConnection.close()
            return str(result)
            