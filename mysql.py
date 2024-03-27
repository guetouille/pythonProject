import pymysql
import subprocess
import time
from time import gmtime, strftime


def backup_mysql():
    # Define database connection details
    host = "51.159.27.98"
    user = "gperez"
    password = "Letouille0101!!"
    database = "ce-prod"
    backup_file = "backup_short.dmp"
    port = 7347
    db_file_name= "db_short_" + time.strftime("%Y%m%d%I%M") + ".dmp"
    # Establish connection with the MySQL database
    connection = pymysql.connect(host=host, user=user, password=password, database=database, port=port)
    
    # Execute the mysqldump command
    #command = f"mysqldump −h{host} −u{user} −-password='{password}' -P{port} {database} > {backup_file}"
    command = f"mysqldump -u {user} --password='{password}' -h {host} -P {port} {database} --ignore-table=ce-prod.ce_login_attempts > /root/gp/backup/'{db_file_name}'"
    print (command)
    subprocess.run(command, shell=True)

    # Close the database connection
    connection.close()