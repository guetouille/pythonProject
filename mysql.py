import pymysql
import subprocess

def backup_mysql():
    # Define database connection details
    host = "51.159.27.98"
    user = "gperez"
    password = "Letouille0101!!"
    database = "ce-prod"
    backup_file = "backup_short.sql"
    port = 7347

    # Establish connection with the MySQL database
    connection = pymysql.connect(host=host, user=user, password=password, database=database, port=port)
    
    # Execute the mysqldump command
    #command = f"mysqldump −h{host} −u{user} −-password='{password}' -P{port} {database} > {backup_file}"
    command = f"mysqldump -u {user} --password='{password}' -h {host} -P {port} {database} --ignore-table=ce-prod.ce_login_attempts > db_short.sql"
    #print (command)
    subprocess.run(command, shell=True)

    # Close the database connection
    connection.close()