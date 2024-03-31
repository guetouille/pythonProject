import os, time
import common
import pathlib

def purge_older_backups(days_retention):

    user,password,host,port,database,connect_timeout,backup_path,instance_id,url, auth_token, region=common.read_config_flatten("database")

    now = time.time()

    for filename in os.listdir(backup_path):
        filestamp = os.stat(os.path.join(backup_path, filename)).st_mtime
        seven_days_ago = now - days_retention * 86400
        if filestamp < seven_days_ago:
            print("deleting {}".format(filename))
            os.remove(filename)
        else:
                print("not deleting {}".format(filename))


def purge_older_backups_mysql(days_retention):

    user,password,host,port,database,connect_timeout,backup_path,instance_id,url, auth_token, region=common.read_config_flatten("database")

    now = time.time()

    for filename in os.listdir(backup_path):
        filestamp = os.stat(os.path.join(backup_path, filename)).st_mtime
        
        seven_days_ago = now - days_retention * 86400
        if filestamp < seven_days_ago:
            if ("db_short" in filename):
                 print("deleting {}".format(filename))
                 os.remove(backup_path + "/" + filename)
            else:
                print("not deleting {}".format(filename))
            
        else:
             print("not deleting {}".format(filename))
