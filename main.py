import sys , mysql
import common, filemng, metrics
import get_idle_tran
import get_pg_stat_statements
import datetime

if __name__ == '__main__':
    params = sys.argv[1:]
    mode= ""
    connection =""
    print ("starting")
   #common.decodepass("toto")
    
    for opt in params:
        
        print("Mode is: " + opt)
        
        if opt in ("sql", "statement"):
            connection = common.read_config("database")
            print (connection)
            mode = "pg_stats_statements"
            sql = get_pg_stat_statements.get_pg_stat_statements(connection)
            print(str(sql[0]))
            common.send_mail_html(sql[0], sql[1])
            get_pg_stat_statements.mng_reset_pg_stat_statements(connection)
        elif opt in ("idle"):
            connection = common.read_config("database")
            message = get_idle_tran.get_idle_in_tran(connection)
            if (message[1] == "OK"):
                print (message)
                common.logging_info(str(datetime.datetime.now()) + message[0])                
            else:
                common.logging_info(str(datetime.datetime.now()) + str(message[0]))
                common.send_mail1(message[0], message[1])
        elif opt in ("connection"):
            connection = common.read_config("database")
            print (connection)
            message = get_idle_tran.get_active_conn(connection)
            common.logging_info(str(datetime.datetime.now()) + "audit connection done")
            
        elif opt in ("table_details"):
            connection = common.read_config("database")
            #print (connection)
            message = get_idle_tran.get_table_space_detail(connection)
            common.logging_info(str(datetime.datetime.now()) + "audit table details done")
            message=get_idle_tran.get_db_size(connection,'ce-emp-prd')
         
        elif opt in ("backup_pg"):
            connection = common.read_config("database")
            print (connection)
            message = common.backup_database()
            common.logging_info(str(datetime.datetime.now()) + " local backup done")
            common.logging_info(str(datetime.datetime.now()) + " purging local backups")
            filemng.purge_older_backups(30)
            common.logging_info(str(datetime.datetime.now()) + " Calling backup in Scaleway")
            common.run_backup_scaleway()
        elif opt in ("restore"):
            connection = common.read_config("database")
            common.restore_database()
            common.logging_info(str(datetime.datetime.now()) + " restore done")
        elif opt in ("vacuum"):
            connection = common.read_config("database")
            common.logging_info(str(datetime.datetime.now()) + "  running vaccuum analyze")
            get_idle_tran.run_vacuum_analyze(connection)
        elif opt in ("metrics"):
            common.logging_info(str(datetime.datetime.now()) + "  running check metrics")
            tab=metrics.load_metrics()
            metrics.mng_metrics(tab)
        elif opt in ("backup_mysql"):
            mysql.backup_mysql()
            filemng.purge_older_backups_mysql(10)
        
    #get_idle_tran.send_metric_to_graphana()
   # get_idle_tran.send_mail2()
