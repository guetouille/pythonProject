from datetime import datetime,timezone
from dateutil.relativedelta import relativedelta 
import common ,json
from urllib import request, parse, error
from psycopg2 import Error

def mng_metrics(dic):      
      disk_usage_percent,cpu_usage_percent,memory_usage_percent,connection=common.read_config_monitoring('monitoring')
      #print(dic['timeseries'])
      #print(dic)
      for i in dic['timeseries']:
                name = i['name']    
                node= i['metadata']
                #print (node)
                #if (name == 'disk_usage_percent'):
                #print (i['name'])
                #print("points:", i['points'])
                for p,value in i['points']:
                                #message = ("param name {} , value : {} , limit : {} for date {} and node {}".format(name,value,disk_usage_percent,p,node))
                                #print (message)
                                if (name == "disk_usage_percent"):
                                    if (float(value) >= int(disk_usage_percent)):
                                        message = ("param name {} , value : {} , limit : {} at date {} and for node {}".format(name,value,disk_usage_percent,p,node))
                                        common.logging_info(str(datetime.now()) + message)
                                        common.send_mail1("Alert on Postgre", message)
                                if (name == "cpu_usage_percent"):
                                    if (float(value) >= int(cpu_usage_percent)):
                                        message = ("param name {} , value : {} , limit : {} at date {} and for node {}".format(name,value,cpu_usage_percent,p,node))
                                        common.logging_info(str(datetime.now()) + message)
                                        common.send_mail1("Alert on Postgre", message)
                                if (name =="memory_usage_percent"):
                                    if (float(value) >= int(memory_usage_percent)):
                                        message = ("param name {} , value : {} , limit : {} at date {} and for node {}".format(name,value,memory_usage_percent,p,node))
                                        common.logging_info(str(datetime.now()) + message)
                                        common.send_mail1("Alert on Postgre", message)   
                                if (name == "total_connections"):
                                    if (int(value) >= int(connection) ):
                                        common.logging_info(str(datetime.now()) + message)
                                        common.send_mail1("Alert on Postgre", message)
      #load_metrics(connection,disk_usage_percent,cpu_usage_percent,memory_usage_percent,connection)                              


def load_metrics():
        user,password,host,port,database,connect_timeout,backup_path,instance_id,backup_api,secret,region=common.read_config_flatten("database")
        
        latz = common.set_timezone("America/Los_Angeles")
        mydate = datetime.now(latz)
        iso_date= mydate.replace(microsecond=0).isoformat()
        iso_date = common.get_date_string(mydate)
        req = request.Request("https://api.scaleway.com/rdb/v1/regions/fr-par/instances/18a9aa07-7f31-4386-9c6c-858a33559d7a/metrics", method="GET")
        req.add_header('Content-Type', 'application/json')
        req.add_header('X-Auth-Token', secret)
        # Sending request to Instance API
        try:
            res=request.urlopen(req).read().decode()
            dic=json.loads(res)
        except error.HTTPError as e:
            res=e.read().decode()
            print (res)
            common.logging_info(str(datetime.datetime.now()) + "   res")
        return  dic
       
def insert_metrics(connection,disk_usage_percent,cpu_usage_percent,memory_usage_percent,totalconnection):
        query = "insert into monitoring.server_mertics values ();"
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
            print("metrics inserted")