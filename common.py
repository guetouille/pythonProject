import smtplib
import logging
from logging.handlers import RotatingFileHandler
from email.mime.text import MIMEText
from pathlib import Path
import logging_loki
import configparser
import psycopg2
import hashlib
import time
from time import gmtime, strftime
import os
from urllib import request, parse, error
import os
import json
from datetime import datetime,timezone
from dateutil.relativedelta import relativedelta 
import rfc3339      # for date object -> date string
import pytz
import common
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.application import MIMEApplication
import ssl

def send_mail1(message, conn_detail):
    disk_usage_percent,cpu_usage_percent,memory_usage_percent,connection=common.read_config_monitoring('monitoring')
    smtp_obj = smtplib.SMTP('smtp.mail.me.com', 587)
    smtp_obj.starttls()
    smtp_obj.login('gaetan.perez@icloud.com', 'toat-dcbf-hute-cedb')
    pairs = {'name_1': 'gaetan.perez@icloud.com', 'name_2': 'nicolas.lebatteux@gmail.com', 'name_3': 'walid.jlidi@club-employes.com','name_4': 'ghazi.bensaid@club-employes.com'}
    #pairs = {'name_1': 'gaetan.perez@icloud.com'}
    
    try:
        for name in pairs.keys():

            msg = ('From: {}\r\nTo: {}\r\n\r\n Subject: SMTP Alert Postgre Server {}\r\r Hi, an alert has been raised for Postgre Scaleway server {} \r Suspect query using idle tran is {}'.format(smtp_obj.user,
                                                             pairs.get(name),conn_detail,
                                                             str(message), str(message[2])))

            print('Sending email to {} at {}...'.format(name, pairs.get(name)))

            send_status = smtp_obj.sendmail(from_addr=smtp_obj.user,
                                            to_addrs=pairs.get(name),
                                            msg=msg)
            if send_status != {}:
                print('There was a problem sending mail to {}.\n{}'.format(name, send_status))
    finally:
        smtp_obj.quit()

def decodepass(text):
    encrypted = hashlib.sha256('1234').hexdigest()
    decrypted = hashlib.sha256(encrypted).digest()
    print (encrypted)
    print (decrypted)

def send_mail_html(message, conn_detail):
    smtp_obj = smtplib.SMTP('smtp.mail.me.com', 587)
    smtp_obj.starttls()
    smtp_obj.login('gaetan.perez@icloud.com', 'toat-dcbf-hute-cedb')
    #pairs = {'name_1': 'gaetan.perez@icloud.com', 'name_2': 'nicolas.lebatteux@gmail.com','name_3': 'walid.jlidi@club-employes.com', 'name_4': 'ghazi.bensaid@club-employes.com'}
    maillist ="gaetan.perez@icloud.com;nicolas.lebatteux@gmail.com;walid.jlidi@club-employes.com;ghazi.bensaid@club-employes.com"
    #maillist ="gaetan.perez@icloud.com"
    cclist = ""
    smtp_server='smtp.mail.me.com'
    smtp_port = 587
    gmail = 'gaetan.perez@icloud.com'
    password = 'toat-dcbf-hute-cedb'
    
    try:    
        message = MIMEMultipart('mixed')
        message['From'] = ' <{sender}>'.format(sender = "gaetan.perez@icloud.com")
        message['To'] = maillist
        message['CC'] = ''
        message['Subject'] = 'Query report for Postgre Server '
        msg_content = '<h4>Hi There,<br> Query report for Postgre Server {}.</h4>\n'.format(conn_detail)
        body = MIMEText(msg_content, 'html')
        message.attach(body)
        attachmentPath = "myhtml2.html"
        with open(attachmentPath) as attachment:

            p = MIMEApplication(attachment.read(),_subtype="html")	
            p.add_header('Content-Disposition', "attachment; filename= %s" % attachmentPath.split("\\")[-1]) 
            message.attach(p)
            msg_full = message.as_string()
            context = ssl.create_default_context()
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.ehlo()  
                server.starttls(context=context)
                server.ehlo()
                server.login(gmail, password)
                server.sendmail(gmail,maillist.split(";") + (cclist.split(";") if cclist else []),msg_full)
                server.quit()

            print("email sent out successfully")
            print('Sending email to {}...'.format(maillist))
    except Exception as e:
	        print(str(e))   
    
def logging_info(message):
    mypath=Path.cwd()
    logging.basicConfig(filename=str(mypath) + "/gp_monitoring.log", level=logging.INFO)
    logging.info(message)

def send_log_to_graphana():
    handler = logging_loki.LokiHandler(
        url="https://logs.cockpit.fr-par.scw.cloud/loki/api/v1/push",
        tags={"job": "logs_from_python"},
        auth=("api_key", "26VdClopfloSKBdNKtK0xj8rkco3UZSZo1sncu82CLgTumOCTYcyRcO-HmpUv5Xt"),
        version="1",
    )
    logger = logging.getLogger("my-first-python-logger")
    logger.addHandler(handler)
    logger.error(
        "Logging a python error with Scaleway cockpit example",
        extra={"tags": {"service": "my-service"}},
    )
    print("Log sent")
    

def send_mail2(message, conn_detail):
    disk_usage_percent,cpu_usage_percent,memory_usage_percent,connection=common.read_config_monitoring('monitoring')
    smtp_obj = smtplib.SMTP('smtp.mail.me.com', 587)
    smtp_obj.starttls()
    smtp_obj.login('gaetan.perez@icloud.com', 'toat-dcbf-hute-cedb')
    #pairs = {'name_1': 'gaetan.perez@icloud.com', 'name_2': 'nicolas.lebatteux@gmail.com','name_3': 'walid.jlidi@club-employes.com', 'name_4': 'ghazi.bensaid@club-employes.com'}
    pairs = {'name_1': 'gaetan.perez@icloud.com'}
    html = open("myhtml2.html")
    msghtml = MIMEText(html.read(), 'html')
    try:
        for name in pairs.keys():
            msg_content = '<h4>Hi There,<br> Query report for Postgre Server {}.</h4>\n'.format(conn_detail)
            body = MIMEText(msg_content, 'html')
            msg = (
                'From: {}\r\nTo: {}\r\n\r\n Subject: Query report for Postgre Server {}\r\r  {} \r '.format(
                    smtp_obj.user,
                    pairs.get(name), conn_detail,
                     str(message))) 
            msg = msghtml
            print('Sending email to {} at {}...'.format(name, pairs.get(name)))

            send_status = smtp_obj.sendmail(from_addr=smtp_obj.user,
                                                    to_addrs=pairs.get(name),
                                                    msg=msg)
            if send_status != {}:
                print('There was a problem sending mail to {}.\n{}'.format(name, send_status))
                logging.basicConfig('There was a problem sending mail to {}.\n{}'.format(name, send_status), level=logging.ERROR)
    finally:
        smtp_obj.quit()
def logging_info(message):
    mypath=Path.cwd()
    logging.basicConfig(filename=str(mypath) + "/gp_monitoring.log", level=logging.INFO)
    
    logging.info(message)

def logging_info2(message):
    mypath=Path.cwd()
    filename= str(mypath) + "/gp_monitoring.log"
    rfh = logging.handlers.RotatingFileHandler(
        filename,
    mode='a',
    maxBytes=5*1024*1024,
    backupCount=2,
    encoding=None,
    delay=0
    )

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)-25s %(levelname)-8s %(message)s",
        datefmt="%y-%m-%d %H:%M:%S",
        handlers=[
            rfh
        ]
    )

    logger = logging.getLogger('main')
    logger.debug(message)

def send_log_to_graphana():
    handler = logging_loki.LokiHandler(
        url="https://logs.cockpit.fr-par.scw.cloud/loki/api/v1/push",
        tags={"job": "logs_from_python"},
        auth=("api_key", "26VdClopfloSKBdNKtK0xj8rkco3UZSZo1sncu82CLgTumOCTYcyRcO-HmpUv5Xt"),
        version="1",
    )
    logger = logging.getLogger("my-first-python-logger")
    logger.addHandler(handler)
    logger.error(
        "Logging a python error with Scaleway cockpit example",
        extra={"tags": {"service": "my-service"}},
    )
    print("Log sent")
    
def read_config(section):
    config_object = configparser.ConfigParser()
    with open("config.ini", "r") as file_object:
        config_object.read_file(file_object)
        url = config_object.get(section, "url")
        connection = psycopg2.connect(user=config_object.get(section,'user'),
        password=config_object.get(section,'password'),
        host=config_object.get(section,'host'),
        port=config_object.get(section,'port'),
        database=config_object.get(section,'database'),
        connect_timeout=config_object.get(section,'ctimeout'))
        return (connection)
    
def read_config_monitoring(section):
    config_object = configparser.ConfigParser()
    with open("config.ini", "r") as file_object:
        config_object.read_file(file_object)
        disk_usage_percent=config_object.get(section,'disk_usage_percent')
        cpu_usage_percent=config_object.get(section,'disk_usage_percent')
        memory_usage_percent=config_object.get(section,'memory_usage_percent')
        connection=config_object.get(section,'connection')
    return (disk_usage_percent,cpu_usage_percent,memory_usage_percent,connection)
 
def read_config_flatten(section):
    config_object = configparser.ConfigParser()
    with open("config.ini", "r") as file_object:
        config_object.read_file(file_object)
        user=config_object.get(section,'user')
        password=config_object.get(section,'password')
        host=config_object.get(section,'host')
        port=config_object.get(section,'port')
        database=config_object.get(section,'database')
        connect_timeout=config_object.get(section,'ctimeout')
        backup_path=config_object.get(section,'backup_path')
        instance_id=config_object.get(section,'instance_id')
        backup_api=config_object.get(section,'backup_api')
        secret= config_object.get(section,'secret')       
        region= config_object.get(section,'region')   
    return (user,password,host,port,database,connect_timeout,backup_path,instance_id,backup_api,secret,region)
        
def create_essentials():        
    user,password,host,port,database,ctimeout,backup_path,instance_id,backup_api,secret,region=read_config_flatten("database")
    db_name = database
    db_user = user
    db_password = password
    db_host = host
    db_port = port
    backup_path = backup_path
    filename = "backup_prod_direct"
    filename = filename + "-" + time.strftime("%Y%m%d") + ".backup.dmp"
    command_str = str(db_host)+" -p "+str(db_port)+" -d "+db_name+" -U "+db_user
    return command_str, backup_path, filename,password

def create_essentials_restore():        
    user,password,host,port,database,ctimeout,backup_path=read_config_flatten("database_to_reload")
    db_name = database
    db_user = user
    db_password = password
    db_host = host
    db_port = port
    backup_path = backup_path
    filename = "backup_prod_direct"
    filename = filename + "-" + time.strftime("%Y%m%d") + ".backup.dmp"
    command_str = str(db_host)+" -p "+str(db_port)+" -d "+db_name+" -U "+db_user
    return command_str, backup_path, filename, db_password
    
def backup_database(table_names=None):
    command_str,backup_path,filename,pasword = create_essentials()
    command_str = "pg_dump -h "+command_str
    
    export = "PGPASSWORD={} ".format(pasword)
    if table_names is not None:
        for x in table_names:
            command_str = command_str +" -t "+x
    
    command_str = export + command_str + " -F c -b -v -x -O -f '"+backup_path+"/"+filename+"'"
    print (command_str)
    logging.info(command_str)
    try:
        os.system(command_str)

    except Exception as e:
        print ("Error  in the backup")
        print (e)

def restore_database(table_names=None):
    
    command_str,backup_path,filename, db_password = create_essentials_restore()
    command_str = "pg_restore -h "+command_str
    #os.system("export PGPASSWORD=Test_user01")
    export = "PGPASSWORD=" + db_password + " "
    if table_names is not None:
        for x in table_names:
            command_str = command_str +" -t "+x

    command_str = export + command_str + " -c --no-owner --role test_user -v '"+backup_path+"/"+filename+"'"
    print ("Restoring database test")
    print (command_str)
    try:
        os.system(command_str)
        print (command_str)
    except Exception as e:
        print ("!!Error occured for restore!!")
        print (e)

def set_timezone(region):
    latz= pytz.timezone(region)
    return latz

def get_date_string(date_object):
  return rfc3339.rfc3339(date_object)

def run_backup_scaleway():
        user,password,host,port,database,connect_timeout,backup_path,instance_id,url, auth_token,region=read_config_flatten("database")
        latz = set_timezone("America/Los_Angeles")
        mydate = datetime.now(latz).replace(year=datetime.now().year + 1)   
        iso_date= mydate.replace(microsecond=0).isoformat()
        iso_date = get_date_string(mydate)
        print ("runnning backup expires at {}".format(iso_date))
        data=json.dumps({"database_name": "{}".format(database),
                        "instance_id": "{}".format(instance_id),
                        "name": "Backup_End_Of_Month",
                        "expires_at":"{}".format(iso_date)
                        }).encode('ascii')
        req = request.Request(url, data=data,  method="POST")
        req.add_header('Content-Type', 'application/json')
        req.add_header('X-Auth-Token', auth_token)
        # Sending request to Instance API
        try:
            res=request.urlopen(req).read().decode()
        except error.HTTPError as e:
            res=e.read().decode()
            print (res)
            logging_info(str(datetime.datetime.now()) + "   res")
        return {
            "body": json.loads(res),
            "headers": {
            "Content-Type": ["application/json"],
            },
            "statusCode": 200,
        }
        try:
            res = request.urlopen(req).read().decode()
        except error.HTTPError as e:
            res = e.read().decode()
            return {
            "body": json.loads(res),
            "headers": {
            "Content-Type": ["application/json"],
                    },
        "statusCode": 200,
            }
