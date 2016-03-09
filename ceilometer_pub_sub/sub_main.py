#!/usr/bin/python
import socket,thread
import sys
import msgpack
import fnmatch
import operator
import logging
import logging.handlers
import logging.config
import ConfigParser
import json
from oslo_utils import units
from oslo_utils import netutils
from pubrecords import *
import pubsub_database
import kafka
import kafka_broker
import shelve

from flask import request, Request, jsonify
from flask import Flask
from flask import make_response
app = Flask(__name__)

COMPARATORS = {
    'gt': operator.gt,
    'lt': operator.lt,
    'ge': operator.ge,
    'le': operator.le,
    'eq': operator.eq,
    'ne': operator.ne,
}

LEVELS = {'DEBUG': logging.DEBUG,
          'INFO': logging.INFO,
          'WARNING': logging.WARNING,
          'ERROR': logging.ERROR,
          'CRITICAL': logging.CRITICAL}

_DEFAULT_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

''' Stores all the subscribed meter's list '''
meter_list = []
''' Stores meter to app-id mapping '''
meter_dict = {}

@app.route('/subscribe',methods=['POST','SUB'])
def subscribe():
    try :
        app_id = request.json['app_id']
        target = request.json['target']
        sub_info = request.json['sub_info']

        try :
            validate_sub_info(sub_info)
        except Exception as e:
            logging.error("* %s",e.__str__())
            return e.__str__()

        ''' Flag to Update pipeling cfg file '''
        config = ConfigParser.ConfigParser()
        config.read('pub_sub.conf')
        if config.get('RABBITMQ','UpdateConfMgmt') == "True" : 
            update_pipeline_conf(sub_info,target,app_id,"ADD")
        else:
            logging.warning("Update Conf Mgmt flag is disabled,enable the flag to  update Conf Mgmt")

        if not 'query' in request.json.keys():
            logging.info("query request is not provided by user")
            query = None 
        else:
             query = request.json['query']
             for i in range(len(query)):
                 if not 'field' in query[i].keys():
                     err_str = "Query field"
                     raise Exception (err_str)
                 if not 'op' in query[i].keys():
                     err_str = "Query op"
                     raise Exception (err_str)
                 if not 'value' in query[i].keys():
                     err_str = "Query value" 
                     raise Exception (err_str)
    except Exception as e:
        err_str = "KeyError: Parsing subscription request " + e.__str__() + "\n"
        logging.error("* KeyError: Parsing subscription request :%s",e.__str__())  
        return err_str 

    parse_target=netutils.urlsplit(target)
    if not parse_target.netloc:
        err_str = "Error:Invalid target format"
        logging.error("* Invalid target format")
        return err_str 

    status = "" 
    if parse_target.scheme == "udp" or  parse_target.scheme == "kafka":
         host,port=netutils.parse_host_port(parse_target.netloc)
         scheme = parse_target.scheme
         app_ip = host 
         app_port = port
 
         if host == None or port == None :
             err_str = "* Error: Invalid IP Address format"
             logging.error("* Invalid IP Address format")
             return err_str
  
         subscription_info = sub_info
         sub_info_filter = query 
         logging.info("Creating subscription for app:%s for meters:%s with filters:%s and target:%s",app_id, subscription_info, sub_info_filter, target)
         subscrip_obj=subinfo(scheme,app_id,app_ip,app_port,subscription_info,sub_info_filter,target)
         status = subscrip_obj.update_subinfo()
         subinfo.print_subinfo()
         if config.get('CLIENT','UpdateDatabase') == "True" :
             database_obj=pubsub_database.database(scheme,app_id,app_ip,app_port,subscription_info,sub_info_filter,target)
             database_obj.add_to_database()
         else:
             logging.warning("Update Database flag is disabled,enable the flag to  update database")

    if parse_target.scheme == "file" :
         pass
    return status 

@app.route('/unsubscribe',methods=['POST','UNSUB'])
def unsubscribe():
    try :  
        app_id = request.json['app_id']
        sub_info,target = subinfo.get_subinfo(app_id)
        if sub_info is None or target is None:
            err_str = "No subscription exists with app id: " + app_id + "\n"
            logging.error("* No subscription exists with app id:%s ",app_id)
            ''' the below statement removing from database is temp placed here '''
            pubsub_database.database.delete_from_database(app_id)
            return err_str 
        else:
            ''' Flag to Update pipeling cfg file '''
            config = ConfigParser.ConfigParser()
            config.read('pub_sub.conf')
            if config.get('RABBITMQ','UpdateConfMgmt') == "True" :
                update_pipeline_conf(sub_info,target,app_id,"DEL")
            else:
                logging.warning("Update Conf Mgmt flag is disabled,enable the flag to  update Conf Mgmt")
            #update_pipeline_conf(sub_info,target,"DEL")
            subinfo.delete_subinfo(app_id)
            if config.get('CLIENT','UpdateDatabase') == "True" :
                pubsub_database.database.delete_from_database(app_id)
            else:
                logging.warning("Update Database flag is disabled,enable the flag to  update database")
    except Exception as e:
         logging.error("* %s",e.__str__())
         return e.__str__()
    return "UnSubscrition is sucessful! \n"

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

def print_subscribed_meter_list():
    logging.debug("-------------------------------------------------")
    #print (meter_list)
    logging.debug("meter_list:%s",meter_list)
    logging.debug("meter_dict:%s",meter_dict)
    #print (meter_dict)
    logging.debug("-------------------------------------------------")

def validate_sub_info(sub_info):
    if type(sub_info) is not list:
        sub_info = [sub_info]
    for meter in sub_info:
        if meter.startswith("*") or meter.startswith("!"):
            err_str = "Given meter is not supported:" + meter + "\n"
            logging.error("* Given meter is not supported:%s",meter)
            raise Exception (err_str)

def update_meter_dict(meterinfo,app_id):
    try :
         if type(meterinfo) == list:
             for meter in meterinfo:  
                 if meter_dict.get(meter) is None:
                     meter_dict[meter] = [app_id]
                 elif app_id not in meter_dict.get(meter):
                     meter_dict.get(meter).append(app_id)
         else:
             if meter_dict.get(meterinfo) is None:
                 meter_dict[meterinfo] = [app_id]
             elif app_id not in meter_dict.get(meterinfo):
                 meter_dict.get(meterinfo).append(app_id)
    except Exception as e:
         logging.error("* %s",e.__str__())

def check_send_msg_confmgmt_del(sub_info,app_id):
    temp_sub_info=[]
    temm_meter_info = None
    if len(meter_list) == 0:
        #print("No subscription exists")
        logging.info("No subscription exists")
        return False,None
    if type(sub_info) == list:
       for meterinfo in sub_info:
           if meter_dict.get(meterinfo) is None:
              logging.warning("%s meter doesn't exist in the meter dict",meterinfo)
              continue 
           if app_id in meter_dict.get(meterinfo):
               if len(meter_dict.get(meterinfo)) == 1:
                   #print "Only single app is subscribing this meter"
                   logging.info("Only single app is subscribing this meter")
                   del meter_dict[meterinfo]
                   temp_sub_info.append(meterinfo)
                   if meterinfo in meter_list:
                       meter_list.remove(meterinfo)
               else:
                   meter_dict.get(meterinfo).remove(app_id)  
       return True,temp_sub_info 
    else :
         if meter_dict.get(sub_info) is None:
              logging.warning("%s meter doesn't exist in the meter dict",sub_info)
              return False,None 
         if app_id in meter_dict.get(sub_info):
             if len(meter_dict.get(sub_info)) == 1:
                  #print "Only single app is subscribing this meter"
                  logging.info("Only single app is subscribing this meter")
                  del meter_dict[sub_info]
                  if sub_info in meter_list:
                     meter_list.remove(sub_info)
                  return True,sub_info   
             else:
                 meter_dict.get(sub_info).remove(app_id)
    return False,None 
     
def check_send_msg_confmgmt_add(sub_info,app_id):
    temp_sub_info=[]
    update_meter_dict(sub_info,app_id)
    #import pdb;pdb.set_trace()
    if len(meter_list) == 0:
        logging.info("No subinfo exits")
        if type(sub_info) == list:
            for j in sub_info:
                meter_list.append(j)
            return True,sub_info
        else :
            meter_list.append(sub_info)
            return True,sub_info
    if type(sub_info) == list:
        for j in sub_info:
            if j in meter_list:
                #print ("meter already exists",j)
                logging.info("meter already exist:%s",j)
                continue
            else :
                 temp_sub_info.append(j)  
                 meter_list.append(j)
        if temp_sub_info is not None:
            return True,temp_sub_info
        else :
            return False,None
    else :
         if sub_info not in meter_list:         
             meter_list.append(sub_info)
             #print ("subscription for  meter doesn't exist",sub_info)
             logging.warning("subscription for  meter doesn't exist:%s",sub_info)
             return True,sub_info
         else :  
             #print ("subscription already exist for ",sub_info)
             logging.info("subscription already exist for:%s ",sub_info)
             return False,sub_info         

def update_pipeline_conf(sub_info,target,app_id,flag):
    import pika

    logging.debug("* sub_info:%s",sub_info)
    logging.debug("* target:%s",target)
  
    #msg={"sub_info":sub_info,"target":target,"action":flag}
    
    #json_msg=json.dumps(msg)
    #msg="image"
    meter_sub_info = None
    if flag == "ADD":
       status,meter_sub_info=check_send_msg_confmgmt_add(sub_info,app_id)
       if status == False or meter_sub_info == None or meter_sub_info == []:
           logging.warning("%s is already subscribed with the conf mgmt")
           return 
    elif flag == "DEL": 
       status,meter_sub_info=check_send_msg_confmgmt_del(sub_info,app_id)
       if status == False or meter_sub_info == None or meter_sub_info == []:
           logging.warning("%s is already unsubscribed with the conf mgmt")
           return 
    try :
        config = ConfigParser.ConfigParser()
        config.read('pub_sub.conf')
        rabbitmq_username = config.get('RABBITMQ','Rabbitmq_username')
        rabbitmq_passwd = config.get('RABBITMQ','Rabbitmq_passwd')
        rabbitmq_host = config.get('RABBITMQ','Rabbitmq_host')
        rabbitmq_port = int ( config.get('RABBITMQ','Rabbitmq_port') )

        ceilometer_client_info = config.get('CLIENT','target')
        #msg={"sub_info":sub_info,"target":ceilometer_client_info,"action":flag}
        msg={"sub_info":meter_sub_info,"target":ceilometer_client_info,"action":flag}
        #print msg
        json_msg=json.dumps(msg)

        credentials = pika.PlainCredentials(rabbitmq_username,rabbitmq_passwd)
        parameters = pika.ConnectionParameters(rabbitmq_host,
                                               rabbitmq_port,
                                               '/',
                                               credentials)
        connection = pika.BlockingConnection(parameters)
        properties = pika.BasicProperties(content_type = "application/json")
        channel = connection.channel()
        channel.exchange_declare(exchange='pubsub',
                         type='fanout')
 
        channel.basic_publish(exchange='pubsub',
                              routing_key='',
                              properties = properties, 
                              body=json_msg)
        logging.debug(" [x] %s Sent",msg)
        logging.info(" [x] %s Sent",msg)
        connection.close() 
    except Exception as e:
           logging.error("Error:%s",e.__str__())
  
def read_notification_from_ceilometer(host,port):
     UDP_IP = host 
     UDP_PORT = port
 
     logging.debug("* Sarting UDP Client on ip:%s , port:%d",UDP_IP,UDP_PORT) 
     udp = socket.socket(socket.AF_INET, # Internet
                          socket.SOCK_DGRAM) # UDP
     udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

     udp.bind((UDP_IP, UDP_PORT))
     
     while True:
            #print thread.get_ident() 
            #logging.debug("thread.get_ident():%s", thread.get_ident()) 
            data, source = udp.recvfrom(64 * units.Ki)
            sample = msgpack.loads(data, encoding='utf-8')
            #logging.debug("* -------------------------------------------------------")
            logging.debug("%s",sample)
            #print(sample)
            for obj in sub_info:
                msg_list = []
                if obj.scheme == "udp" :
                    if type(obj.subscription_info) is list:
                        for info in obj.subscription_info:
                            msg_list.append(fnmatch.fnmatch(sample['counter_name'],info))
                    else :
                        msg_list.append(fnmatch.fnmatch(sample['counter_name'],obj.subscription_info)) 
                    try:  
                        if reduce(operator.or_, msg_list): 
                            host = obj.ipaddress
                            port = int(obj.portno)
                            l=[]
                            #logging.debug("* -------------------------------------------------------")
                            if obj.sub_info_filter is None:
                                try:  
                                    logging.debug("* Sending data without query over UDP for host:%s and port:%s",host,port) 
                                    udp.sendto(data,(host,port))
                                except Exception as e:
                                    logging.error ("Unable to send sample over UDP for %s and %s,%s",host,port,e.__str__())
                                    ret_str = ("Unable to send sample over UDP for %s and %s,%s")%(host,port,e.__str__())
                                continue 
                            for i in range(len(obj.sub_info_filter)):
                                if obj.sub_info_filter[i]['op'] in COMPARATORS:
                                    op = COMPARATORS[obj.sub_info_filter[i]['op']]
                                    logging.debug("* obj.sub_info_filter[i]['value']:%s",obj.sub_info_filter[i]['value'])
                                    logging.debug("* obj.sub_info_filter[i]['field']:%s",obj.sub_info_filter[i]['field'])
                                    l.append(op(obj.sub_info_filter[i]['value'],sample[obj.sub_info_filter[i]['field']]))
                                    logging.info("* Logical and of Query %s",l)    
                                else:
                                    logging.deubg("* Not a valid operator ignoring app_id:%s",obj.app_id)
                                    l.append(False)
                                    logging.info("* Logical and of Query %s",l)    
                            if reduce(operator.and_, l):
                                try:  
                                    logging.debug("* Sending data over UDP for host:%s and port:%s",host,port) 
                                    udp.sendto(data,(host,port))
                                except Exception:
                                    logging.error ("Unable to send sample over UDP for %s and %s ",host,port)
                                    ret_str = ("Unable to send sample over UDP for %s and %s ")%(host,port)
                            else :
                                 logging.warning("* No Notification found with the given subscription")
                        else :
                            logging.warning("* No valid subscrition found for %s",obj.app_id)
                    except Exception as e:
                       logging.error("Key_Error:%s ",e.__str__())
                       ret_str = ("Key_Error:%s \n")% e.__str__()

def read_notification_from_ceilometer_over_udp(host,port):
    UDP_IP = host
    UDP_PORT = port

    logging.debug("* Sarting UDP Client on ip:%s , port:%d",UDP_IP,UDP_PORT)
    udp = socket.socket(socket.AF_INET, # Internet
                          socket.SOCK_DGRAM) # UDP
    udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    udp.bind((UDP_IP, UDP_PORT))

    while True:
        #print thread.get_ident()
        #logging.debug("thread.get_ident():%s", thread.get_ident())
        data, source = udp.recvfrom(64 * units.Ki)
        sample = msgpack.loads(data, encoding='utf-8')
        status = process_ceilometer_message(sample,data)
  
def read_notification_from_ceilometer_over_kafka(parse_target):
    logging.info("Kafka target:%s",parse_target)
    try :
        kafka_publisher=kafka_broker.KafkaBrokerPublisher(parse_target)
        for message in kafka_publisher.kafka_consumer:
            #print message.value
            #logging.debug("%s",message.value)
            #logging.info("%s",message.value)
            status = process_ceilometer_message(json.loads(message.value),message.value)
            #print status
    except Exception as e:
        logging.error("Error in Kafka setup:%s ",e.__str__())

def process_ceilometer_message(sample,data):
    logging.debug("%s",sample)
    #logging.info("%s",sample)
    if len(sub_info) < 1:
        #print  "No subscription exists"
        return
    for obj in sub_info:
         #import pdb;pdb.set_trace()
         msg_list = []
         if type(obj.subscription_info) is list:
             for info in obj.subscription_info:
                 msg_list.append(fnmatch.fnmatch(sample['counter_name'],info))
         else :
             msg_list.append(fnmatch.fnmatch(sample['counter_name'],obj.subscription_info))
         try:
             if reduce(operator.or_, msg_list):
                 ''' 
                 kafka_publisher = None
                 if obj.scheme == "kafka" :
		    parse_target=netutils.urlsplit(obj.target)
	            try :
		        kafka_publisher=kafka_broker.KafkaBrokerPublisher(parse_target)
                    except Exception as e:
                        logging.error("* Error in connecting kafka broker:%s",e.__str__())
                       # return False
                        continue 
                 '''
                 host = obj.ipaddress
                 port = int(obj.portno)
                 l=[]
                 logging.debug("* -------------------------------------------------------")
                 if obj.sub_info_filter is None:
                     try:
                         if obj.scheme == "udp" :
                              #logging.debug("* Sending data without query over UDP for host:%s and port:%s",host,port)
                              #logging.info("* Sending data without query over UDP for host:%s and port:%s",host,port)
                              #udp = socket.socket(socket.AF_INET, # Internet
                              #                     socket.SOCK_DGRAM) # UDP
                              #udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
                              obj.udp.sendto(data,(host,port))
                              #return True
                              continue
                         elif obj.scheme == "kafka" :
                              #logging.debug("* Sending data over kafka for host:%s and port:%s and topec:%s",host,port,kafka_publisher.topic)
                              #logging.info("* Sending data over kafka for host:%s and port:%s and topec:%s",host,port,kafka_publisher.topic)
                              obj.kafka_publisher._send(sample)  
                              #return True
                              continue                                  
                     except Exception as e:
                          logging.error ("Unable to send sample over UDP/kafka for %s and %s,%s",host,port,e.__str__())
                          ret_str = ("Unable to send sample over UDP for %s and %s,%s ")%(host,port,e.__str__())
                          #return False
                          continue 
                 for i in range(len(obj.sub_info_filter)):
                     if obj.sub_info_filter[i]['op'] in COMPARATORS:
                          op = COMPARATORS[obj.sub_info_filter[i]['op']]
                          #logging.debug("* obj.sub_info_filter[i]['value']:%s",obj.sub_info_filter[i]['value'])
                          #logging.debug("* obj.sub_info_filter[i]['field']:%s",obj.sub_info_filter[i]['field'])
                          l.append(op(obj.sub_info_filter[i]['value'],sample[obj.sub_info_filter[i]['field']]))
                          #logging.info("* Logical and of Query %s",l)
                     else:
                          logging.info("* Not a valid operator ignoring app_id:%s",obj.app_id)
                          l.append(False)
                          #logging.info("* Logical and of Query %s",l)
                 if reduce(operator.or_, l):
                     try:
                         if obj.scheme == "udp" :
                              logging.debug("* Sending data over UDP for host:%s and port:%s",host,port)
                              #udp = socket.socket(socket.AF_INET, # Internet
                              #                    socket.SOCK_DGRAM) # UDP
                              #udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                              obj.udp.sendto(data,(host,port))
                              #return True
                              continue
                         elif obj.scheme == "kafka" :
                              logging.debug("* Sending data over kafka for host:%s and port:%s and topec:%s",host,port,obj.kafka_publisher.topic)
                              obj.kafka_publisher._send(sample)  
                              #return True
                              continue                                  
                     except Exception:
                         logging.error ("Unable to send sample over UDP/Kafka for %s and %s ",host,port)
                         ret_str = ("Unable to send sample over UDP/Kafka for %s and %s ")%(host,port)
                         #return False
                         continue   
                 else :
		       logging.debug("* No Notification found with the given subscription")
                       continue
             else :
                  logging.debug("* No matching subscrition found for %s",sample['counter_name'])
                  continue
         except Exception as e:
             logging.error("Key_Error:%s ",e.__str__())
             ret_str = ("Key_Error:%s \n")%e.__str__()
             #return False
             continue

def  build_subscription_info_from_database():
     ''' Reading each object from database and creating subinfo object'''
     config = ConfigParser.ConfigParser()
     config.read('pub_sub.conf')

     if config.get('CLIENT','UpdateDatabase') == "False" :
          logging.warning("Update Database flag is disabled,enable the flag to  update database")
          return

     db = shelve.open('pubsubdb')
     if len(db) > 0:
         for info in list(db.keys()):
             scheme = db[info].scheme
             app_id = db[info].app_id
             app_ip = db[info].ipaddress
             app_port = db[info].portno
             subscription_info = db[info].subscription_info
             sub_info_filter = db[info].sub_info_filter
             target = db[info].target
             subscrip_obj=subinfo(scheme,app_id,app_ip,app_port,subscription_info,sub_info_filter,target)
             status = subscrip_obj.update_subinfo()
             subinfo.print_subinfo()
     else:
         logging.info("pub-sub database is empty")
     db.close()

def initialize(ceilometer_client):
     logging.debug("Ceilometer client info:%s",ceilometer_client)
     ''' Building subscription info from the database '''
     build_subscription_info_from_database()
     parse_target=netutils.urlsplit(ceilometer_client)
     if not parse_target.netloc:
        err_str = "Error:Invalid client format"
        logging.error("* Invalid client format")
        return err_str
     if parse_target.scheme == "udp" :
         host,port=netutils.parse_host_port(parse_target.netloc)
         scheme = parse_target.scheme
         app_ip = host
         app_port = port
         if host == None or port == None :
             err_str = "* Error: Invalid IP Address format"
             logging.error("* Invalid IP Address format")
             return err_str
         thread.start_new(read_notification_from_ceilometer_over_udp,(host,port,))
     elif parse_target.scheme == "kafka" :
         thread.start_new(read_notification_from_ceilometer_over_kafka,(parse_target,))
     
        
if __name__ == "__main__":

    try:
        config = ConfigParser.ConfigParser()
        config.read('pub_sub.conf')
        webserver_host = config.get('WEB_SERVER','webserver_host')
        webserver_port = int (config.get('WEB_SERVER','webserver_port'))
       # client_host    = config.get('CLIENT','client_host')
      #  client_port    = int (config.get('CLIENT','client_port'))
        ceilometer_client_info = config.get('CLIENT','target')
        '''  
        log_level      = config.get('LOGGING','level')
        log_file       = config.get('LOGGING','filename')
        maxbytes       = int (config.get('LOGGING','maxbytes'))
        backupcount    = int (config.get('LOGGING','backupcount'))
        level = LEVELS.get(log_level, logging.NOTSET)
        '''
        logging.config.fileConfig('pub_sub.conf', disable_existing_loggers=False)
        ''' 
        logging.basicConfig(filename=log_file,format='%(asctime)s %(levelname)s %(message)s',\
                    datefmt=_DEFAULT_LOG_DATE_FORMAT,level=level)

        # create rotating file handler
        
        rfh = logging.handlers.RotatingFileHandler(
                 log_file, encoding='utf8', maxBytes=maxbytes,
                 backupCount=backupcount,delay=0)
        logging.getLogger().addHandler(rfh)
        '''
         
    except Exception as e:
        print("* Error in config file:",e.__str__())
        #logging.error("* Error in confing file:%s",e.__str__())
    else: 
        #initialize(client_host,client_port)
        initialize(ceilometer_client_info)
        app.run(host=webserver_host,port=webserver_port,debug=False)
