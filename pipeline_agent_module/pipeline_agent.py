import pika
import yaml
import subprocess
import logging
import logging.config
import operator
import json
import ConfigParser
import pipeline
import utils
#from ceilometer import pipeline
from collections import  OrderedDict


class UnsortableList(list):
    def sort(self, *args, **kwargs):
        pass

class UnsortableOrderedDict(OrderedDict):
    def items(self, *args, **kwargs):
        return UnsortableList(OrderedDict.items(self, *args, **kwargs))

#yaml.add_representer(UnsortableOrderedDict, yaml.representer.SafeRepresenter.represent_dict)


tmp_pipeline_conf = "/tmp/pipeline.yaml"

'''
LEVELS = {'DEBUG': logging.DEBUG,
          'INFO': logging.INFO,
          'WARNING': logging.WARNING,
          'ERROR': logging.ERROR,
          'CRITICAL': logging.CRITICAL}

_DEFAULT_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
'''
def get_source_info(meter):
    sink_name = meter + "_sink"
    meter_name = meter+"_name"   
    source_info = {'interval': 6,'meters': [meter],'name': meter_name,'sinks':[sink_name]}
    logging.debug("* new source_info :%s",source_info)
    return (source_info,sink_name)

def get_sink_info(meter,sink_name,target):
    sink_info = {'publishers':['notifier://',target],'transformers':None ,'name': sink_name}
    logging.debug("* new source_info :%s",sink_info)
    return sink_info

def restart_ceilometer_services():
    try : 
       config = ConfigParser.ConfigParser()
       config.read('pipeline_agent.conf')
       services = config.get('RABBITMQ','Ceilometer_service')
       service = services.split(",")
    except Exception as e:
        logging.error("* Error in confing file:%s",e.__str__())
        return False
    else :
        for service_name in service:
            command = ['service',service_name, 'restart'];
            logging.debug("Executing: %s command",command)
            #shell=FALSE for sudo to work.
            try :
                subprocess.call(command, shell=False)
            except Exception as e:
                logging.error("* %s command execution failed with error %s",command,e.__str__())
                return False
    return True 
   
def check_meter_with_pipeline_cfg(pipeline_cfg_file,meter=None,target=None):
    #import pdb;pdb.set_trace() 
    try :
        pipeline._setup_pipeline_manager(pipeline_cfg_file,None)
    except Exception as e:
        logging.error ("Got Exception: %s",e.__str__())
        return False 
    return True
   

def callback(ch, method, properties, msg):
    logging.debug(" [x] Received %r",msg)
    #import pdb; pdb.set_trace()
    #yaml.add_representer(UnsortableOrderedDict, yaml.representer.SafeRepresenter.represent_dict)
    orig_pipeline_conf = "/etc/ceilometer/pipeline.yaml"
    with open (orig_pipeline_conf, 'r') as fap:
         data = fap.read()
         pipeline_cfg = yaml.safe_load(data)
    logging.debug("Pipeline config: %s", pipeline_cfg)

    try : 
        json_msg = json.loads(msg)
        meter = json_msg['sub_info']
        publisher = json_msg['target']
        flag = json_msg['action']
        update_status = []  
        if type(meter) is list:
            logging.debug("Metere is a list ... Need to handle it ")
            for meter_info in meter :
                update_status.append(update_pipeline_yaml(meter_info,publisher,flag))
        else :
             update_status.append(update_pipeline_yaml(meter,publisher,flag))
 
        if reduce(operator.or_,  update_status):
            if not restart_ceilometer_services():
                logging.error("Error in restarting ceilometer services")
                return False
    except Exception as e :
        logging.error("Got exception:%s in parsing message",e.__str__())
        return False

   


 
def update_pipeline_yaml(meter,publisher,flag):
    logging.debug("meter name:%s",meter)
    logging.debug("publisher or target name:%s",publisher)

    orig_pipeline_conf = "/etc/ceilometer/pipeline.yaml"
    ''' Parsing orginal pipeline yaml file '''
    try :
         with open (orig_pipeline_conf, 'r') as fap:
             data = fap.read()
             pipeline_cfg = yaml.safe_load(data)
         logging.debug("Pipeline config: %s", pipeline_cfg)
   
         ''' Chcking parsing errors '''
    
         if not check_meter_with_pipeline_cfg(orig_pipeline_conf) :
             logging.error("Original pipeline.yaml parsing failed")
             return False
         else :
             status = None
             if flag == "ADD" :
                 status = utils.update_conf_to_pipe_line_cfg(meter,publisher,pipeline_cfg)
             elif flag == "DEL" :
                 status = utils.delete_conf_from_pipe_line_cfg(meter,publisher,pipeline_cfg)
       
             if status == True : 
                 tmp_pipeline_conf = "/tmp/pipeline.yaml"
                 with open(tmp_pipeline_conf, "w") as f:
                      yaml.safe_dump( pipeline_cfg, f ,default_flow_style=False)
                 if check_meter_with_pipeline_cfg(tmp_pipeline_conf,meter,publisher) :
                      logging.debug("Tmp pipeline.yaml parsed sucessfully,coping it as orig")
                      with open(orig_pipeline_conf, "w") as f:
                          yaml.safe_dump( pipeline_cfg, f ,default_flow_style=False)
                      return True
                 else :
                      logging.info("Retaining original conf,as update meter info has errors")
                      return False     
    except Exception as e:
        logging.error("* Error in confing file:%s",e.__str__())
        return False

 
def msg_queue_listner():
    
    try:
        config = ConfigParser.ConfigParser()
        config.read('pipeline_agent.conf')
        rabbitmq_username = config.get('RABBITMQ','Rabbitmq_username')
        rabbitmq_passwd = config.get('RABBITMQ','Rabbitmq_passwd')
        rabbitmq_host = config.get('RABBITMQ','Rabbitmq_host')
        rabbitmq_port = int ( config.get('RABBITMQ','Rabbitmq_port') )
        '''
        log_level    = config.get('LOGGING','level')
        log_file       = config.get('LOGGING','filename')
 
        level = LEVELS.get(log_level, logging.NOTSET)
        logging.basicConfig(filename=log_file,format='%(asctime)s %(filename)s %(levelname)s %(message)s',\
                    datefmt=_DEFAULT_LOG_DATE_FORMAT,level=level)
        '''
        logging.config.fileConfig('pipeline_agent.conf', disable_existing_loggers=False)  
    except Exception as e:
        logging.error("* Error in confing file:%s",e.__str__())
    else :
        logging.debug("*------------------Rabbit MQ Server Info---------")
        logging.debug("rabbitmq_username:%s",rabbitmq_username)
        logging.debug("rabbitmq_passwd:%s",rabbitmq_passwd)
        logging.debug("rabbitmq_host:%s",rabbitmq_host)
        logging.debug("rabbitmq_port:%s",rabbitmq_port)
        credentials = pika.PlainCredentials(rabbitmq_username,rabbitmq_passwd)
        parameters = pika.ConnectionParameters(rabbitmq_host,
                                               rabbitmq_port,
                                               '/',
                                               credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        #channel.queue_declare(queue='pubsub')
        channel.exchange_declare(exchange='pubsub',
                         type='fanout')

        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange='pubsub',
                    queue=queue_name)
        logging.debug("[*] Waiting for messages. To exit press CTRL+C")

        channel.basic_consume(callback,
                              queue=queue_name,
                              no_ack=True)
        channel.start_consuming()

if __name__ == "__main__":
    #logging.debug("* Starting pipeline agent module")
    msg_queue_listner()

