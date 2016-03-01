#!/usr/bin/python
import socket
from oslo_utils import units
from oslo_utils import netutils
import kafka
import kafka_broker
import fnmatch
import logging
import copy

sub_info=[] 
class subinfo:
    def __init__(self,scheme,app_id,app_ip,app_port,subscription_info,sub_info_filter,target):
        logging.debug("* Updating subscription_info ") 
        self.scheme = scheme
        self.app_id = app_id
        self.ipaddress = app_ip 
        self.portno = app_port 
        self.subscription_info = subscription_info
        self.sub_info_filter = sub_info_filter
        self.target = target
        
        if scheme == "kafka":
            ''' Creating kafka publisher to send message over kafka '''
            parse_target = netutils.urlsplit(target)
            self.kafka_publisher = kafka_broker.KafkaBrokerPublisher(parse_target)
        elif scheme == "udp":
            ''' Creating UDP socket to send message over UDP '''
            self.udp = socket.socket(socket.AF_INET, # Internet
                                     socket.SOCK_DGRAM) # UDP
            self.udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)   

    def update_subinfo(self):
        logging.info("* inside %s",self.update_subinfo.__name__)
        if not sub_info:
            logging.debug("* -----------List is EMpty -------------") 
            sub_info.append(self)
            logging.debug("* Subscription is sucessful") 
            return "Subscription is sucessful \n" 
        for obj in sub_info:
            if obj.app_id == self.app_id :
               # obj.subscription_info=self.subscription_info
                sub_info.remove(obj)
                sub_info.append(self)
                logging.warning("* entry already exists so overwriting this subscription \n")
                return "entry already exists so overwriting this subscription \n" 
        sub_info.append(self)
        return "Subscription is sucessful \n"
 
    @staticmethod
    def delete_subinfo(app_id):
        logging.info("* inside %s",subinfo.delete_subinfo.__name__)
        Flag = False 
        for obj in sub_info:
            if obj.app_id == app_id : 
                    sub_info.remove(obj)
                    Flag = True
                    logging.debug("* Un-Subscription is sucessful") 
                    return "Un-Subscription is sucessful \n"
        if not Flag :
           err_str = "No subscription exists with app id: " + app_id + "\n"
           logging.error("* No subscription exists with app id:%s ",app_id)
           raise Exception (err_str)
       
    @staticmethod
    def print_subinfo():
        logging.info("* inside %s",subinfo.print_subinfo.__name__)
        for obj in sub_info:
            logging.debug("* ------------------------------------------------") 
            logging.debug("* scheme:%s",obj.scheme)  
            logging.debug("* app_id:%s",obj.app_id)
            logging.debug("* portno:%s",obj.portno ) 
            logging.debug("* ipaddress:%s",obj.ipaddress)  
            logging.debug("* subscription_info:%s",obj.subscription_info)
            logging.debug("* sub_info_filter:%s",obj.sub_info_filter)
            logging.debug("* target:%s",obj.target)
            logging.debug("* ------------------------------------------------")
    @staticmethod
    def get_subinfo(app_id):
        logging.info("* inside %s",subinfo.get_subinfo.__name__)
        Flag = False
        for obj in sub_info:
            if obj.app_id == app_id :
                    return obj.subscription_info,obj.target
        return (None,None)
       
 
    @staticmethod
    def get_sub_list(notif_subscription_info):
        logging.info("* inside %s",subinfo.get_sublist.__name__)
        sub_list=[]  
        for obj in sub_info:
            if obj.subscription_info == notif_subscription_info:
                sub_list.append(obj)
        return sub_list
