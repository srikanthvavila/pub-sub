#!/usr/bin/python
import shelve
import logging 
class database:
    def __init__(self,scheme,app_id,app_ip,app_port,subscription_info,sub_info_filter,target):
        logging.debug("* Updating subscription_info for database")
        self.scheme = scheme
        self.app_id = app_id
        self.ipaddress = app_ip
        self.portno = app_port
        self.subscription_info = subscription_info
        self.sub_info_filter = sub_info_filter
        self.target = target

    def add_to_database(self):
        db = shelve.open('pubsubdb')
        app_id = str(self.app_id)
        if db.has_key(app_id):
            logging.info("Subscription already exist in the database,so overwriting this subscription \n")
            del db[app_id]
            db[app_id] = self
        else:
            db[app_id] = self
        db.close()

    @staticmethod
    def delete_from_database(appid):
        db = shelve.open('pubsubdb')
        app_id = str(appid)
        if db.has_key(app_id):
            del db[app_id]
        else:
            logging.info("No Subscription exists in the database with app_id:%s",app_id)
        db.close() 
       
