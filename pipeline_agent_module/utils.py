import yaml
import random
import string
import logging
import fnmatch

def main():
    orig_pipeline_conf = "/etc/ceilometer/pipeline.yaml"
    with open (orig_pipeline_conf, 'r') as fap:
        data = fap.read()
        pipeline_cfg = yaml.safe_load(data)
    return pipeline_cfg

def build_meter_list():
    ''' function to exiting  meter list from pipeline.yaml'''
    orig_pipeline_conf = "/etc/ceilometer/pipeline.yaml"
    with open (orig_pipeline_conf, 'r') as fap:
         data = fap.read()
         pipeline_cfg = yaml.safe_load(data)
    source_cfg = pipeline_cfg['sources']
    meter_list=[]
    for i in source_cfg:
         meter_list.append(i['meters'])
    
    return meter_list

def get_sink_name_from_publisher(publisher,pipeline_cfg):
    sink_cfg = pipeline_cfg['sinks']
    ''' Iterating over the list of publishers to get sink name'''
    try :
        for sinks in sink_cfg:
            pub_list = sinks.get('publishers')
            try :
                k = pub_list.index(publisher)
                return sinks.get('name') 
            except Exception as e:
                #print ("Got Exception",e.__str__())
                continue   
    except Exception as e:
        return None

def get_source_name_from_meter(meter,pipeline_cfg):
    source_cfg = pipeline_cfg['sources']
    ''' Iternating over the list of meters to get source name'''
    try :  
        for sources in source_cfg:
            meter_list = sources.get('meters')
            try :
                k = meter_list.index(meter)
                return sources.get('name')
            except Exception as e:
                #print ("Got Exception",e.__str__())
                continue   
    except Exception as e:
        return None

def get_source_name_from_with_meter_patter_match(meter,pipeline_cfg):
    ''' Iternating over the list of meters for wildcard match to get source name'''
    source_cfg = pipeline_cfg['sources']
    try :
        for sources in source_cfg:
            meter_list = sources.get('meters')
            for k in meter_list:
                if k[0] == "*":
                    logging.warning("Ignoring wild card meter(*) case ")
                    continue
                if fnmatch.fnmatch(k,meter):
                    logging.debug("substring match")
                    return (sources.get('name'),"superset",k)
                if fnmatch.fnmatch(meter,k):
                    logging.debug("input is super match")
                    return (sources.get('name'),"subset",k)
    except Exception as e:
        return None,None,None

    return None,None,None

def get_source_name_from_sink_name(sink_name,pipeline_cfg):
    ''' iterating over list of sources to get sink name'''
    source_cfg = pipeline_cfg['sources']
    try :
        for sources in source_cfg:
            sink_list = sources.get("sinks")
            try :
                k = sink_list.index(sink_name)
            #sources.get("meters").append("m2")
                return sources.get("name")
            except Exception as e:
                continue
    except Exception as e:
        return None

def get_sink_name_from_source_name(source_name,pipeline_cfg):
    ''' iterating over list of sinks to get sink name'''
    source_cfg = pipeline_cfg['sources']
    try :
        for sources in source_cfg:
            try :
                if  sources.get("name") == source_name: 
                    return sources.get("sinks")
            except Exception as e:
                continue
    except Exception as e:
        return None 

def add_meter_to_source(meter_name,source_name,pipeline_cfg):
    ''' iterating over the list of sources to add meter to the matching source'''
    source_cfg = pipeline_cfg['sources']
    try :
        for sources in source_cfg:
            try :
                if  sources.get("name") == source_name: 
                    sources.get("meters").append(meter_name)
                    return True 
            except Exception as e:
                continue
    except Exception as e:
        return False

def get_meter_list_from_source(source_name,pipeline_cfg):
    ''' iterating over the list of sources to get meters under the given source'''
    source_cfg = pipeline_cfg['sources']
    try :
        for sources in source_cfg:
            try :
                if  sources.get("name") == source_name:
                    return sources.get("meters")
            except Exception as e:
                continue
    except Exception as e:
        return None

def get_publisher_list_from_sink(sink_name,pipeline_cfg):
    sink_cfg = pipeline_cfg['sinks']
    ''' Iterating over the list of sinks to build publishers list '''
    publisher_list = []
    try :
        for sinks in sink_cfg:
            try :
                for j in sink_name:
                    if j == sinks.get("name"):
                        publisher_list.append(sinks.get("publishers"))
                        return publisher_list
            except Exception as e:
                #print ("Got Exception",e.__str__())
                continue   
    except Exception as e:
        return None

def get_publisher_list_from_sinkname(sink_name,pipeline_cfg):
    sink_cfg = pipeline_cfg['sinks']
    ''' Iterating over the list of sinks to build publishers list '''
    try :
        for sinks in sink_cfg:
            pub_list = sinks.get('publishers')
            try :
                 if sink_name == sinks.get("name"):
                     return pub_list   
            except Exception as e:
                #print ("Got Exception",e.__str__())
                continue
    except Exception as e:
        return None


def delete_meter_from_source(meter_name,source_name,pipeline_cfg) :
    ''' function to delete meter for the given source '''
    source_cfg = pipeline_cfg['sources']
    try :
        for sources in source_cfg:
            try :
                if  sources.get("name") == source_name:
                    meter_list = sources.get('meters')
                    try :
                       meter_index = meter_list.index(meter_name)
                       logging.debug("meter name is present at index:%s",meter_index)
                       if len(meter_list) == 1 and meter_index == 0:
                           logging.debug("Only one meter exists removing entire source entry")
                           source_cfg.remove(sources)
                       else :
                           meter_list.pop(meter_index)
                       return True     
                    except Exception as e:
                        continue
            except Exception as e:
                continue
    except Exception as e:
        return False 

def delete_publisher_from_sink(publisher,sink_name,pipeline_cfg):
    sink_cfg = pipeline_cfg['sinks']
    ''' Iterating over the list of publishers '''
    try :
        for sinks in sink_cfg:
            pub_list = sinks.get('publishers')
            #print pub_list
            try :
                if sink_name == sinks.get("name"):
                    k = pub_list.index(publisher)
                    pub_list.pop(k)
                    #print k
                    return True 
            except Exception as e:
                #print ("Got Exception",e.__str__())
                continue   
    except Exception as e:
        return None

def delete_sink_from_pipeline(sink_name,pipeline_cfg):
    sink_cfg = pipeline_cfg['sinks']
    try :
        for sinks in sink_cfg:
            if sink_name == sinks.get("name"):
                sink_cfg.remove(sinks)
                return True
    except Exception as e:
        return False 

def add_publisher_to_sink(publisher_name,sink_name,pipeline_cfg):
    sink_cfg = pipeline_cfg['sinks']
    try :
        for sinks in sink_cfg:
            if sink_name == sinks.get("name"):
                sinks.get('publishers').append(publisher_name)
                return True
    except Exception as e:
        return None 

def get_source_info(meter):
    name = ''.join(random.choice(string.ascii_lowercase) for _ in range(9))
    sink_name = name + "_sink"
    meter_name = name + "_source"
    source_info = {'interval': 6,'meters': [meter],'name': meter_name,'sinks':[sink_name]}
    logging.debug("* new source_info :%s",source_info)
    return (source_info,sink_name)

def get_sink_info(meter,sink_name,target):
    sink_info = {'publishers':['notifier://',target],'transformers':None ,'name': sink_name}
    logging.debug("* new source_info :%s",sink_info)
    return sink_info

def delete_conf_from_pipe_line_cfg(meter,publisher,pipeline_cfg):
    #import pdb;pdb.set_trace()
   
    sink_name = get_sink_name_from_publisher(publisher,pipeline_cfg)
    source_name = get_source_name_from_meter(meter,pipeline_cfg)
      
    if sink_name is None or source_name is None:
       logging.error("Either sink or source name Exists in the pipeline.yaml")
       return False
   
    meter_list = get_meter_list_from_source(source_name,pipeline_cfg)
   
    temp_meter_list = []
   
    for j in meter_list:
        temp_meter_list.append(j)
  
    pub_list = get_publisher_list_from_sinkname(sink_name,pipeline_cfg)
    if len(pub_list) > 2 and  len(temp_meter_list) == 1:
        if delete_publisher_from_sink(publisher,sink_name,pipeline_cfg):
            return True
        else:
            return False    
  
    if delete_meter_from_source(meter,source_name,pipeline_cfg) :
        if len(temp_meter_list) == 1:
            if delete_publisher_from_sink(publisher,sink_name,pipeline_cfg) :
                if get_source_name_from_sink_name(sink_name,pipeline_cfg) is None:
                    delete_sink_from_pipeline(sink_name,pipeline_cfg)  
                return True
            else :
                return False 
        return True         
    return False
    

def update_sink_aggrgation(meter,publisher,source_name,matching_meter,meter_match,pipeline_cfg):
    ''' Build new source and sink '''
    new_source_info,new_sink_name = get_source_info(meter)
    new_sink_info = get_sink_info(meter,new_sink_name,publisher)

    meter_list = get_meter_list_from_source(source_name,pipeline_cfg)
    sink_name = get_sink_name_from_source_name(source_name,pipeline_cfg)
    publisher_list = get_publisher_list_from_sink(sink_name,pipeline_cfg)
    for i in publisher_list:
        for j in i:
            #print j
            if j not in new_sink_info.get("publishers") :
                new_sink_info.get("publishers").append(j)
                #print new_sink_info

    cfg_source = pipeline_cfg['sources']
    cfg_sink = pipeline_cfg['sinks']
    if meter_match == "superset" :
        new_source_info.get("meters").append("!"+ matching_meter)
    elif meter_match == "subset" :
        ''' here need to get list of meters with sub-string match '''
        add_meter_to_source("!"+meter,source_name,pipeline_cfg)
        add_publisher_to_sink(publisher,sink_name,pipeline_cfg)

    logging.debug("-----------  Before Updating Meter Info ------------------")
    logging.debug("%s",pipeline_cfg)

    ''' Updating source and sink info '''
    cfg_source.append(new_source_info)
    cfg_sink.append(new_sink_info)
    logging.debug("-----------  After Updating Meter Info --------------------")
    logging.debug("%s",pipeline_cfg)

def update_conf_to_pipe_line_cfg(meter,publisher,pipeline_cfg):
    #import pdb;pdb.set_trace()
    sink_name = get_sink_name_from_publisher(publisher,pipeline_cfg)
    source_name = get_source_name_from_meter(meter,pipeline_cfg)
    if sink_name is None :
        logging.debug("No Sink exists with the given Publisher")
        if source_name is None:
            ''' Commenting the code related t owild card '''
            '''
            pattern_source_name,pattern,matching_meter = get_source_name_from_with_meter_patter_match(meter,pipeline_cfg)
            if pattern_source_name is not None:
                if pattern == "superset" :
                    #add_meter_to_source("!"+meter,pattern_source_name,pipeline_cfg)
                    update_sink_aggrgation(meter,publisher,pattern_source_name,matching_meter,"superset",pipeline_cfg)
                    #print pipeline_cfg
                    return True 
                if pattern == "subset" :
                   update_sink_aggrgation(meter,publisher,pattern_source_name,matching_meter,"subset",pipeline_cfg)
                   return True    
            ''' 
            source_info,sink_name = get_source_info(meter)
            sink_info = get_sink_info(meter,sink_name,publisher)
  
            cfg_source = pipeline_cfg['sources']
            cfg_sink = pipeline_cfg['sinks']

            logging.debug("-----------  Before Updating Meter Info ------------------")
            logging.debug("%s",pipeline_cfg)

            ''' Updating source and sink info '''
            cfg_source.append(source_info)
            cfg_sink.append(sink_info)
            logging.debug("-----------  After Updating Meter Info --------------------")
            logging.debug("%s",pipeline_cfg)
            return True
        else :
             logging.debug("Meter already exists in the conf file under source name:%s ",source_name)
             meter_list = get_meter_list_from_source(source_name,pipeline_cfg)
             publisher_list=[]
             if len(meter_list) > 1:
                sink_name = get_sink_name_from_source_name(source_name,pipeline_cfg)
                '''
                if type(sink_name) is list :
                    for sinkname in sink_name:    
                        publisher_list.append(get_publisher_list_from_sink(sinkname,pipeline_cfg))
                else :
                     publisher_list.append(get_publisher_list_from_sink(sink_name,pipeline_cfg))
                ''' 
                publisher_list = get_publisher_list_from_sink(sink_name,pipeline_cfg)
                new_source_info,new_sink_name = get_source_info(meter)
                new_sink_info = get_sink_info(meter,new_sink_name,publisher)
                for i in publisher_list:
                     for j in i:
                          #print j
                          if j not in new_sink_info.get("publishers") :
                              new_sink_info.get("publishers").append(j)
                cfg_source = pipeline_cfg['sources']
                cfg_sink = pipeline_cfg['sinks']

                logging.debug("-----------  Before Updating Meter Info ------------------")
                logging.debug("%s",pipeline_cfg)

                ''' Updating source and sink info '''
                cfg_source.append(new_source_info)
                cfg_sink.append(new_sink_info)
                logging.debug("-----------  After Updating Meter Info --------------------")
                logging.debug("%s",pipeline_cfg)
                delete_meter_from_source(meter,source_name,pipeline_cfg)
                logging.debug("%s",pipeline_cfg)
                return True
             else :
                  logging.debug ("Source already exists for this meter add publisher to it .....:%s",source_name)
                  sink_name_list = get_sink_name_from_source_name(source_name,pipeline_cfg)
                  for sink_name in sink_name_list :
                      add_publisher_to_sink(publisher,sink_name,pipeline_cfg)
                  return True    
                  #print pipeline_cfg
    else :
         logging.debug ("Publisher already exists under sink:%s",sink_name)
         if get_source_name_from_meter(meter,pipeline_cfg) is not None:
             logging.debug("Both meter  and publisher already exists in the conf file")
             logging.debug( "Update request is not sucessful")
             return False
         else :
             source_name = get_source_name_from_sink_name(sink_name,pipeline_cfg) 
             logging.debug ("Need to add meter to already existing source which \
                    has this publisher under one of its sink")
             #print source_name
             if add_meter_to_source(meter,source_name,pipeline_cfg):
                 logging.debug("Meter added sucessfully")
                 return True   
               
            
        
