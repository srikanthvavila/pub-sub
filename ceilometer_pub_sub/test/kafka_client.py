import kafka
import kafka_broker
from oslo_utils import netutils
import logging

def read_notification_from_ceilometer_over_kafka(parse_target):
    logging.info("Kafka target:%s",parse_target)
    try :
        kafka_publisher=kafka_broker.KafkaBrokerPublisher(parse_target)
        for message in kafka_publisher.kafka_consumer:
            #print message.value
            logging.info("%s",message.value)
            #print status
    except Exception as e:
        logging.error("Error in Kafka setup:%s ",e.__str__())

ceilometer_client="kafka://10.11.10.1:9092?topic=test"
logging.basicConfig(format='%(asctime)s %(filename)s %(levelname)s %(message)s',filename='kafka_client.log',level=logging.INFO)
parse_target=netutils.urlsplit(ceilometer_client)
read_notification_from_ceilometer_over_kafka(parse_target)
