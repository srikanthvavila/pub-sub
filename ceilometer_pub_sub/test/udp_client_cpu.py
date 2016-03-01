import socket
import msgpack
from oslo_utils import units
import logging
UDP_IP = "10.11.10.1"
UDP_PORT = 5006

logging.basicConfig(format='%(asctime)s %(filename)s %(levelname)s %(message)s',filename='udp_client.log',level=logging.INFO)
udp = socket.socket(socket.AF_INET, # Internet
                     socket.SOCK_DGRAM) # UDP
udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
udp.bind((UDP_IP, UDP_PORT))
while True:
     data, source = udp.recvfrom(64 * units.Ki)
     #print data
     #try:
     sample = msgpack.loads(data, encoding='utf-8')
     logging.info("%s",sample)
     print sample
     #except Exception:
         #logging.info("%s",sample)
     #    print ("UDP: Cannot decode data sent by %s"), source
