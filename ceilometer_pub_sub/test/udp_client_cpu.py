import socket
import msgpack
from oslo_utils import units

UDP_IP = "10.11.10.1"
UDP_PORT = 5006

udp = socket.socket(socket.AF_INET, # Internet
                     socket.SOCK_DGRAM) # UDP
udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
udp.bind((UDP_IP, UDP_PORT))

while True:
     data, source = udp.recvfrom(64 * units.Ki)
     #print data
     #try:
     sample = msgpack.loads(data, encoding='utf-8')
     print sample
         #for k, v in sample.items():
          #    print(k, v)
    # except Exception:
    #      print ("UDP: Cannot decode data sent by %s"), source
