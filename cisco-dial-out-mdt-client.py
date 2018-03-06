import logging
import socket
import selectors
import sys
import json
from pprint import pprint
from multiprocessing import Pool
from struct import unpack
from argparse import ArgumentParser
from databases import InfluxDBUploader
from utils import format_mdt_output

class SelectorServer(object):
    def __init__(self, host, port, logger, func, database):
        self.main_socket = socket.socket()
        self.main_socket.setblocking(False)
        self.main_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.main_socket.bind((host, port))
        self.main_socket.listen(10)
        self.logger = logger
        self.selector = selectors.DefaultSelector()
        self.selector.register(fileobj=self.main_socket, events=selectors.EVENT_READ, data=self.on_accept)
        self.endianness = sys.byteorder
        self.process_function = func
        self.database_conn = database
        self.header_size = 12
        self.pool = Pool(10)
        
    def on_accept(self, sock, mask):
        conn, address = self.main_socket.accept()
        self.logger.info(f'Accepted connection from {address}')
        self.selector.register(fileobj=conn, events=selectors.EVENT_READ, data=self.on_read)

    def close_connection(self, conn):
        self.logger.info(f"Closing connection to {conn}")
        self.selector.unregister(conn)
        conn.close()
        
    def on_read(self, conn, mask):
        peer_name = conn.getpeername() 
        header_data = conn.recv(self.header_size)
        while header_data:
            msg_type, encode_type, msg_version, flags, msg_length = unpack('>hhhhi',header_data)
            self.logger.info(f"Got MDT data ({msg_length} bytes) from {peer_name}")
            msg_data = b''
            while len(msg_data) < msg_length:
                packet = conn.recv(msg_length - len(msg_data))
                msg_data += packet
            self.pool.apply_async(self.process_function, (msg_data, self.database_conn, ))
            header_data = conn.recv(self.header_size)


    def collect(self):
        while True:
            events = self.selector.select()
            for key, mask in events:
                handler = key.data
                handler(key.fileobj, mask)



def process_output_and_upload(data, database):
    json_data = json.loads(data.decode("utf-8"))
    print(json_data)
    
    
def main():
    parser = ArgumentParser()
    parser.add_argument("-a", "--address", dest="address", help="Server ip address to bind to", required=True)
    parser.add_argument("-p", "--port", dest="port", help="Server Port to listen on", required=True)
    parser.add_argument("-t", "--database_server", dest="db_server", help="InfluxDB server name", required=True)
    parser.add_argument("-r", "--database_port", dest="db_port", help="InfluxDB server port", required=True)
    parser.add_argument("-d", "--database", dest="database", help="InfluxDB Database to use", required=True)
    parser.add_argument("-v", "--verbose", action="store_true", help="Turn on debug logs")
    args = parser.parse_args()

    logger = logging.getLogger("Telemetry-Memory-Monitor")
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    influxdb = InfluxDBUploader(args.db_server, int(args.db_port), args.database)
    server = SelectorServer(args.address, int(args.port), logger, process_output_and_upload, influxdb)
    server.collect()


if __name__ == '__main__':
    main()
