import json
from argparse import ArgumentParser
from configparser import ConfigParser
from tornado.tcpserver import TCPServer
from tornado.iostream import StreamClosedError
from tornado.queues import Queue
from tornado import gen
from tornado.ioloop import IOLoop
from struct import Struct, unpack
from telemetry_pb2 import Telemetry
from requests import request

class TelemetryTCPDialOutServer(TCPServer):
    def __init__(self, queue):
        super().__init__()
        self.header_size = 12
        self.header_struct = Struct('>hhhhi')
        self._UNPACK_HEADER = self.header_struct.unpack
        self.queue = queue
        
    @gen.coroutine
    def handle_stream(self, stream, address):
        print(f"Got connection from {address}")
        while not stream.closed():
            try:
                header_data = yield stream.read_bytes(self.header_size)
                msg_type, encode_type, msg_version, flags, msg_length = self._UNPACK_HEADER(header_data)
                encoding = {1:'gpb', 2:'json'}[encode_type]
                msg_data = b''
                if encode_type == 1:
                    #print(f'Got {msg_length} bytes from {address} with encoding {encoding}')
                    while len(msg_data) < msg_length:
                        packet = yield stream.read_bytes(msg_length - len(msg_data))
                        msg_data += packet
                    #gpb_data =Telemetry()
                    #gpb_data.ParseFromString(msg_data)
                    #print(gpb_data)
                else:
                    #print(f'Got {msg_length} bytes from {address} with encoding {encoding}')
                    while len(msg_data) < msg_length:
                        packet = yield stream.read_bytes(msg_length - len(msg_data))
                        msg_data += packet
                    json_data = json.loads(msg_data.decode("ascii"))
                    del json_data
                    #yield self.queue.put(json_data)
                    #url = f"http://web-ott-tsdb-server-1:9200/{json_data['encoding_path'].replace('/','-').lower()}"
                    #headers = {'Content-Type': "application/json"}
                    #response = request("HEAD", url, headers=headers)
                    #if response.status_code == 404:
                    #    if not request("PUT", url, headers=headers).status_code == 200:
                    #        print("Error when creating index")
                    #url = f"{url}/{json_data['node_id_str']}"
                    #body = json_data['data_json']
                    #print(url)
                    #print(body)
                    #if not request("POST", url, json=body).status_code == 201:
                    #    print("ERROR in POST")
                        
            except Exception as e:
                print(e)
                print('Closing Session')
                stream.close()


@gen.coroutine
def rest_request(queue):
    while True:
        item = yield queue.get()
        try:
            #print(item)
            pass
        finally:
            queue.task_done()
                
def main():
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", dest="config", help="Config file for dial-out telemetry server", required=True)
    args = parser.parse_args()
    config_parser = ConfigParser()
    config_parser.read(args.config)
    queue = Queue()
    tcp_server = TelemetryTCPDialOutServer(queue)
    for section in config_parser.sections():
        for port in json.loads(config_parser.get(section,"Ports")):
            if section == 'TCP':
                tcp_server.bind(port)
            elif section == 'GRPC':
                pass
            else:
                pass
    tcp_server.start(0)
    #IOLoop.current().add_callback(rest_request, queue)
    IOLoop.current().start()


if __name__ == '__main__':
    main()
