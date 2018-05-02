import json
import logging
from argparse import ArgumentParser
from configparser import ConfigParser
from tornado.tcpserver import TCPServer
from tornado.iostream import StreamClosedError
from tornado import gen
from tornado.ioloop import IOLoop
from struct import Struct, unpack
from telemetry_pb2 import Telemetry


class TelemetryTCPDialOutServer(TCPServer):
    def __init__(self):
        super().__init__()
        self.header_size = 12
        self.header_struct = Struct('>hhhhi')
        self._UNPACK_HEADER = self.header_struct.unpack
        
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
                    print(f'Got {msg_length} bytes from {address} with encoding {encoding}')
                    while len(msg_data) < msg_length:
                        packet = yield stream.read_bytes(msg_length - len(msg_data))
                        msg_data += packet
                    #gpb_data =Telemetry()
                    #gpb_data.ParseFromString(msg_data)
                    #print(gpb_data)
                else:
                    print(f'Got {msg_length} bytes from {address} with encoding {encoding}')
                    while len(msg_data) < msg_length:
                        packet = yield stream.read_bytes(msg_length - len(msg_data))
                        msg_data += packet
                    #json_data = json.loads(msg_data.decode("ascii"))
                    #print(json_data)
            except Exception as e:
                print(e)
                stream.close()

        
                
def main():
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", dest="config", help="Config file for dial-out telemetry server", required=True)
    args = parser.parse_args()
    config_parser = ConfigParser()
    config_parser.read(args.config)
    tcp_server = TelemetryTCPDialOutServer()
    tcp_server.bind(5557)
    tcp_server.bind(5556)
    tcp_server.bind(5555)
    tcp_server.start(0)
    IOLoop.current().start()


if __name__ == '__main__':
    main()
