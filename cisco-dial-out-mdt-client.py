import requests
import json
import logging
import socket
import selectors
from argparse import ArgumentParser


class SelectorServer(object):
    def __init__ (self, host, port, logger):
        self.main_socket = socket.socket()
        self.main_socket.bind((host, port))
        self.main_socket.listen(100)
        self.logger = logger
        self.selector = selectors.DefaultSelector()
        self.selector.register(fileobj=self.main_socket, events=selectors.EVENT_READ, data=self.on_accept)

    def on_accept(self, sock, mask):
        conn, address = self.main_socket.accept()
        self.logger.info(f"Accepted connection from {address}")
        self.selector.register(fileobj=conn, events=selectors.EVENT_READ, data=self.on_read)

    def close_connection(self, conn):
        self.logger.info(f"Closing connection to {conn}")
        self.selector.unregister(conn)
        conn.close()

    def on_read(self, conn, mask):
        still_data = True
        output = ''
        try:
            peer_name = conn.getpeername()
            while still_data:
                data = conn.recv(256000)
                data = data.decode('latin-1').encode("utf-8")
                data = str(data)[2:]
                data = data[:-1]
                if data:
                    output = output + data
                    if '"collection_end_time":' in data and not '"collection_end_time":0' in data:
                        self.logger.info(f"Got all info from {peer_name}")
                        still_data = False
                else:
                    still_data = False
        except ConnectionResetError:
            self.close_connection(conn)
        finally:
            try:
                print(output)
            except:
                self.close_connection(conn)
                exit(1)

    def collect(self):
        while True:
            events = self.selector.select()
            for key, mask in events:
                handler = key.data
                handler(key.fileobj, mask)


def main():
    parser = ArgumentParser()
    parser.add_argument("-a", "--address", dest="address", help="Server ip address to bind to", required=True)
    parser.add_argument("-p", "--port", dest="port", help="Server Port to listen on", required=True)
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
    server = SelectorServer(args.address, args.port, logger)
    server.collect()



if __name__ == '__main__':
    main()
