import logging
import socket
import selectors
from argparse import ArgumentParser
from databases import InfluxDBUploader
from utils import format_mdt_output

class SelectorServer(object):
    def __init__(self, host, port, logger, database):
        self.main_socket = socket.socket()
        self.main_socket.bind((host, port))
        self.main_socket.listen(100)
        self.logger = logger
        self.selector = selectors.DefaultSelector()
        self.selector.register(fileobj=self.main_socket, events=selectors.EVENT_READ, data=self.on_accept)
        self.database = database

    def on_accept(self, sock, mask):
        conn, address = self.main_socket.accept()
        self.logger.info(f'Accepted connection from {address}')
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
                data = format_mdt_output(output)
                upload_result = self.database.upload_to_db(data)
                if not upload_result[0]:
                    self.logger.error("Error while posting data to database")
                    self.logger.error(upload_result)
                    exit(1)
            except Exception as e:
                self.logger.error(e)
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
    server = SelectorServer(args.address, int(args.port), logger, influxdb)
    server.collect()


if __name__ == '__main__':
    main()
