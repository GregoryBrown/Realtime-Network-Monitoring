import sys

sys.path.append("../")

import urllib
import grpc
import json
import logging
import traceback
from utils.utils import process_cisco_encoding
from logging.handlers import RotatingFileHandler
from argparse import ArgumentParser
from tornado.tcpserver import TCPServer
from tornado.iostream import StreamClosedError
from tornado.httpclient import AsyncHTTPClient, HTTPError, HTTPRequest
from tornado.locks import Lock
from tornado.ioloop import IOLoop
from struct import Struct, unpack
from tornado.netutil import bind_sockets
from tornado.process import fork_processes, task_id
from multiprocessing import current_process


class Error(Exception):
    pass


class GetIndexListError(Error):
    def __init__(self, code, response_json, message, traceback):
        self.code = code
        self.response = response_json
        self.message = message
        self.traceback = traceback


class PostDataError(Error):
    def __init__(self, code, response_json, data, message, traceback):
        self.code = code
        self.response = response_json
        self.data = data
        self.message = message
        self.traceback = traceback


class PutIndexError(Error):
    def __init__(self, code, response_json, index, message, traceback):
        self.code = code
        self.response = response_json
        self.index = index
        self.message = message
        self.traceback = traceback


class TelemetryTCPDialOutServer(TCPServer):
    def __init__(self, elasticsearch_server):
        super().__init__()
        self.elastic_server = elasticsearch_server
        self.lock = Lock()
        # log_name = __file__.strip('.py')
        self.log = self.init_log(f"{current_process().name}-{task_id()}.log")
        self.lock = Lock()
        self.http_client = AsyncHTTPClient()

    async def get_index_list(self, url):
        try:
            indices = []
            response = await self.http_client.fetch(f"http://{self.elastic_server}:9200/*")
            response = json.loads(response.body.decode())
            for key in response:
                if not key.startswith("."):
                    indices.append(key)
            return indices
        except HTTPError as e:
            raise e
        except Exception as e:
            raise GetIndexListError(response.code, str(e), "Got Exception while trying to get index list")

    async def post_data(self, data_to_post):
        try:
            headers = {"Content-Type": "application/x-ndjson"}
            url = f"http://{self.elastic_server}:9200/_bulk"
            request = HTTPRequest(url=url, method="POST", headers=headers, body=data_to_post)
            response = await self.http_client.fetch(request=request)
            return True
        except HTTPError as e:
            raise e
        except HTTPTimeoutError as e:
            return False
        except Exception as e:
            raise PostDataError(
                response.code, str(e), data_to_post, "Error while posting data to ElasticSearch",
            )

    async def put_index(self, index):
        try:
            url = f"http://{self.elastic_server}:9200/{index}"
            headers = {"Content-Type": "application/json"}
            mapping = '{"mappings": {"properties": {"@timestamp": {"type": "date"}}}}'
            request = HTTPRequest(url=url, method="PUT", headers=headers, body=mapping)
            response = await self.http_client.fetch(request)
            return True
        except HTTPError as e:
            if e.code == 400 and e.message == "Bad Request":
                return False
            else:
                raise e
        except Exception as e:
            raise PutIndexError(
                response.code, str(e), index, "Error while putting index to ElasticSearch",
            )

    def init_log(self, log_name):
        logger = logging.getLogger(log_name)
        file_handler = RotatingFileHandler(log_name, maxBytes=536870912, backupCount=2)
        screen_handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s")
        file_handler.setFormatter(formatter)
        screen_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(screen_handler)
        logger.setLevel(logging.DEBUG)
        return logger

    async def handle_stream(self, stream, address):
        try:
            HEADER_SIZE = 12
            header_struct = Struct(">hhhhi")
            _UNPACK_HEADER = header_struct.unpack
            self.log.info(f"Got Connection from {address[0]}:{address[1]}")
            while not stream.closed():
                header_data = await stream.read_bytes(HEADER_SIZE)
                msg_type, encode_type, msg_version, flags, msg_length = _UNPACK_HEADER(header_data)
                encoding = {1: "gpb", 2: "json"}[encode_type]
                msg_data = b""
                if encode_type == 1:
                    while len(msg_data) < msg_length:
                        packet = await stream.read_bytes(msg_length - len(msg_data))
                        msg_data += packet
                sorted_by_index = {}
                converted_decode_segments = process_cisco_encoding([msg_data])
                for converted_decode_segment in converted_decode_segments:
                    if not converted_decode_segment["_index"] in sorted_by_index.keys():
                        sorted_by_index[converted_decode_segment["_index"]] = [converted_decode_segment]
                    else:
                        sorted_by_index[converted_decode_segment["_index"]].append(converted_decode_segment)

                index_list = await self.get_index_list(f"http://{self.elastic_server}:9200/*")
                for index in sorted_by_index.keys():
                    if index not in index_list:
                        async with self.lock:
                            index_list = await self.get_index_list(f"http://{self.elastic_server}:9200/*")
                            if index not in index_list:
                                self.log.info("Acciqured lock to put index in elasticsearch")
                                put_rc = False
                                while not put_rc:
                                    put_rc = await self.put_index(index)
                                index_list.append(index)

                    segment_list = sorted_by_index[index]
                    elastic_index = {"index": {"_index": f"{index}"}}
                    payload_list = [elastic_index]
                    for segment in segment_list:
                        segment.pop("_index", None)
                        payload_list.append(segment)
                        payload_list.append(elastic_index)
                    payload_list.pop()
                    data_to_post = "\n".join(json.dumps(d) for d in payload_list)
                    data_to_post += "\n"
                    await self.post_data(data_to_post)

        except StreamClosedError as e:
            self.log.error(e)
            stream.close()

        except GetIndexListError as e:
            self.log.error(traceback.print_exc())
            self.log.error(e.code)
            self.log.error(e.response)
            self.log.error(e.message)
            self.log.error(f"Closing connection from {address[0]} due to get index list error")
            stream.close()
        except PostDataError as e:
            self.log.error(traceback.print_exc())
            self.log.error(e.code)
            self.log.error(e.response)
            self.log.error(e.data)
            self.log.error(e.message)
            self.log.error(f"Closing connection from {address[0]} due to posting data error")
            stream.close()
        except PutIndexError as e:
            self.log.error(traceback.print_exc())
            self.log.error(e.code)
            self.log.error(e.response)
            self.log.error(e.index)
            self.log.error(e.message)
            self.log.error(f"Closing connection from {address[0]} due to putting index error")
            stream.close()

        except Exception as e:
            self.log.error(e)
            self.log.error(traceback.print_exc())
            self.log.error(f"Closing connection from {address[0]} due to generic error")
            stream.close()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-a", "--host", dest="host", help="host", required=True)
    parser.add_argument("-r", "--port", dest="port", help="port", required=True)
    parser.add_argument(
        "-e", "--elastic_server", dest="elastic_server", help="Elastic Server", required=True,
    )
    args = parser.parse_args()
    sockets = bind_sockets(args.port)
    fork_processes(0)
    tcp_server = TelemetryTCPDialOutServer(args.elastic_server)
    tcp_server.add_sockets(sockets)
    tcp_server.log.info(f"Starting listener on {args.host}:{args.port}")
    IOLoop.current().start()
