import logging
import traceback
import json
from logging.handlers import RotatingFileHandler
from tornado.tcpserver import TCPServer
from tornado.iostream import StreamClosedError
from tornado.httpclient import AsyncHTTPClient, HTTPError, HTTPRequest
from google.protobuf.message import DecodeError
from tornado.locks import Lock
from multiprocessing import current_process
from tornado.process import task_id
from tornado import gen
from struct import Struct, unpack
from errors.errors import GetIndexListError, PostDataError, PutIndexError
from converters.converters import DataConverter
from aiohttp import ClientSession


class TelemetryTCPDialOutServer(TCPServer):
    def __init__(self, output, batch_size, log_name):
        try:
            super().__init__()
            self.batch_size = int(batch_size)
            self.url = f"http://{output['address']}:{output['port']}"
            self.lock = Lock()
            AsyncHTTPClient.configure(None, max_clients=1000)
            self.http_client = AsyncHTTPClient()
            self.index_list = None
            # self.log = init_logs([f"{current_process().name}-{task_id()}.log"], path)
            self.log = logging.getLogger(log_name)
        except Exception as e:
            print(e)
            print("Failed in init code")
            exit(1)

    async def get_index_list(self):
        try:
            index_list = []
            # async with ClientSession() as session:
            #    async with session.get("http://httpbin.org/headers") as r:
            #        response = await r.json()
            request = HTTPRequest(url=f"{self.url}/*", connect_timeout=40.0, request_timeout=40.0)
            response = await self.http_client.fetch(request)
            response = json.loads(response.body.decode())
            for key in response:
                if not key.startswith("."):
                    index_list.append(key)
            return index_list
        except Exception as e:
            raise GetIndexListError(response.status, str(e), "Got Exception while trying to get index list")

    async def post_data(self, data_to_post):
        try:
            headers = {"Content-Type": "application/x-ndjson"}
            request = HTTPRequest(
                url=f"{self.url}/_bulk",
                method="POST",
                headers=headers,
                body=data_to_post,
                connect_timeout=40.0,
                request_timeout=40.0,
            )
            response = await self.http_client.fetch(request=request)
            return True
        except HTTPError as e:
            self.log.error(traceback.format_exc())
            if e.code == 599:
                self.log.error(f"Hit a TimeoutError in POST data {e}")

                return False
            else:
                self.log.error(e)
                raise e
        except Exception as e:
            raise PostDataError(
                response.code, str(e), data_to_post, "Error while posting data to ElasticSearch",
            )

    async def put_index(self, index):
        try:
            self.log.info(f"Putting {index} into Elasticsearch")
            headers = {"Content-Type": "application/json"}
            mapping = '{"mappings": {"properties": {"@timestamp": {"type": "date"}}}}'
            request = HTTPRequest(
                url=f"{self.url}/{index}",
                method="PUT",
                headers=headers,
                body=mapping,
                connect_timeout=40.0,
                request_timeout=40.0,
            )
            response = await self.http_client.fetch(request)
            return True
        except HTTPError as e:
            self.log.error(traceback.format_exc())
            if e.code == 400 and e.message == "Bad Request":
                self.log.error(f"Hit a Bad request in PUT index")
                return False
            elif e.code == 599:
                self.log.error(f"Hit a TimeoutError in PUT index {e}")
                return False
            else:
                raise e

        except Exception as e:
            raise PutIndexError(
                response.code, str(e), index, "Error while putting index to ElasticSearch",
            )

    async def handle_stream(self, stream, address):
        try:
            self.index_list = await self.get_index_list()
            _HEADER_SIZE = 12
            header_struct = Struct(">hhhhi")
            _UNPACK_HEADER = header_struct.unpack
            self.log.info(f"Got Connection from {address[0]}:{address[1]}")
            while not stream.closed():
                batch_list = []
                while len(batch_list) < self.batch_size:
                    header_data = await stream.read_bytes(_HEADER_SIZE)
                    (msg_type, encode_type, msg_version, flags, msg_length,) = _UNPACK_HEADER(header_data)
                    encoding = {1: "gpb", 2: "json"}[encode_type]
                    # implement json encoding
                    msg_data = b""
                    while len(msg_data) < msg_length:
                        packet = await stream.read_bytes(msg_length - len(msg_data))
                        msg_data += packet
                    batch_list.append(msg_data)
                sorted_by_index = {}
                data_converter = DataConverter(batch_list, self.log, "cisco-ems")
                converted_decode_segments = data_converter.process_batch_list()
                if converted_decode_segments is None:
                    self.log.error("Error parsing and decoding message")
                else:
                    for converted_decode_segment in converted_decode_segments:
                        if not converted_decode_segment["_index"] in sorted_by_index.keys():
                            sorted_by_index[converted_decode_segment["_index"]] = [converted_decode_segment]
                        else:
                            sorted_by_index[converted_decode_segment["_index"]].append(converted_decode_segment)
                    for index in sorted_by_index.keys():
                        if index not in self.index_list:
                            async with self.lock:
                                self.log.info("Acquired lock to put index in elasticsearch")
                                put_rc = False
                                while not put_rc:
                                    self.index_list = await self.get_index_list()
                                    if index not in self.index_list:
                                        put_rc = await self.put_index(index)
                                    else:
                                        put_rc = True
                                self.index_list.append(index)
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

        except DecodeError as e:
            self.log.error(e)
            self.log.error(f"Unable to upload data to Elasticsearch due to decode error")
        except StreamClosedError as e:
            self.log.error(traceback.print_exc())
            self.log.error(e)
            self.log.error(f"Getting closed stream from {address[0]}:{address[1]}")
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
