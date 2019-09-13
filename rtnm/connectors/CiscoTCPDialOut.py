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
from utils.utils import process_cisco_encoding, init_log

class TelemetryTCPDialOutServer(TCPServer):
    def __init__(self, elasticsearch_server, elasticsearch_port, batch_size, path):
        super().__init__()
        self.elastic_server = elasticsearch_server
        self.elastic_server_port = elasticsearch_port
        self.url = f"http://{self.elastic_server}:{self.elastic_server_port}"
        self.lock = Lock()
        self.batch_size = batch_size
        AsyncHTTPClient.configure(None, max_clients=1000)
        self.http_client = AsyncHTTPClient()
        try:
            self.log = init_log(f"{current_process().name}-{task_id()}.log", path)
        except Exception as e:
            print(e)
            exit(1)

        
    async def get_index_list(self):
        try:
            index_list = []
            response = await self.http_client.fetch(f"{self.url}/*")
            response = json.loads(response.body.decode())
            for key in response:
                if not key.startswith('.'):
                    index_list.append(key)
            return index_list
        except Exception as e:
            raise GetIndexListError(response.code, str(e), "Got Exception while trying to get index list")
            

    async def post_data(self, data_to_post):
        try:
            headers = {'Content-Type': "application/x-ndjson"}
            request = HTTPRequest(url=f"{self.url}/_bulk", method="POST", headers=headers, body=data_to_post)
            response = await self.http_client.fetch(request=request)
            return True
        except HTTPError as e:
            if e.code == 599:
                return False
            else:
                raise e
        except Exception as e:
            raise PostDataError(response.code, str(e), data_to_post, "Error while posting data to ElasticSearch")

    
    async def put_index(self, index):
        try:
            headers = {'Content-Type': "application/json"}
            mapping = '{"mappings": {"properties": {"@timestamp": {"type": "date"}}}}'
            request = HTTPRequest(url=f"{self.url}/{index}", method="PUT", headers=headers, body=mapping)
            response = await self.http_client.fetch(request)
            return True
        except HTTPError as e:
            if e.code == 400 and e.message == "Bad Request":
                return False
            elif e.code == 599:
                return False
            else:
                raise e
            
        except Exception as e:
            raise PutIndexError(response.code, str(e), index, "Error while putting index to ElasticSearch")



    
    async def handle_stream(self, stream, address):
        try:
            self.index_list = await self.get_index_list()
            HEADER_SIZE = 12
            header_struct = Struct('>hhhhi')
            _UNPACK_HEADER = header_struct.unpack
            self.log.info(f"Got Connection from {address[0]}:{address[1]}")
            access_log = logging.getLogger("tornado.access")
            app_log = logging.getLogger("tornado.application")
            gen_log = logging.getLogger("tornado.general")
            
            access_log.setLevel(logging.DEBUG)
            app_log.setLevel(logging.DEBUG)
            gen_log.setLevel(logging.DEBUG)
            
            screen_handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
            screen_handler.setFormatter(formatter)
            access_log.addHandler(screen_handler)
            app_log.addHandler(screen_handler)
            gen_log.addHandler(screen_handler)
            while not stream.closed():
                batch_list = []
                while len(batch_list) < self.batch_size:
                    header_data = await stream.read_bytes(HEADER_SIZE)
                    msg_type, encode_type, msg_version, flags, msg_length = _UNPACK_HEADER(header_data)
                    encoding = {1:'gpb', 2:'json'}[encode_type]
                    msg_data = b''
                    while len(msg_data) < msg_length:
                        packet = await stream.read_bytes(msg_length - len(msg_data))
                        msg_data += packet
                    batch_list.append(msg_data)
                    print(len(batch_list))
                sorted_by_index = {}
                converted_decode_segments = process_cisco_encoding(batch_list)
                if converted_decode_segments == None:
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
                                self.index_list = await self.get_index_list()
                                while not put_rc:
                                    put_rc = await self.put_index(index)
                                self.index_list.append(index)
                    segment_list = sorted_by_index[index]
                    elastic_index = {'index': {'_index': f'{index}'}}
                    payload_list = [elastic_index]
                    for segment in segment_list:
                        segment.pop('_index', None)
                        payload_list.append(segment)
                        payload_list.append(elastic_index)
                    payload_list.pop()
                    data_to_post = '\n'.join(json.dumps(d) for d in payload_list)
                    data_to_post += '\n'
                    await self.post_data(data_to_post)

        except DecodeError as e:
            self.log.error(e)
            self.log.error(f"Unable to upload data to Elasticsearch due to decode error")
        except StreamClosedError as e:
            print(dir(e))
            print(e.errno)
            print(e.real_error)
            print(e.filename)
            print(e.filename2)
            print(e.strerror)
            print(e.args)
            self.log.error(e)
            self.log.error(f"Getting closed stream from {address[0]}:{address[1]}")
        except  GetIndexListError as e:
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
