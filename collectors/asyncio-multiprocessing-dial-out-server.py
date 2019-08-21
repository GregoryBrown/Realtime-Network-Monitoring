import sys
sys.path.append("../")

from utils.utils import process_cisco_encoding
from py_protos.telemetry_pb2 import Telemetry
from argparse import ArgumentParser
from struct import Struct, unpack
from aiohttp import ClientSession
from logging.handlers import RotatingFileHandler, QueueHandler
from multiprocessing import Process
import grpc
import logging
import asyncio
import json
import logging 
import traceback
import uvloop

class Error(Exception):
    pass

class GetIndexListError(Error):
    def __init__(self, traceback, response_json, message, e):
        self.traceback = traceback
        self.response = response_json
        self.message = message
        self.exception = e

class PostDataError(Error):
    def	__init__(self, traceback, response_json, data, message, e):
        self.traceback = traceback
        self.response =	response_json
        self.data = data
        self.message = message
        self.exception = e

class PutIndexError(Error):
    def	__init__(self, traceback, response_json,  message, e):
        self.traceback = traceback
        self.response =	response_json
        self.message = message
        self.exception = e
        
class ElasticSearchError(Error):
    def	__init__(self, response_json, message):
        self.response =	response_json
        self.message = message
        

class ClientConnection(object):
    def __init__(self, elastic_server):
        self.elastic_server = elastic_server
        self.lock = asyncio.Lock()
        self.log = None
        
    async def get_index_list(self, url):
        indices = []
        try:
            async with ClientSession() as session:
                async with session.get(url) as response:
                    response = await response.read()
                    response = json.loads(response.decode())
                    for key in response:
                        if not key.startswith('.'):
                            indices.append(key)
            return indices
        except Exception as e:
            raise GetIndexListError(traceback.print_exc(), response, "Got Exception while trying to get index list", e)
        
    async def post_data(self, data_to_post):
        headers = {'Content-Type': "application/x-ndjson"}
        url = f"http://{self.elastic_server}:9200/_bulk"
        try:
            async with ClientSession() as session:
                response = await session.post(url, data=data_to_post, headers=headers)
            return response       
        except Exception as e:
            raise PostDataError(traceback.print_exc(), data_to_post, "Got Exception while trying to post data", e)

            
    async def put_index(self, index):
        url = f"http://{self.elastic_server}:9200/{index}"
        headers = {'Content-Type': "application/json"}
        mapping = {"mappings": {"properties": {"@timestamp": {"type": "date"}}}}
        try:
            async with ClientSession() as session:
                response = await session.put(url, json=mapping, headers=headers)
            return response
        except Exception as e:
            raise PutDataError(traceback.print_exc(), response, f"Got Exception while trying to put index {index}", e)


    async def init_logger(self, address):
        log_name = "dial-out.log"
        log = logging.getLogger(log_name)
        log.setLevel(logging.INFO)
        file_handler = RotatingFileHandler(log_name, maxBytes=536870912, backupCount=2)
        screen_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
        file_handler.setFormatter(formatter)
        screen_handler.setFormatter(formatter)
        log.addHandler(file_handler)
        log.addHandler(screen_handler)
        return log
    
    async def handle_connection(self, reader, writer):
        try:
            HEADER_SIZE = 12
            header_struct = Struct('>hhhhi')
            _UNPACK_HEADER = header_struct.unpack
            address = writer.get_extra_info('peername')
            if self.log is None:
                self.log = await self.init_logger(address)
            self.log.info(f"Got Connection from {address[0]}:{address[1]}")
            while True:
                header_data = await reader.read(HEADER_SIZE)
                msg_type, encode_type, msg_version, flags, msg_length = _UNPACK_HEADER(header_data)
                encoding = {1:'gpb', 2:'json'}[encode_type]
                msg_data = b''
                if encode_type == 1:
                    while len(msg_data) < msg_length:
                        packet = await reader.read(msg_length - len(msg_data))
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
                                response = await self.put_index(index)
                                if response.status is not 200:
                                    raise ElasticSearchError(await response.json(), "Unable to put index into Elasticsearch")
                                else:
                                    index_list.append(index)
                    else:
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
                        response = await self.post_data(data_to_post)
                        if response.status is not 200:
                            raise ElasticSearchError(await response.json(), "Unable to put data into Elasticsearch")
        except  GetIndexListError as e:
            self.log.error(traceback.print_exc())
            self.log.error(e.message)
            self.log.error(e.traceback)
            self.log.error(e.response)
            self.log.error(e.exception)
            await writer.drain()
            self.log.error(f"Closing connection from {address[0]} due to get index list error")
            writer.close()
        except PostDataError as e:
            self.log.error(traceback.print_exc())
            self.log.error(e.message)
            self.log.error(e.traceback)
            self.log.error(e.response)
            self.log.error(e.exception)
            self.log.error(e.data)
            await writer.drain()
            self.log.error(f"Closing connection from {address[0]} due to posting data error")
            writer.close()
        except PutIndexError as e:
            self.log.error(traceback.print_exc())
            self.log.error(e.message)
            self.log.error(e.traceback)
            self.log.error(e.response)
            await writer.drain()
            self.log.error(f"Closing connection from {address[0]} due to putting index error")
            writer.close()
        except ElasticSearchError as e:
            self.log.error(traceback.print_exc())
            self.log.error(e.message)
            self.log.error(e.response)
            await writer.drain()
            self.log.error(f"Closing connection from {address[0]} due to elasticsearch error")
            writer.close()
        except Exception as e:
            self.log.error(e)
            self.log.error(traceback.print_exc())
            await writer.drain()
            self.log.error(f"Closing connection from {address[0]} due to generic error")
            writer.close()




class Conn(Process):
    def __init__(self, address, reader, writer):
        super().__init__()
        self.address = address
        self.client_connection = ClientConnection()
        
    def run(self):
        print(f"Got connection from {self.address[0]}:{self.address[1]} on process {self.name}")
        self.

async def handle_connection(reader, writer):
    address = writer.get_extra_info('peername')
    conn = Conn(address, reader, writer)
    conn.start()
    
        
async def dial_out_server(args):
    #conn_handler = ClientConnection(args.elastic_server)
    #server = await asyncio.start_server(
    #    conn_handler.handle_connection, args.host, args.port)

    #addr = server.sockets[0].getsockname()
    #print(f'Serving on {addr}')

    #async with server:
    #   await server.serve_forever()
    server = await asyncio.start_server(handle_connection, args.host, args.port)
    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')
    async with server:
        await server.serve_forever()





if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-a", "--host", dest="host", help="host", required=True)
    parser.add_argument("-r", "--port", dest="port", help="port", required=True)
    parser.add_argument("-e", "--elastic_server", dest="elastic_server", help="Elastic Server", required=True)
    args = parser.parse_args()
    asyncio.run(dial_out_server(args))


    
