import sys
sys.path.append("../")

from utils.utils import process_cisco_encoding
from py_protos.telemetry_pb2 import Telemetry
from argparse import ArgumentParser
from struct import Struct, unpack
from aiohttp import ClientSession
import grpc
import logging
import asyncio
import json

        
class ClientConnection(object):
    def __init__(self, elastic_server):
        self.elastic_server = elastic_server
        self.lock = asyncio.Lock()

    async def get_index_list(self, url):
        indices = []
        async with ClientSession() as session:
            async with session.get(url) as response:
                response = await response.read()
                response = json.loads(response.decode())
                for key in response:
                    if not key.startswith('.'):
                        indices.append(key)
                return indices



    async def post_data(self, data_to_post):
        headers = {'Content-Type': "application/x-ndjson"}
        url = f"http://{self.elastic_server}:9200/_bulk"
        async with ClientSession() as session:
            response = await session.post(url, data=data_to_post, headers=headers)
            return response       

            
    async def put_index(self, index):
        url = f"http://{self.elastic_server}:9200/{index}"
        headers = {'Content-Type': "application/json"}
        mapping = {"mappings": {"properties": {"@timestamp": {"type": "date"}}}}
        async with ClientSession() as session:
            response = await session.put(url, json=mapping, headers=headers)
            return response
        
    async def handle_connection(self, reader, writer):
        HEADER_SIZE = 12
        header_struct = Struct('>hhhhi')
        _UNPACK_HEADER = header_struct.unpack
        address = writer.get_extra_info('peername')
        #print(f"Got connection from {address}")
        while True:
            try:
                header_data = await reader.read(HEADER_SIZE)
                msg_type, encode_type, msg_version, flags, msg_length = _UNPACK_HEADER(header_data)
                encoding = {1:'gpb', 2:'json'}[encode_type]
                msg_data = b''
                address = writer.get_extra_info('peername')
                if encode_type == 1:
                    #print(f'Got {msg_length} bytes from {address} with encoding {encoding}')
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
                                print('Acciqured lock to put index in elasticsearch')
                                response = await self.put_index(index)
                                if response.status is not 200:
                                    print("Error")
                                    print(await response.json())
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
                            print("Error:")
                            print(await response.json())
            except Exception as e:
                import traceback
                traceback.print_exc()
                print(e)
                print(address)
                print('Closing Session')
                break




async def dial_out_server(args):
    conn_handler = ClientConnection(args.elastic_server)
    server = await asyncio.start_server(
        conn_handler.handle_connection, args.host, args.port)

    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    async with server:
        await server.serve_forever()


def main():
    parser = ArgumentParser()
    parser.add_argument("-a", "--host", dest="host", help="host", required=True)
    parser.add_argument("-r", "--port", dest="port", help="port", required=True)
    parser.add_argument("-e", "--elastic_server", dest="elastic_server", help="Elastic Server", required=True)
    args = parser.parse_args()
    asyncio.run(dial_out_server(args))


if __name__ == '__main__':
    main()
