import sys
sys.path.append("../")

from utils.multi_process_logging import *
from utils.connectors import DialInClient, TLSDialInClient
from py_protos.telemetry_pb2 import Telemetry
from databases import databases
from google.protobuf import json_format
from collections import defaultdict
from argparse import ArgumentParser
from requests import request
from multiprocessing import Pool, Manager
from collections import defaultdict
from utils.utils import format_output, _format_fields
import grpc
import json
import logging


def elasticsearch_upload(segments, args, lock, index_list):
    decode_segments = []
    converted_decode_segments = []
    sorted_by_index = {}
    #Decode all segments
    for segment in segments:
        telemetry_pb = Telemetry()                                                                                                                                                                     
        telemetry_pb.ParseFromString(segment.data)                                                                                                                               
        decode_segments.append(json_format.MessageToJson(telemetry_pb))
    #Convert decode segments into elasticsearch messages, and save to list for upload 
    for decode_segment in decode_segments:
        for output in format_output(decode_segment):
            converted_decode_segments.append(json.loads(output))
    #Sort all segments by index
    for converted_decode_segment in converted_decode_segments:
        if not converted_decode_segment["encode_path"] in sorted_by_index.keys():
            sorted_by_index[converted_decode_segment["encode_path"]] = [converted_decode_segment]
        else:
            sorted_by_index[converted_decode_segment["encode_path"]].append(converted_decode_segment)
    #Bulk upload each index to elasticsearch
    for key in sorted_by_index.keys():
        index = key.replace('/','-').lower()
        index_url = f"http://{args.elastic_server}:9200/{index}"
        if not index in index_list:
            with lock:
                if not index in index_list:
                    print('Acciqured lock to put index in elasticsearch')
                    headers = {'Content-Type': "application/json"}
                    mapping = {"settings": {"index.mapping.total_fields.limit": 2000},"mappings": {"nodes": {
                        "properties": {"type": {"type": "keyword"},"keys": {"type": "object"},"content": {"type": "object"},"timestamp": {"type": "date"}}}}}
                    index_put_response = request("PUT", index_url, headers=headers, json=mapping)
                    if not index_put_response.status_code == 200:
                        print("Error when creating index")
                        print(index_put_response.status_code)
                        print(index_put_response.json())
                        exit(0)
                    index_list.append(index)
        data_url = f"http://{args.elastic_server}:9200/_bulk"
        segment_list = sorted_by_index[key]
        elastic_index = {'index': {'_index': f'{index}', '_type': 'nodes'}}
        payload_list = []
        payload_list.append(elastic_index)
        for segment in segment_list:
            payload_list.append(segment)
            payload_list.append(elastic_index)
        payload_list.pop()
        data_to_post = '\n'.join(json.dumps(d) for d in payload_list)
        data_to_post += '\n'
        headers = {'Content-Type': "application/x-ndjson"}
        reply = request("POST", data_url, data=data_to_post, headers=headers)
        if reply.json()['errors']:
            print("Error while uploading in bulk")
            exit(0)



def main():
    parser = ArgumentParser()
    parser.add_argument("-s", "--subscription", dest="sub", help="Subscription name", required=True)
    parser.add_argument("-u", "--username", dest="username", help="Username", required=True)
    parser.add_argument("-p", "--password", dest="password", help="Password", required=True)
    parser.add_argument("-a", "--host", dest="host", help="host", required=True)
    parser.add_argument("-r", "--port", dest="port", help="port", required=True)
    parser.add_argument("-b", "--batch_size", dest="batch_size", help="Batch size", required=True)
    parser.add_argument("-e", "--elastic_server", dest="elastic_server", help="Elastic Server", required=True)
    parser.add_argument("-t", "--tls", dest="tls", help="TLS enabled", required=False, action='store_true')
    parser.add_argument("-m", "--pem", dest="pem", help="pem file", required=False)
    args = parser.parse_args()
    logger = logging.getLogger('Cisco-gRPC-Dialin')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    if args.tls:
        if args.pem:
            client = TLSDialInClient(args.host, args.port, args.pem, user=args.username, password=args.password)
        else:
            logger.error("Need pem file when using tls")
            exit(0)
    else:
        client = DialInClient(args.host, args.port, user=args.username, password=args.password)
    elastic_lock = Manager().Lock()
    sensor_list = Manager().list()
    get_all_sensors_url = f"http://{args.elastic_server}:9200/*"
    get_all_sensors_response = request("GET", get_all_sensors_url)
    if not get_all_sensors_response.status_code == 200:
        logger.error("Error getting all devices to populate the device list")
        exit(0)
    for key in get_all_sensors_response.json():
        if not key.startswith('.'):
            sensor_list.append(key)
    if not client.isconnected():
        client.connect()
    with Pool() as pool:
        batch_list = []
        for response in client.subscribe(args.sub):
            batch_list.append(response)
            if len(batch_list) >= int(args.batch_size):
                result = pool.apply_async(elasticsearch_upload, (batch_list, args, elastic_lock, sensor_list, logger_queue ))
                #elasticsearch_upload(batch_list, args, elastic_lock, sensor_list)
                del batch_list
                batch_list = []
                for log in list(logger.queue):
                    
        
        
if __name__ == '__main__':
    main()
