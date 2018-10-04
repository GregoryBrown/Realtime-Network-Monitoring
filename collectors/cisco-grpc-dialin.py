import sys
sys.path.append("../")

from utils.multi_process_logging import  MultiProcessQueueLoggingListner, MultiProcessQueueLogger
from utils.connectors import DialInClient, TLSDialInClient
from google.protobuf import json_format
from collections import defaultdict
from argparse import ArgumentParser
from requests import request
from multiprocessing import Pool, Manager, Queue
from collections import defaultdict
from utils.utils import format_output, _format_fields
from py_protos.telemetry_pb2 import Telemetry
import grpc
import json
import logging


def elasticsearch_upload(batch_list, args, lock, index_list, log_queue):
    process_logger = MultiProcessQueueLogger('cisco-grpc-dialin.log', log_queue)
    process_logger.logger.info('Got into the process')
    decode_segments = []
    converted_decode_segments = []
    sorted_by_index = {}
    #Decode all segments
    for segment in batch_list:
        telemetry_pb = Telemetry()                                                                                                                                                                     
        telemetry_pb.ParseFromString(segment)
        decode_segments.append(json_format.MessageToJson(telemetry_pb))
    #Convert decode segments into elasticsearch messages, and save to list for upload 
    for decode_segment in decode_segments:
    #for decode_segment in segments:
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
                    process_logger.logger.info('Acciqured lock to put index in elasticsearch')
                    headers = {'Content-Type': "application/json"}
                    mapping = {"settings": {"index.mapping.total_fields.limit": 2000},"mappings": {"nodes": {
                        "properties": {"type": {"type": "keyword"},"keys": {"type": "object"},"content": {"type": "object"},"timestamp": {"type": "date"}}}}}
                    index_put_response = request("PUT", index_url, headers=headers, json=mapping)
                    if not index_put_response.status_code == 200:
                        process_logger.logger.error("Error when creating index")
                        process_logger.logger.error(index_put_response.status_code)
                        process_logger.logger.error(index_put_response.json())
                        return False
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
            process_logger.logger.error("Error while uploading in bulk")
            return False
    return True



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
    log_queue = Manager().Queue()
    elastic_lock = Manager().Lock()
    sensor_list = Manager().list()
    log_listener = MultiProcessQueueLoggingListner('cisco-grpc-dialin.log', log_queue)
    log_listener.start()
    main_logger = MultiProcessQueueLogger('cisco-grpc-dialin.log', log_queue)
    if args.tls:
        if args.pem:
            client = TLSDialInClient(args.host, args.port, args.pem, user=args.username, password=args.password)
        else:
            main_logger.logger.error("Need pem file when using tls")
            main_logger.queue.put(None)
            log_listener.join()
            exit(0)            
    else:
        client = DialInClient(args.host, args.port, user=args.username, password=args.password)
    get_all_sensors_url = f"http://{args.elastic_server}:9200/*"
    get_all_sensors_response = request("GET", get_all_sensors_url)
    if not get_all_sensors_response.status_code == 200:
        main_logger.logger.error("Error getting all devices to populate the device list")
        main_logger.queue.put(None)
        log_listener.join()
        exit(0)
    for key in get_all_sensors_response.json():
        if not key.startswith('.'):
            sensor_list.append(key)
    if not client.isconnected():
        client.connect()
    with Pool() as pool:
        batch_list = []
        for response in client.subscribe(args.sub):
            batch_list.append(response.data)
            if len(batch_list) >= int(args.batch_size):
                result = pool.apply_async(elasticsearch_upload, (batch_list, args, elastic_lock, sensor_list, log_queue, ))
                print(result.get())
                #elasticsearch_upload(batch_list, args, elastic_lock, sensor_list, log_queue)
                del batch_list
                batch_list = []
                                 
        
    main_logger.queue.puut(None)
    log_listener.join()
if __name__ == '__main__':
    main()
