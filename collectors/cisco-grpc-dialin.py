import sys
sys.path.append("../")

from utils.exceptions import DeviceFailedToConnect, FormatDataError
from utils.multi_process_logging import  MultiProcessQueueLoggingListner, MultiProcessQueueLogger
from utils.connectors import DialInClient, TLSDialInClient
from google.protobuf import json_format
from collections import defaultdict
from argparse import ArgumentParser
from requests import request
from multiprocessing import Pool, Manager, Queue, Lock, Value
from queue import Empty
from collections import defaultdict
from utils.utils import format_output, _format_fields
from py_protos.telemetry_pb2 import Telemetry
from ctypes import c_bool
import grpc
import json
import logging
import traceback
import multiprocessing

def elasticsearch_upload(batch_list, args, lock, index_list, log_name):
    decode_segments = []
    converted_decode_segments = []
    sorted_by_index = {}
    process_logger = logging.getLogger(log_name)
    #Decode all segments
    for segment in batch_list:
        telemetry_pb = Telemetry()                                             
        telemetry_pb.ParseFromString(segment)
        decode_segments.append(json_format.MessageToJson(telemetry_pb))
        #print(telemetry_pb)
    #Convert decode segments into elasticsearch messages, and save to list for upload 
    for decode_segment in decode_segments:
        try:
            for output in format_output(decode_segment):
                converted_decode_segments.append(json.loads(output))
        except FormatDataError as e:
            process_logger.error(e)
            process_logger.error(traceback.format_exc())
            return False
        except Exception as e:
            process_logger.error(e)
            process_logger.error(traceback.format_exc())
            return False
            
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
                        process_logger.error("Error when creating index")
                        process_logger.error(index_put_response.status_code)
                        process_logger.error(index_put_response.json())
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
            process_logger.error("Error while uploading in bulk")
            process_logger.error(reply.json())
            #process_logger.logger.error(data_to_post)
            return False
    return True


def init_logging(name, queue):
    log_listener = MultiProcessQueueLoggingListner(name, queue)
    log_listener.start()
    main_logger = MultiProcessQueueLogger(name, queue)
    return log_listener, main_logger



def populate_index_list(elastic_server, main_logger):
    sensor_list = Manager().list()
    get_all_sensors_url = f"http://{elastic_server}:9200/*"
    try:
        get_all_sensors_response = request("GET", get_all_sensors_url)
        if not get_all_sensors_response.status_code == 200:
            main_logger.logger.error("Response status wasn't 200")
            main_logger.logger.error(get_all_sensors_response.json())
            return False
    except Exception as e:
        main_logger.logger.error(e)
        return False
    for key in get_all_sensors_response.json():
        if not key.startswith('.'):
            sensor_list.append(key)
    return sensor_list




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
    log_queue = Queue()
    error_queue = Queue()
    data_queue = Queue()
    elastic_lock = Manager().Lock()
    connected = Value(c_bool,False)
    log_name = f"{args.sub}-{args.host}-grpc.log"
    log_listener, main_logger = init_logging(log_name, log_queue)
    if args.tls:
        if args.pem:
            try:
                with open(args.pem, "rb") as fp:
                    pem = fp.read()
                client = TLSDialInClient(args.host, args.port, data_queue, log_name, args.sub, args.username, args.password, connected, pem)
            except Exception as e:
                main_logger.logger.error(e)
                log_listener.queue.put(None)
                log_listener.join()
                exit(0)
        else:
            main_logger.logger.error("Need to have a pem file")
            log_listener.queue.put(None)
            log_listener.join()
            exit(0)            
    else:
        client = DialInClient(args.host, args.port, data_queue, log_name,  args.sub, args.username, args.password, connected)
    indexices = populate_index_list(args.elastic_server, main_logger)
    if indexices == False:
        log_listener.queue.put(None)
        log_listener.join()
        exit(0)
    client.start()
    batch_list = []
    while not client.isconnected() and client.is_alive():
        pass
    with Pool() as pool:
        while client.is_alive():
            try:
                data = data_queue.get(timeout=1)
                if not data == None:
                    batch_list.append(data)
                    #print(len(batch_list))
                    if len(batch_list) >= int(args.batch_size):
                        result = pool.apply_async(elasticsearch_upload, (batch_list, args, elastic_lock, indexices, log_name, ))
                        #print(result.get())
                        #if not result.get():
                        #    break
                        #elasticsearch_upload(batch_list, args, elastic_lock, indexices, log_name)
                        del batch_list
                        batch_list = []
            except Empty:
                if not len(batch_list) == 0:
                    main_logger.logger.info(f"Flushing data of length {len(batch_list)}, due to timeout, increase batch size by {len(batch_list)}")
                    result = pool.apply_async(elasticsearch_upload, (batch_list, args, elastic_lock, indexices, log_name, ))
                    #if not result.get():                                                                                                                                                                  
                    #    break
                    #elasticsearch_upload(batch_list, args, elastic_lock, indexices, log_name)
                    del batch_list
                    batch_list = []

                    

    client.join()
    log_listener.queue.put(None)
    log_listener.join()
    

    
if __name__ == '__main__':
    main()
