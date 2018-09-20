import sys
sys.path.append("../")

from py_protos.ems_grpc_pb2_grpc import gRPCConfigOperStub
from py_protos.ems_grpc_pb2_grpc import CreateSubsArgs
from google.protobuf import json_format
from py_protos.telemetry_pb2 import Telemetry
from collections import defaultdict
from argparse import ArgumentParser
from requests import request
from time import time
from multiprocessing import Pool, Manager
from collections import defaultdict
from grpc import ChannelConnectivity
import grpc
import json
import logging

#test
class DialInClient(object):
    def __init__(self, host, port, timeout=10000000, user='root', password='lablab'):
        self._host = host
        self._port = port
        self._timeout = float(timeout)
        self._channel = None
        self._cisco_ems_stub = None
        self._connected = False
        self._metadata = [('username', user), ('password', password)]
        if self._host and self._port:
            self.connect()
            
    def subscribe(self, sub_id):
        sub_args = CreateSubsArgs(ReqId=1, encode=3, subidstr=sub_id)
        stream = self._cisco_ems_stub.CreateSubs(sub_args, timeout=self._timeout, metadata=self._metadata)
        for segment in stream:
            #telemetry_pb = Telemetry()
            #telemetry_pb.ParseFromString(segment.data)
            #yield json_format.MessageToJson(telemetry_pb)
            yield segment 
        
    def connect(self):
        self._channel = grpc.insecure_channel(':'.join([self._host,self._port]))
        try:
            grpc.channel_ready_future(self._channel).result(timeout=10)
            self._connected = True
        except grpc.FutureTimeoutError:
            print('Error connecting to server')
            exit(1)
        else:
            self._cisco_ems_stub = gRPCConfigOperStub(self._channel)

    def isconnected(self):
        return self._connected
        
class TLSDialInClient(DialInClient):
    def __init__(self, host, port, pem, timeout=10000000, user='root', password='lablab'):
        self._pem = pem
        super().__init__(host, port, timeout, user, password)
    def connect(self):
        creds = grpc.ssl_channel_credentials(open(self._pem, "rb").read())
        opts = (('grpc.ssl_target_name_override', 'ems.cisco.com',),)
        self._channel = grpc.secure_channel(':'.join([self._host,self._port]), creds, opts)
        try:
            grpc.channel_ready_future(self._channel).result(timeout=10)
            self._connected = True
        except grpc.FutureTimeoutError:
            print('Error connecting to server')
            exit(1)
        else:
            self._cisco_ems_stub = gRPCConfigOperStub(self._channel)
    
def format_output(telemetry_jsonformat):
    telemetry_json = json.loads(telemetry_jsonformat)
    if "dataGpbkv" in telemetry_json:
        for data in telemetry_json["dataGpbkv"]:
            output = _format_fields(data["fields"])
            output["encode_path"] = telemetry_json["encodingPath"]
            output["node"] = telemetry_json["nodeIdStr"]
            output['timestamp'] = data["timestamp"]
            yield json.dumps(output)

def _format_fields(data):
    data_dict = defaultdict(list)
    for item in data:
        if "fields"in item:
            data_dict[item["name"]].append(_format_fields(item["fields"]))
        else:
            rc_value = {'null':'null'}
            for key, value in item.items():
                if 'Value' in key:
                    rc_value = value
                    data_dict[item["name"]] = rc_value
    return data_dict



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
    if args.tls:
        if args.pem:
            client = TLSDialInClient(args.host, args.port, args.pem, user=args.username, password=args.password)
        else:
            print("Need pem file when using tls")
            exit(0)
    else:
        client = DialInClient(args.host, args.port, user=args.username, password=args.password)
    elastic_lock = Manager().Lock()
    sensor_list = Manager().list()
    get_all_sensors_url = f"http://{args.elastic_server}:9200/*"
    get_all_sensors_response = request("GET", get_all_sensors_url)
    if not get_all_sensors_response.status_code == 200:
        print("Error getting all devices to populate the device list")
        exit(0)
    for key in get_all_sensors_response.json():
        if not key.startswith('.'):
            sensor_list.append(key)
    with Pool() as pool:
        batch_list = []
        for response in client.subscribe(args.sub):
            batch_list.append(response)
            if len(batch_list) >= int(args.batch_size):
                result = pool.apply_async(elasticsearch_upload, (batch_list, args, elastic_lock, sensor_list, ))
                #elasticsearch_upload(batch_list, args, elastic_lock, sensor_list)
                del batch_list
                batch_list = []
        
        
if __name__ == '__main__':
    main()
