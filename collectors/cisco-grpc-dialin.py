from mdt_grpc_dialin_pb2_grpc import gRPCConfigOperStub
from mdt_grpc_dialin_pb2 import CreateSubsArgs
from google.protobuf import json_format
from telemetry_pb2 import Telemetry
from collections import defaultdict
from argparse import ArgumentParser
from requests import request
from time import time
from multiprocessing import Pool, Manager
import grpc
import json
import logging
from collections import defaultdict
from grpc import ChannelConnectivity




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
            telemetry_pb = Telemetry()
            telemetry_pb.ParseFromString(segment.data)
            yield json_format.MessageToJson(telemetry_pb)

        
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


def elasticsearch_upload(data, args, lock, sensor_list):
    #Create index (indexes are sensor names
    for output in format_output(data):
        output = json.loads(output)
        sensor = output.pop("encode_path").replace('/','-').lower()
        device = output['node'].replace('/','-').lower()
        index_url = f"http://{args.elastic_server}:9200/{sensor}"
        headers = {'Content-Type': "application/json"}
        if not sensor in sensor_list:
            with lock:
                if not sensor in sensor_list:
                    print('Acciqured lock to put index in elasticsearch')
                    mapping = {"settings": {"index.mapping.total_fields.limit": 2000},"mappings": {"nodes": {
                        "properties": {"type": {"type": "keyword"},"keys": {"type": "object"},
                                       "content": {"type": "object"},"timestamp": {"type": "date"}}}}}
                    index_put_response = request("PUT", index_url, headers=headers, json=mapping)
                    if not index_put_response.status_code == 200:
                        print("Error when creating index")
                        print(index_put_response.status_code)
                        print(index_put_response.json())
                    sensor_list.append(sensor)
        data_url = f"{index_url}/nodes"
        output["type"] = device
        data_response = request("POST", data_url, json=output)
        if not data_response.status_code == 201:
            print(data_response.status_code)
            print(data_response.json())
            print(output)
            

def main():
    parser = ArgumentParser()
    parser.add_argument("-s", "--subscription", dest="sub", help="Subscription name", required=True)
    parser.add_argument("-u", "--username", dest="username", help="Username", required=True)
    parser.add_argument("-p", "--password", dest="password", help="Password", required=True)
    parser.add_argument("-a", "--host", dest="host", help="host", required=True)
    parser.add_argument("-r", "--port", dest="port", help="port", required=True)
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
        for response in client.subscribe(args.sub):
            result = pool.apply_async(elasticsearch_upload, (response, args, elastic_lock, sensor_list, ))
            #elasticsearch_upload(response, args, elastic_lock, sensor_list)
if __name__ == '__main__':
    main()
