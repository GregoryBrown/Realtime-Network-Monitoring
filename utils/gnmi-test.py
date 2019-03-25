import sys
import grpc
import logging
import re
sys.path.append("../")

from py_protos.ems_grpc_pb2_grpc import gRPCConfigOperStub
from py_protos.ems_grpc_pb2 import CreateSubsArgs
from py_protos.gnmi_pb2_grpc import gNMIStub
from py_protos.gnmi_pb2 import SubscribeRequest, SubscriptionList, Subscription, PathElem, Path
from google.protobuf import json_format


def create_gnmi_path(path):
    path_elements = []
    if path[0] == '/':
        if path[-1] == '/':
            path_list = re.split(r'''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)[1:-1]
        else:
            path_list = re.split(r'''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)[1:]
    else:
        if path[-1] == '/':
            path_list = re.split(r'''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)[:-1]
        else:
            path_list = re.split(r'''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)

    for elem in path_list:
        elem_name = elem.split("[", 1)[0]
        elem_keys = re.findall(r'\[(.*?)\]', elem)
        dict_keys = dict(x.split('=', 1) for x in elem_keys)
        path_elements.append(PathElem(name=elem_name, key=dict_keys))
    return Path(elem=path_elements)

class DialInClient(object):
    def __init__(self, host, port, user, password, path, sample):
        self._path = path
        self._sample = sample
        self._host = host
        self._port = port
        self._timeout = float(1000000)
        self._channel = None
        self._gnmi_stub = None
        self._metadata = [('username', user), ('password', password)]

    @staticmethod
    def sub_to_path(sub):
        yield sub

    def subscribe(self):
        path = create_gnmi_path(self._path)
        print(path)
        sample = int(self._sample) * 1000000000
        self._gnmi_stub = gNMIStub(self._channel)
        sub = Subscription(path=path, mode=2, sample_interval=sample)
        sub_list = SubscriptionList(subscription=[sub], mode=0, encoding=2)
        sub_request = SubscribeRequest(subscribe=sub_list)
        req_iterator = self.sub_to_path(sub_request)
        for response in self._gnmi_stub.Subscribe(req_iterator, metadata=self._metadata):
            print(response)
            json_format.MessageToJson(response)
            
    def connect(self):
        self._channel = grpc.insecure_channel(':'.join([self._host, self._port]))
        try:
            grpc.channel_ready_future(self._channel).result(timeout=10)
        except grpc.FutureTimeoutError as e:
            print(f"Can't connect to {self._host}:{self._port}")

            
def main():
    client = DialInClient('52.52.52.1', '57400', 'root', 'lablab', 'openconfig-interfaces:interfaces/interface', 5)
    client.connect()
    client.subscribe()



if __name__ == '__main__':
    main()
