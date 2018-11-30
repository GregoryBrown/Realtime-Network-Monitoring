import sys
sys.path.append("../")

from py_protos.ems_grpc_pb2_grpc import gRPCConfigOperStub
from py_protos.ems_grpc_pb2 import CreateSubsArgs
from utils.exceptions import DeviceFailedToConnect
from multiprocessing import Process, Queue
from py_protos.telemetry_pb2 import Telemetry
from google.protobuf import json_format
import grpc

class DialInClient(Process):
    def __init__(self, host, port, data_queue, sub_args, timeout=100000000, user='root', password='lablab', name='DialInClient'):
        super().__init__(name=name)
        self._host = host
        self.name = name
        self._port = port
        self._timeout = float(timeout)
        self._channel = None
        self._cisco_ems_stub = None
        self._connected = False
        self._metadata = [('username', user), ('password', password)]
        self.queue = data_queue
        self.sub_id = sub_args
        
    def subscribe(self):
        sub_args = CreateSubsArgs(ReqId=1, encode=3, subidstr=self.sub_id)
        stream = self._cisco_ems_stub.CreateSubs(sub_args, timeout=self._timeout, metadata=self._metadata)
        for segment in stream:
            self.queue.put_nowait(segment.data)

        
    def connect(self):
        self._channel = grpc.insecure_channel(':'.join([self._host,self._port]))
        try:
            grpc.channel_ready_future(self._channel).result(timeout=10)
            self._connected = True
        except grpc.FutureTimeoutError as e:
            raise DeviceFailedToConnect from e
        else:
            self._cisco_ems_stub = gRPCConfigOperStub(self._channel)


    def isconnected(self):
        return self._connected

    def run(self):
        self.connect()
        if self.isconnected():
            self.subscribe()

        
class TLSDialInClient(DialInClient):
    def __init__(self, host, port, data_queue, sub_args, pem, timeout=100000000, user='root', password='lablab', name='DialInClient'):
        self._pem = pem
        super().__init__(host, port, data_queue, sub_args, timeout, user, password)

    def connect(self):
        creds = grpc.ssl_channel_credentials(open(self._pem, "rb").read())
        opts = (('grpc.ssl_target_name_override', 'ems.cisco.com',),)
        self._channel = grpc.secure_channel(':'.join([self._host,self._port]), creds, opts)
        try:
            grpc.channel_ready_future(self._channel).result(timeout=10)
            self._connected = True
        except grpc.FutureTimeoutError:
            raise DeviceFailedToConnect
        else:
            self._cisco_ems_stub = gRPCConfigOperStub(self._channel)
    
