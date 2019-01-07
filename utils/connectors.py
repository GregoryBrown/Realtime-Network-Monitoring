import sys
import grpc
import logging

sys.path.append("../")

from py_protos.ems_grpc_pb2_grpc import gRPCConfigOperStub
from py_protos.ems_grpc_pb2 import CreateSubsArgs
from py_protos.gnmi_pb2_grpc import gNMIStub
from py_protos.gnmi_pb2 import SubscribeRequest, SubscriptionList, Subscription
from multiprocessing import Process
from google.protobuf import json_format


class DialInClient(Process):
    def __init__(self, host, port, data_queue, log_name, sub_args, user, password, connected, timeout=100000000,
                 name='DialInClient', gnmi=False, path=None, sample=None):
        super().__init__(name=name)
        self._gnmi = gnmi
        self._path = path
        self._sample = sample
        self._host = host
        self.name = name
        self._port = port
        self._timeout = float(timeout)
        self._channel = None
        self.log = logging.getLogger(log_name)
        self._cisco_ems_stub = None
        self._gnmi_stub = None
        self._connected = connected
        self._metadata = [('username', user), ('password', password)]
        self.queue = data_queue
        self.sub_id = sub_args

    @staticmethod
    def sub_to_path(sub):
        yield sub

    def subscribe(self):
        try:
            if self._gnmi:
                self._gnmi_stub = gNMIStub(self._channel)
                sub = Subscription(path=self._path, mode=2, sample_interval=self._sample)
                sub_list = SubscriptionList(subscription=[sub], mode=0, encoding=2)
                sub_request = SubscribeRequest(subscribe=sub_list)
                req_iterator = self.sub_to_path(sub_request)
                got_sync = False
                for response in self._gnmi_stub.Subscribe(req_iterator, metadata=self._metadata):
                    if got_sync:
                        self.queue.put_nowait(json_format.MessageToJson(response))
                    if response.sync_response:
                        got_sync = True
            else:
                self._cisco_ems_stub = gRPCConfigOperStub(self._channel)
                sub_args = CreateSubsArgs(ReqId=1, encode=3, subidstr=self.sub_id)
                stream = self._cisco_ems_stub.CreateSubs(sub_args, timeout=self._timeout, metadata=self._metadata)
                for segment in stream:
                    if segment.errors:
                        self.log.error(segment.errors)
                        self.queue.put_nowait(None)
                        self._connected.value = False
                    else:
                        self.queue.put_nowait(segment.data)
        except Exception as e:
            self.log.error(e)
            self.queue.put_nowait(None)
            self._connected.value = False

    def connect(self):
        self._channel = grpc.insecure_channel(':'.join([self._host, self._port]))
        try:
            grpc.channel_ready_future(self._channel).result(timeout=10)
            self._connected.value = True
        except grpc.FutureTimeoutError as e:
            self.log.error(f"Can't connect to {self._host}:{self._port}")
            self.log.error(e)
            exit(0)

    def isconnected(self):
        return self._connected.value

    def run(self):
        self.connect()
        if self.isconnected():
            self.subscribe()


class TLSDialInClient(DialInClient):
    def __init__(self, host, port, data_queue, log_name, sub_args, user, password, connected, pem, timeout=100000000,
                 name='DialInClient', gnmi=False, path=None, sample=None):
        self._pem = pem
        super().__init__(host, port, data_queue, log_name, sub_args, user, password, connected, timeout, name, gnmi,
                         path, sample)

    def connect(self):
        creds = grpc.ssl_channel_credentials(self._pem)
        opts = (('grpc.ssl_target_name_override', 'ems.cisco.com',),)
        self._channel = grpc.secure_channel(':'.join([self._host, self._port]), creds, opts)
        try:
            grpc.channel_ready_future(self._channel).result(timeout=10)
            self._connected.value = True
        except grpc.FutureTimeoutError as e:
            self.log.error(f"Can't connect to {self._host}:{self._port}")
            self.log.error(e)
            exit(0)
