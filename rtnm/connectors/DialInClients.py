import grpc
import logging
import json

from protos.cisco_mdt_dial_in_pb2_grpc import gRPCConfigOperStub
from protos.cisco_mdt_dial_in_pb2 import CreateSubsArgs, ConfigGetArgs
from protos.gnmi_pb2_grpc import gNMIStub
from protos.gnmi_pb2 import (
    Subscription,
    SubscriptionList,
    SubscribeRequest,
    SubscribeResponse,
    GetRequest,
    GetResponse,
)
from multiprocessing import Process
from google.protobuf import json_format
from utils.utils import create_gnmi_path
import traceback


class DialInClient(Process):
    def __init__(
        self,
        connected,
        data_queue,
        log_name,
        options=[("grpc.ssl_target_name_override", "ems.cisco.com")],
        timeout=10000000,
        *args,
        **kwargs,
    ):
        super().__init__(name=kwargs["name"])
        self._host = kwargs["address"]
        self._port = kwargs["port"]
        self.queue = data_queue
        self.log = logging.getLogger(log_name)
        self._metadata = [
            ("username", kwargs["username"]),
            ("password", kwargs["password"]),
        ]
        self._connected = connected
        self._format = kwargs["format"]
        self.encoding = kwargs["encoding"]
        if self._format == "gnmi":
            self.sub_mode = kwargs["sub-mode"]
            self.sensors = kwargs["sensors"]
            self.sample_interval = int(kwargs["sample-interval"]) * 1000000000
            self.stream_mode = kwargs["stream-mode"]
        else:
            self.subs = kwargs["subs"]
        self.options = options
        self._timeout = float(timeout)
        self.channel = None
        self.gnmi_stub = None
        self.cisco_ems_stub = None

    def get_host_node(self):
        self.gnmi_stub = gNMIStub(self.channel)
        path = "Cisco-IOS-XR-shellutil-cfg:host-names"
        request = GetRequest(path=[create_gnmi_path(path)], encoding=4, type=2)
        response = self.gnmi_stub.Get(request, metadata=self._metadata)
        for notification in response.notification:
            for update in notification.update:
                return json.loads(update.val.json_ietf_val)["host-name"]

    @staticmethod
    def sub_to_path(request):
        yield request

    def gnmi_subscribe(self):
        try:
            node = self.get_host_node()
            self.gnmi_stub = gNMIStub(self.channel)
            subs = []
            for path in self.sensors:
                subs.append(
                    Subscription(
                        path=create_gnmi_path(path),
                        mode=self.sub_mode,
                        sample_interval=self.sample_interval,
                    )
                )
            sub_list = SubscriptionList(
                subscription=subs, mode=self.stream_mode, encoding=2
            )
            sub_request = SubscribeRequest(subscribe=sub_list)
            req_iterator = self.sub_to_path(sub_request)
            for response in self.gnmi_stub.Subscribe(
                req_iterator, metadata=self._metadata
            ):
                if response.error.message:
                    self.log.error("Response had error")
                    self.log.error(response)
                    self.log.error(response.error.message)
                    self.log.error(response.error.code)
                    self._connected.value = False
                    exit(1)
                elif response.sync_response:
                    self.log.info("Got all values atleast once")
                else:
                    self.queue.put_nowait((response.SerializeToString(), node))
        except Exception as e:
            self.log.error("Error in Conneciton gNMI")
            self.log.error(e)
            self.log.error(traceback.print_exc())
            self._connected.value = False
            exit(1)

    def ems_subscribe(self):
        try:
            self.cisco_ems_stub = gRPCConfigOperStub(self.channel)
            sub_args = CreateSubsArgs(ReqId=1, encode=3, Subscriptions=self.subs)
            stream = self.cisco_ems_stub.CreateSubs(
                sub_args, timeout=self._timeout, metadata=self._metadata
            )
            for segment in stream:
                if segment.errors:
                    self.log.error(segment.errors)
                    self._connected.value = False
                    exit(1)
                else:
                    self.queue.put_nowait(segment.data)
        except Exception as e:
            self.log.error(e)
            self._connected.value = False
            exit(1)

    def connect(self):
        self.channel = grpc.insecure_channel(":".join([self._host, self._port]))
        try:
            grpc.channel_ready_future(self.channel).result(timeout=10)
            self._connected.value = True
            self.log.info("Connected")
        except grpc.FutureTimeoutError as e:
            self.log.error(f"Can't connect to {self._host}:{self._port}")
            self.log.error(e)
            self.queue.put_nowait(None)
            self._connected.value = False

    def is_connected(self):
        return self._connected.value

    def run(self):
        self.connect()
        if self.is_connected():
            if self._format == "gnmi":
                self.gnmi_subscribe()
            else:
                self.ems_subscribe()


class TLSDialInClient(DialInClient):
    def __init__(self, pem, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._pem = pem

    def connect(self):
        credentials = grpc.ssl_channel_credentials(self._pem)
        self.channel = grpc.secure_channel(
            ":".join([self._host, self._port]), credentials, self.options
        )
        try:
            grpc.channel_ready_future(self.channel).result(timeout=10)
            self.log.info("Connected")
            self._connected.value = True
        except grpc.FutureTimeoutError as e:
            self.log.error(f"Can't connect to {self._host}:{self._port}")
            self.log.error(e)
            self._connected.value = False
