"""
.. module:: DialInClients
   :platform: Unix, Windows
   :synopsis: Class file of the gRPC connectors for dial in subscriptions
.. moduleauthor:: Greg Brown <gsb5067@gmail.com>
"""
import json
import random
import grpc
from multiprocessing import Process, Queue
from typing import List, Tuple
from time import sleep
from logging import Logger, getLogger
from protos.cisco_mdt_dial_in_pb2_grpc import gRPCConfigOperStub
from protos.cisco_mdt_dial_in_pb2 import CreateSubsArgs
from protos.gnmi_pb2_grpc import gNMIStub
from protos.gnmi_pb2 import (
    Encoding,
    GetRequest,
    GetResponse,
    Subscription,
    SubscriptionList,
    SubscribeRequest,
    TypedValue
)
from utils.utils import create_gnmi_path


class DialInClient(Process):
    """ Dial in class that represents a process that connects to the gRPC device

    :param data_queue: The queue used from transfer the raw string data from the connector process
    to the main process for upload
    :type data_queue: Queue
    :param log_name: The log name that will be used for logging
    :type log_name: str
    :param options:

    """

    def __init__(self, data_queue: Queue, log_name: str, options: List[Tuple[str, str]] = None, timeout: int = 100000000, *args, **kwargs):
        super().__init__(name=kwargs["name"])
        if options is None:
            opts: List[Tuple[str, str]] = [("grpc.ssl_target_name_override", "ems.cisco.com"), ("grpc.keepalive_time_ms", 60000), ("grpc.keepalive_timeout_ms", 10000)]
        self.options: List[Tuple[str, str]] = opts
        self._host: str = kwargs["address"]
        self._port: int = kwargs["port"]
        self.queue: Queue = data_queue
        self.log: Logger = getLogger(log_name)
        self._metadata: List[Tuple[str, str]] = [
            ("username", kwargs["username"]),
            ("password", kwargs["password"]),
        ]
        self._format: str = kwargs["format"]
        self.encoding: str = kwargs["encoding"]
        self.debug: bool = kwargs["debug"]
        self.retry: bool = kwargs["retry"]
        self.compression: bool = kwargs["compression"]
        if self._format == "gnmi":
            self.sub_mode = kwargs["subscription-mode"]
            self.sensors: List[str] = kwargs["sensors"]
            self.sample_interval: int = kwargs["sample-interval"]
            self.stream_mode: str = kwargs["stream-mode"]
        else:
            self.subs: List[str] = kwargs["subscriptions"]
        self._timeout: float = float(timeout)
        self.gnmi_stub: gNMIStub = None
        self.cisco_ems_stub: gRPCConfigOperStub = None
        self.log.debug("Finished initialzing %s", self.name)
        self.min_backoff_time: int = 1
        self.max_backoff_time: int = 128

    def _get_gnmi_stub(self) -> gNMIStub:
        self.gnmi_stub: gNMIStub = gNMIStub(self.channel)
        return self.gnmi_stub

    def _get_ems_stub(self) -> gRPCConfigOperStub:
        self.cisco_ems_stub: gRPCConfigOperStub = gRPCConfigOperStub(self.channel)
        return self.cisco_ems_stub

    def _get_version(self) -> str:
        stub: gNMIStub = self._get_gnmi_stub()
        get_message: GetRequest = GetRequest(
            path=[create_gnmi_path(
                'Cisco-IOS-XR-install-oper:install/version')],
            type=GetRequest.DataType.Value("STATE"),
            encoding=Encoding.Value("JSON_IETF"),
        )
        response: GetResponse = stub.Get(get_message, metadata=self._metadata, timeout=self._timeout)

        def _parse_version(version: GetResponse) -> str:
            rc = ""
            for notification in version.notification:
                for update in notification.update:
                    rc = json.loads(update.val.json_ietf_val)
                    return rc["label"]
        return _parse_version(response)

    def _get_hostname(self) -> str:
        stub: gNMIStub = self._get_gnmi_stub()
        get_message: GetRequest = GetRequest(
            path=[create_gnmi_path("Cisco-IOS-XR-shellutil-cfg:host-names")],
            type=GetRequest.DataType.Value("CONFIG"),
            encoding=Encoding.Value("JSON_IETF"),
        )
        response: GetResponse = stub.Get(get_message, metadata=self._metadata, timeout=self._timeout)

        def _parse_hostname(hostname_response: GetResponse) -> str:
            for notification in hostname_response.notification:
                for update in notification.update:
                    hostname: str = update.val.json_ietf_val
                    if not hostname:
                        return ""
                    return json.loads(hostname)["host-name"]

        return _parse_hostname(response)

    def _backoff(self) -> None:
        delay: float = self.min_backoff_time + random.randint(0, 1000) / 1000.0
        sleep(delay)
        if self.min_backoff_time < self.max_backoff_time:
            self.min_backoff_time *= 2

    def sub_to_path(self, request):
        yield request

    def gnmi_subscribe(self) -> None:
        """ Subscribe to a device via gNMI"""
        retry: bool = True
        subs: List[Subscription] = []
        hostname: str = ""
        version: str = ""
        while retry:
            try:
                self.connect()
                if not hostname:
                    hostname: str = self._get_hostname()
                if not version:
                    version: str = self._get_version()
                for sensor in self.sensors:
                    subs.append(
                        Subscription(path=create_gnmi_path(sensor), mode=self.sub_mode,
                                     sample_interval=self.sample_interval))
                sub_list: SubscriptionList = SubscriptionList(
                    subscription=subs, mode=self.stream_mode, encoding=self.encoding,
                )
                sub_request: SubscribeRequest = SubscribeRequest(subscribe=sub_list)
                stub: gNMIStub = self._get_gnmi_stub()
                for response in stub.Subscribe(self.sub_to_path(sub_request), metadata=self._metadata, timeout=self._timeout):
                    if response.error.message:
                        raise grpc.RpcError(response.error.message)
                    elif response.sync_response:
                        self.log.debug("Got all values atleast once")
                    else:
                        self.queue.put_nowait(("gnmi", response.SerializeToString(), hostname, version, self._host))
            except grpc.RpcError as error:
                self.log.error(error)
            except Exception as error:
                self.log.error(error)
            finally:
                self.disconnect()
                retry = self.retry
                if retry:
                    self._backoff()

    def ems_subscribe(self) -> None:
        retry: bool = True
        version: str = ""
        while retry:
            try:
                self.connect()
                if not version:
                    version: str = self._get_version()
                stub: gRPCConfigOperStub = self._get_ems_stub()
                sub_args: CreateSubsArgs = CreateSubsArgs(ReqId=1, encode=self.encoding,
                                                          Subscriptions=self.subs)
                for segment in stub.CreateSubs(sub_args, timeout=self._timeout,
                                               metadata=self._metadata):
                    if segment.errors:
                        raise grpc.RpcError(segment.errors)
                    else:
                        self.queue.put_nowait(("ems", segment.data, None, version, self._host))
            except grpc.RpcError as error:
                self.log.error(error)
                retry = self.retry
            except Exception as error:
                self.log.error(error)
            finally:
                self.disconnect()
                if retry:
                    self._backoff()

    def connect(self) -> None:
        if self.compression:
            self.channel = grpc.insecure_channel(":".join([self._host, self._port]), self.options,
                                                 compression=grpc.Compression.Gzip)
        else:
            self.channel = grpc.insecure_channel(":".join([self._host, self._port]), self.options)

    def disconnect(self) -> None:
        self.log.info(f"Closing channel for {self.name}")
        self.channel.close()

    def run(self):
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
        if self.compression:
            self.channel = grpc.secure_channel(
                ":".join([self._host, self._port]), credentials, self.options, compression=grpc.Compression.Gzip)
        else:
            self.channel = grpc.secure_channel(":".join([self._host, self._port]), credentials, self.options)
