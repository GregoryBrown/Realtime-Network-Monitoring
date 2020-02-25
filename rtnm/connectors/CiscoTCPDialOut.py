"""
.. module:: CiscoTCPDialOut
   :platform: Unix, Windows
   :synopsis: A TCP server for listening to streams of telemetry data from a Cisco device
.. moduleauthor:: Greg Brown <gsb5067@gmail.com>
"""
import json
import logging
from struct import Struct
from typing import List, Dict, Tuple, Any

from errors.errors import TelemetryTCPDialOutServerError
from converters.converters import DataConverter
from tornado.httpclient import AsyncHTTPClient, HTTPError, HTTPRequest, HTTPResponse
# from tornado.iostream import StreamClosedError
from tornado.locks import Lock
from tornado.tcpserver import TCPServer
from tornado.iostream import IOStream


class TelemetryTCPDialOutServer(TCPServer):
    """Create a TCP dial out server to listen for telemetry data from a Cisco device

    :param address: The IP address to bind to
    :type address: str
    :param port: The port to listen on
    :type port: str
    :param batch_size: The number of messages to gather before uploading
    :type batch_size: int
    :param log_name: Used for getting the application log
    :type log_name: str

    """

    def __init__(self, address: str, port: str, batch_size: int, log_name: str) -> None:
        try:
            super().__init__()
            self.batch_size: int = batch_size
            self.url: str = f"http://{address}:{port}"
            self.lock: Lock = Lock()
            self.http_client = AsyncHTTPClient(max_clients=1000)
            self.log: logging.Logger = logging.getLogger(log_name)
            self.index_list: List[str] = []
        except Exception as e:
            raise TelemetryTCPDialOutServerError(f"Error while initializing dial out server:\n {e}")

    async def get_index_list(self) -> List[str]:
        """
        :return: The indices from an Elastic Search used to check if we need to put a new index

        """
        try:
            index_list: List[str] = []
            request: HTTPRequest = HTTPRequest(url=f"{self.url}/*", connect_timeout=40.0, request_timeout=40.0)
            response: HTTPResponse = await self.http_client.fetch(request)
            response: Dict[str, Any] = json.loads(response.body.decode())
            for key in response:
                if not key.startswith("."):
                    index_list.append(key)
            return index_list
        except Exception as e:
            raise TelemetryTCPDialOutServerError(f"Error while getting index:\n {e}")

    async def post_data(self, data_to_post: str) -> bool:
        """

        :param data_to_post: Json data to post into the Elastic Search instance
        :type data_to_post: str
        :return: True if successfully uploaded data, else False
        """
        try:
            headers: Dict[str, str] = {"Content-Type": "application/x-ndjson"}
            request: HTTPRequest = HTTPRequest(
                url=f"{self.url}/_bulk",
                method="POST",
                headers=headers,
                body=data_to_post,
                connect_timeout=40.0,
                request_timeout=40.0,
            )
            response: HTTPResponse = await self.http_client.fetch(request=request)
            if not response.error:
                return True
        except HTTPError as e:
            if e.code == 400 and e.message == "Bad Request":
                raise TelemetryTCPDialOutServerError(f"HTTP Error while posting data due to bad request:\n {e}")
            elif e.code == 599:
                return False
            else:
                raise TelemetryTCPDialOutServerError(f"HTTP Error while posting data:\n {e}")
        except Exception as e:
            raise TelemetryTCPDialOutServerError(f"Error while posting data:\n {e}")

    async def put_index(self, index: str) -> bool:
        """

        :param index: The name of the index you want to create in Elastic Search
        :type index: str
        :return: True if successfully uploaded data, else False
        """
        try:
            self.log.info(f"Putting {index} into Elastic Search")
            headers: Dict[str, str] = {"Content-Type": "application/json"}
            mapping: str = '{"settings": {"number_of_shards": 1, "number_of_replicas": 2},"mappings": {"properties": ' \
                           '{"@timestamp": {"type": "date"}}}} '
            request: HTTPRequest = HTTPRequest(
                url=f"{self.url}/{index}",
                method="PUT",
                headers=headers,
                body=mapping,
                connect_timeout=40.0,
                request_timeout=40.0,
            )
            response: HTTPResponse = await self.http_client.fetch(request)
            if not response.error:
                return True
        except HTTPError as e:
            if e.code == 400 and e.message == "Bad Request":
                raise TelemetryTCPDialOutServerError(f"HTTP Error while putting index due to bad request:\n {e}")
            elif e.code == 599:
                return False
            else:
                raise TelemetryTCPDialOutServerError(f"HTTP Error while putting index:\n {e}")
        except Exception as e:
            raise TelemetryTCPDialOutServerError(f"Error while putting index:\n {e}")

    async def handle_stream(self, stream: IOStream, address: Tuple[str, str]) -> None:
        """

        :param stream: Client IOStream to read telemetry data from
        :type stream: IOStream
        :param address: The IP address and port on which a client connects to the server
        :type address: Tuple[str,str]
        :return: None
        """
        self.index_list: List[str] = await self.get_index_list()
        _HEADER_SIZE: int = 12
        header_struct: Struct = Struct(">hhhhi")
        _UNPACK_HEADER = header_struct.unpack
        self.log.info(f"Got Connection from {address[0]}:{address[1]}")
        while not stream.closed():
            batch_list: List[bytes] = []
            while len(batch_list) < self.batch_size:
                header_data: bytes = await stream.read_bytes(_HEADER_SIZE)
                (msg_type, encode_type, msg_version, flags, msg_length,) = _UNPACK_HEADER(header_data)
                # encoding = {1: "gpb", 2: "json"}[encode_type]
                # implement json encoding
                msg_data: bytes = b""
                while len(msg_data) < msg_length:
                    packet: bytes = await stream.read_bytes(msg_length - len(msg_data))
                    msg_data += packet
                batch_list.append(msg_data)
            sorted_by_index: Dict[str, List[Dict[str, Any]]] = {}
            data_converter: DataConverter = DataConverter(batch_list, self.log, "cisco-ems")
            converted_decode_segments: List[Dict[str, Any]] = data_converter.process_batch_list()
            if converted_decode_segments is None:
                raise TelemetryTCPDialOutServerError("Error decoding the telemetry data")
            else:
                for converted_decode_segment in converted_decode_segments:
                    if not converted_decode_segment["_index"] in sorted_by_index.keys():
                        sorted_by_index[converted_decode_segment["_index"]] = [converted_decode_segment]
                    else:
                        sorted_by_index[converted_decode_segment["_index"]].append(converted_decode_segment)
                for index in sorted_by_index.keys():
                    if index not in self.index_list:
                        async with self.lock:
                            self.log.info("Acquired lock to put index in Elastic Search")
                            put_rc = False
                            while not put_rc:
                                self.index_list = await self.get_index_list()
                                if index not in self.index_list:
                                    put_rc = await self.put_index(index)
                                else:
                                    put_rc = True
                            self.index_list.append(index)
                    segment_list: List[Dict[str, Any]] = sorted_by_index[index]
                    elastic_index: Dict[str, Any] = {"index": {"_index": f"{index}"}}
                    payload_list: List[Dict[str, Any]] = [elastic_index]
                    for segment in segment_list:
                        segment.pop("_index", None)
                        payload_list.append(segment)
                        payload_list.append(elastic_index)
                    payload_list.pop()
                    data_to_post: str = "\n".join(json.dumps(d) for d in payload_list)
                    post_rc = False
                    while not post_rc:
                        post_rc = await self.post_data(f"{data_to_post}\n")
