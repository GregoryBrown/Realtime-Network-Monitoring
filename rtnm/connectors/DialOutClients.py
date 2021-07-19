"""
.. module:: DialOutClient
   :platform: Unix, Windows
   :synopsis: A TCP server for listening to streams of telemetry data from a Cisco device
.. moduleauthor:: Greg Brown <gsb5067@gmail.com>
"""
import json
import gzip
from logging import getLogger, Logger
from datetime import datetime
from struct import Struct
from typing import List, Dict, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, Future
from tornado.httpclient import AsyncHTTPClient, HTTPError, HTTPRequest, HTTPResponse
from tornado.tcpserver import TCPServer
from tornado.iostream import IOStream
from tornado.netutil import bind_sockets
from tornado.process import fork_processes
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from multiprocessing import Process, Queue



        
class DialOutClient(Process, TCPServer):
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

    def __init__(self, data_queue: Queue, log_name: str, inputs: Dict[str,str], name: str) -> None:
        Process.__init__(self, name=name)
        TCPServer.__init__(self, max_buffer_size=10485760000, read_chunk_size=104857600)
        self.address: str = inputs["address"]
        self.port: int = inputs["port"]
        self.log: Logger = getLogger(log_name)
        self.log.info("Starting dial out client[%s]", self.name)
        self.url: str = f"http://{self.address}:{self.port}"
        self._header_size: int = 12
        self._header_struct: Struct = Struct(">hhhhi")
        self.data_queue: Queue = data_queue
        
    async def handle_stream(self, stream: IOStream, address: Tuple[str, str]) -> None:
        """

        :param stream: Client IOStream to read telemetry data from
        :type stream: IOStream
        :param address: The IP address and port on which a client connects to the server
        :type address: Tuple[str,str]
        :return: None
        """

        try:
            self.log.info(f"Got Connection from {address[0]}:{address[1]}")
            while not stream.closed():
                header_data: bytes = await stream.read_bytes(self._header_size)
                (msg_type, encode_type, msg_version, flags, msg_length,) = self._header_struct.unpack(header_data)
                # encoding = {1: "gpb", 2: "json"}[encode_type]
                # implement json encoding
                msg_data: bytes = b""
                while len(msg_data) < msg_length:
                    packet: bytes = await stream.read_bytes(msg_length - len(msg_data))
                    msg_data += packet
                self.data_queue.put_nowait(("ems", msg_data, None, None, address[0]))
        except StreamClosedError as error:
            self.log.error(f'{address[0]}:{address[1]}  {error}')
            stream.close()


    def run(self):
        sockets = bind_sockets(self.port)
        fork_processes(0)
        self.log.info("Started dial out server listening on %s:%s", self.address, self.port)
        self.add_sockets(sockets)
        IOLoop.current().set_default_executor(ThreadPoolExecutor(10))
        IOLoop.current().start()
