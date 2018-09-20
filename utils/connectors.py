import sys
sys.path.append("../")

from py_protos.ems_grpc_pb2_grpc import gRPCConfigOperStub
from py_protos.ems_grpc_pb2 import CreateSubsArgs
import grpc

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
    
