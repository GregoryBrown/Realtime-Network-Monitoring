from mdt_grpc_dialin_pb2_grpc import gRPCConfigOperStub
from mdt_grpc_dialin_pb2 import CreateSubsArgs
from google.protobuf import json_format
from telemetry_pb2 import Telemetry
import grpc


class DialInClient(object):
    def __init__(self, host, port, timeout=10000000, user='root', password='lablab'):
        self._host = host
        self._port = port
        self._timeout = float(timeout)
        self._metadata = [('username', user), ('password', password)]
        self._channel = grpc.insecure_channel("[2001:10:8:44::3]:57400")
            #':'.join([self._host,self._port]))
        self._stub = gRPCConfigOperStub(self._channel)


    def getsubscription(self, sub_id, unmarshal=True):
        print('GET')
        sub_args = CreateSubsArgs(ReqId=1, encode=3, subidstr=sub_id)
        stream = self._stub.CreateSubs(sub_args, timeout=self._timeout, metadata=self._metadata)
        for segment in stream:
            if not unmarshal:
                yield segment
            else:
                # Go straight for telemetry data
                telemetry_pb = Telemetry()
                telemetry_pb.ParseFromString(segment.data)
                # Return in JSON format instead of protobuf.
                yield json_format.MessageToJson(telemetry_pb)





def main():
    client = DialInClient('10.8.44.3', '57400')
    for json_response in client.getsubscription('dial-in-test'):
        print(json_response)

if __name__ == '__main__':
    main()
