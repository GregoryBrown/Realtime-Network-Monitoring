from mdt_grpc_dialin_pb2_grpc import gRPCConfigOperStub
from mdt_grpc_dialin_pb2 import CreateSubsArgs
from google.protobuf import json_format
from telemetry_pb2 import Telemetry
from gnmi_pb2_grpc import gNMIStub
from gnmi_pb2 import Subscription, SubscriptionMode, Path, SubscriptionList, Encoding, PathElem, SubscribeRequest, CapabilityRequest
import grpc


class DialInClient(object):
    def __init__(self, host, port, timeout=10000000, user='root', password='lablab'):
        self._host = host
        self._port = port
        self._timeout = float(timeout)
        self._metadata = [('username', user), ('password', password)]
        self._channel = grpc.insecure_channel(':'.join([self._host,self._port]))
        self._gnmi_stub = gNMIStub(self._channel)
        self._cisco_ems_stub = gRPCConfigOperStub(self._channel)
        
    def subscribe(self, sub_id, unmarshal=True):
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


    def subscribe_to_path(self, path):
        pathelem = [PathElem(name=path)]
        path = Path(elem=pathelem)
        subscription = Subscription(path=path, sample_interval=5000000000, mode="SAMPLE")
        sublist = SubscriptionList(subscription=[subscription], encoding=2)
        subreq = [SubscribeRequest(subscribe=sublist)]
        stream = self._gnmi_stub.Subscribe(subreq, metadata=self._metadata)
        for unit in stream:
            yield unit
            

    def getgnmicapability(self):
        message = CapabilityRequest()
        responses = self._gnmi_stub.Capabilities(message, metadata=self._metadata)
        return responses


def main():
    client = DialInClient('10.8.44.3', '57400')
    print(client.getgnmicapability())
    #for json_response in client.subscribe_to_path("Cisco-IOS-XR-ip-tcp-oper:tcp-nsr"):
    #    print(json_response)

if __name__ == '__main__':
    main()