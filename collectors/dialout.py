from mdt_grpc_dialout_pb2_grpc import  gRPCMdtDialoutStub
from mdt_grpc_dialout_pb2 import MdtDialoutArgs
import grpc



def main():
    host = '0.0.0.0'
    port = '7777'
    user = 'root'
    password = 'lablab'
    metadata = [('username', user), ('password', password)]
    channel = grpc.insecure_channel(':'.join([host,port]))
    cisco_ems_stub = gRPCMdtDialoutStub(channel)
    for i in cisco_ems_stub.MdtDialout(MdtDialoutArgs()):
        print(i)


if __name__ == '__main__':
    main()
