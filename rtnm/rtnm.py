import os
from argparse import ArgumentParser
from utils.configurationparser import ConfigurationParser
from utils.utils import init_dial_in_logs
from connectors.DialInClients import DialInClient, TLSDialInClient
from connectors.CiscoTCPDialOut import TelemetryTCPDialOutServer
from multiprocessing import Process, current_process, Pool, Manager, Queue, Value, Lock
from queue import Empty
from ctypes import c_bool
from tornado.ioloop import IOLoop
from tornado.netutil import bind_sockets
from tornado.process import fork_processes, task_id
from databases.databases import  ElasticSearchUploader


def start_dial_out(address, port, elasticsearch_server, elasticsearch_port, batch_size):
    sockets = bind_sockets(port)
    fork_processes(0)
    tcp_server = TelemetryTCPDialOutServer(elasticsearch_server, elasticsearch_port, batch_size)
    tcp_server.add_sockets(sockets)
    tcp_server.log.info(f"Starting listener on {address}:{port}")
    IOLoop.current().start()


def start_cisco_dial_in_sub(address, port, username, password, elasticsearch_server, elasticsearch_port, batch_size, sub, index_list, lock, pem=None):
    log_name = f"{current_process().name}.log"
    path = f"{os.path.dirname(os.path.realpath(__file__))}/logs"
    data_queue = Queue()
    connected = Value(c_bool, False)
    worker_pool = Pool()
    sub_log, es_log, conn_log, worker_log = init_dial_in_logs(log_name, path)
    es = ElasticSearchUploader(elasticsearch_server, elasticsearch_port, batch_size, lock, es_log, index_list, False)
    with lock:
        index_list = es.populate_index_list()
        if index_list == False:
            sub_log.error(f"Failed to get index list from {elasticsearch_server}:{elasticsearch_port}")

    if pem:
        with open(pem, "rb") as fp:
            pem = fp.read()
        client = TLSDialInClient(address, str(port), data_queue, conn_log, sub, username,
                                 password, connected, pem, name=f"conn-{current_process().name}")
    else:
        client = DialInClient(address, str(port), data_queue, conn_log, sub, username, password,
                              connected, name=f"conn-{current_process().name}")
    client.start()
    batch_list = []
    while not client.isconnected() and client.is_alive():
        pass
    print(client.is_alive())
    with worker_pool as pool:
        conn_log.info("Connected")
        while client.isconnected():
            try:
                data = data_queue.get(timeout=1)
                if data is not None:
                    batch_list.append(data)
                    if len(batch_list) >= int(batch_size):
                        es_log.info(f"Uploading full batch size")
                        result = pool.apply_async(es.elasticsearch_upload, (batch_list,))
                        #es.elasticsearch_upload(batch_list)
                        del batch_list
                        batch_list = []
            except Empty:
                if not len(batch_list) == 0:
                    es_log.info(f"Uploading data of length {len(batch_list)}")
                    result = pool.apply_async(es.elasticsearch_upload, (batch_list,))
                    #es.elasticsearch_upload(batch_list)
                    del batch_list
                    batch_list = []
            except Exception as e:
                sub_log.error(e)
                print(e)
            
                


def main():
    parser = ArgumentParser()
    parser.add_argument("-s", "--subscription", dest="sub", help="Subscription name used for non gNMI dial-in")
    parser.add_argument("-u", "--username", dest="username", help="Username")
    parser.add_argument("-p", "--password", dest="password", help="Password")
    parser.add_argument("-a", "--host", dest="host", help="Host IP")
    parser.add_argument("-r", "--port", dest="port", help="gRPC Port")
    parser.add_argument("-b", "--batch_size", dest="batch_size", help="Batch size to upload to ElasticSearch")
    parser.add_argument("-e", "--elastic_server", dest="elastic_server", help="ElasticSearch server IP")
    parser.add_argument("-t", "--tls", dest="tls", help="Flag to enable TLS", action='store_true')
    parser.add_argument("-m", "--pem", dest="pem", help="Pem file used with TLS")
    parser.add_argument("-g", "--gnmi", dest="gnmi", help="Flag to enable gNMI", action='store_true')
    parser.add_argument("-l", "--sample", dest="sample", help="Sample poll time for gNMI (ns)")
    parser.add_argument("-i", "--path", dest="path", help="Path for gNMI to subscribe to")
    parser.add_argument("-c", "--config", dest="config", help="Location of the configuration file")
    args = parser.parse_args()
    processes = []
    if args.config:
        config = ConfigurationParser(args.config)
        if config.parse_config():
            clients = config.clients
        else:
            parser.error("Can't parse the config file")
    else:
        clients = parser_cli_options(args) #TODO
    dial_out = clients["cisco-dial-out"]
    if not dial_out["Address"] == "None":
        processes.append(Process(target=start_dial_out, args=(dial_out["Address"], dial_out["Port"],
                                                              dial_out["DatabaseIP"][0], dial_out["ES-Port"], dial_out["BatchSize"], ), name="DialOutProcess"))
    '''
    elastic_lock = Manager().Lock()
    index_list = Manager().list()
    for client in clients:
        if not client == "cisco-dial-out":
            tls = False
            if 'TLS' in clients[client]:
                tls = True
            for sub in clients[client]["Subscriptions"]:
                if tls:
                    processes.append(Process(target=start_cisco_dial_in_sub, args=(clients[client]["Address"], clients[client]["Port"], clients[client]["Username"],
                                                                                   clients[client]["Password"], clients[client]["DatabaseIP"][0], clients[client]["ES-Port"],
                                                                                   clients[client]["BatchSize"], sub, index_list,
                                                                                   elastic_lock, clients[client]["Pem"], ), name=f"{client}-sub-{sub}"))
                else:
                    processes.append(Process(target=start_cisco_dial_in_sub, args=(clients[client]["Address"], clients[client]["Port"], clients[client]["Username"],
                                                                                   clients[client]["Password"], clients[client]["DatabaseIP"][0], clients[client]["ES-Port"],
                                                                                   clients[client]["BatchSize"], sub, index_list,
                                                                                   elastic_lock, ), name=f"{client}-sub-{sub}"))
    ''' 
    for process in processes:
        process.start()
    for process in processes:
        process.join()


if __name__ == '__main__':
    main()

    
