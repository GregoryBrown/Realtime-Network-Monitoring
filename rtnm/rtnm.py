import os
import logging
import datetime
from argparse import ArgumentParser
from utils.configurationparser import ConfigurationParser
from errors.errors import (
    IODefinedError,
    PutIndexError,
    PostDataError,
    GetIndexListError,
    DecodeError,
)
from connectors.DialInClients import DialInClient, TLSDialInClient
from connectors.CiscoTCPDialOut import TelemetryTCPDialOutServer
from multiprocessing import Process, current_process, Pool, Manager, Queue, Value
from queue import Empty
from ctypes import c_bool
from tornado.ioloop import IOLoop
from tornado.netutil import bind_sockets
from tornado.process import fork_processes, task_id
from databases.databases import ElasticSearchUploader
from loggers.loggers import init_logs, MultiProcessQueueLogger
from converters.converters import DataConverter


def start_dial_out(input_args, output, log_name):
    # dialout_log = logging.getLogger(log_name)
    sockets = bind_sockets(int(input_args["port"]), input_args["address"])
    fork_processes(0)
    # path = f"{os.path.dirname(os.path.realpath(__file__))}/logs"
    tcp_server = TelemetryTCPDialOutServer(output, input_args["batch-size"], log_name)
    tcp_server.add_sockets(sockets)
    tcp_server.log.info(
        f"Starting listener on {input_args['address']}:{input_args['port']}"
    )
    IOLoop.current().start()


def process_and_upload_data(batch_list, encoding, log_name, output, index_list, lock):
    processor_log = logging.getLogger(log_name)
    try:
        es = ElasticSearchUploader(
            output["address"], output["port"], index_list, lock, processor_log
        )
        converter = DataConverter(batch_list, processor_log, encoding)
        data_list = converter.process_batch_list()
        es.upload(data_list)
    except Exception as e:
        processor_log.error(e)


def set_pool_process_name(name):
    current_process().name = f"{name}-{current_process().name}"


def start_dial_in_sub(input_args, output, lock, index_list, log_name):
    sub_log = logging.getLogger(log_name)
    data_queue = Queue()
    connected = Value(c_bool, False)
    process_name = f"{current_process().name}"
    worker_pool = Pool(initializer=set_pool_process_name, initargs=(process_name,))
    if "pem-file" in input_args:
        with open(input_args["pem-file"], "rb") as fp:
            pem = fp.read()
        sub_log.info("Creating TLS Connector")
        client = TLSDialInClient(
            pem,
            connected,
            data_queue,
            log_name,
            **input_args,
            name=f"conn-{current_process().name}",
        )
    else:
        sub_log.info("Creating Connector")
        client = DialInClient(
            connected,
            data_queue,
            log_name,
            **input_args,
            name=f"conn-{current_process().name}",
        )
    client.start()
    batch_list = []
    while not client.is_connected() and client.is_alive():
        pass
    with worker_pool as pool:
        while client.is_connected():
            try:
                data = data_queue.get(timeout=1)
                if data is not None:
                    batch_list.append(data)
                    if len(batch_list) >= int(input_args["batch-size"]):
                        sub_log.info(f"Uploading full batch size")
                        pool.apply_async(
                            process_and_upload_data,
                            (
                                batch_list,
                                input_args["format"],
                                log_name,
                                output,
                                index_list,
                                lock,
                            ),
                        )
                        del batch_list
                        batch_list = []
            except Empty:
                if not len(batch_list) == 0:
                    sub_log.info(f"Uploading data of length {len(batch_list)}")
                    pool.apply_async(
                        process_and_upload_data,
                        (
                            batch_list,
                            input_args["format"],
                            log_name,
                            output,
                            index_list,
                            lock,
                        ),
                    )
                    del batch_list
                    batch_list = []
            except Exception as e:
                sub_log.error(e)
                exit(1)
        worker_pool.close()


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "-s",
        "--subscription",
        dest="sub",
        help="Subscription name used for non gNMI dial-in",
    )
    parser.add_argument("-u", "--username", dest="username", help="Username")
    parser.add_argument("-p", "--password", dest="password", help="Password")
    parser.add_argument("-a", "--host", dest="host", help="Host IP")
    parser.add_argument("-r", "--port", dest="port", help="gRPC Port")
    parser.add_argument(
        "-b",
        "--batch_size",
        dest="batch_size",
        help="Batch size to upload to ElasticSearch",
    )
    parser.add_argument(
        "-e", "--elastic_server", dest="elastic_server", help="ElasticSearch server IP"
    )
    parser.add_argument(
        "-t", "--tls", dest="tls", help="Flag to enable TLS", action="store_true"
    )
    parser.add_argument("-m", "--pem", dest="pem", help="Pem file used with TLS")
    parser.add_argument(
        "-g", "--gnmi", dest="gnmi", help="Flag to enable gNMI", action="store_true"
    )
    parser.add_argument(
        "-l", "--sample", dest="sample", help="Sample poll time for gNMI (ns)"
    )
    parser.add_argument(
        "-i", "--path", dest="path", help="Path for gNMI to subscribe to"
    )
    parser.add_argument(
        "-c", "--config", dest="config", help="Location of the configuration file"
    )
    parser.add_argument(
        "-v", "--verbose", dest="debug", help="Enable debugging", action="store_true"
    )
    args = parser.parse_args()
    processes = []
    if args.config:
        try:
            config = ConfigurationParser(args.config)
            inputs, outputs = config.generate_clients()
            output = outputs[next(iter(outputs))]
            path = f"{os.path.dirname(os.path.realpath(__file__))}/logs"
            log_queue = Queue()
            log_name = f"rtnm_{datetime.date.today()}_{datetime.datetime.now().time().strftime('%H:%M:%S')}"
            if args.debug:
                log_listener, rtnm_log = init_logs(log_name, f"{path}", log_queue, True)
            else:
                log_listener, rtnm_log = init_logs(log_name, f"{path}", log_queue)
            elastic_lock = Manager().Lock()
            index_list = Manager().list()
            rtnm_log.logger.info("Starting inputs and outputs")
            for client in inputs:
                inputs[client]["debug"] = args.debug
                if inputs[client]["io"] == "out":
                    rtnm_log.logger.info(f"Starting dial out client [{client}]")
                    processes.append(
                        Process(
                            target=start_dial_out,
                            args=(inputs[client], output, log_name,),
                            name=client,
                        )
                    )
                else:
                    rtnm_log.logger.info(f"Starting dial in client [{client}]")
                    processes.append(
                        Process(
                            target=start_dial_in_sub,
                            args=(
                                inputs[client],
                                output,
                                elastic_lock,
                                index_list,
                                log_name,
                            ),
                            name=client,
                        )
                    )
            for process in processes:
                process.start()
            for process in processes:
                process.join()
            log_listener.queue.put(None)
            log_listener.join()
        except IODefinedError:
            parser.error("Need to define both an input and output in the configuraiton")
        except KeyError as e:
            parser.error(
                f"Error in the configuration file: No key for {e}.\nCan't parse the config file"
            )
        except Exception as e:
            parser.error(f"Error {e}")
    else:
        print("Need to implement cli parsing")
        exit(0)


if __name__ == "__main__":
    main()
