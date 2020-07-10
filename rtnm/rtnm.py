from argparse import ArgumentParser
from pathlib import Path
from typing import List, Dict, Union, Tuple, Union, Optional
from multiprocessing import Pool, Queue, Value
from queue import Empty
from ctypes import c_bool
from logging import getLogger, Logger

from parsers.ElasticSearchParser import ElasticSearchParser
from loggers.loggers import init_logs
from databases.databases import ElasticSearchUploader
from errors.errors import IODefinedError, ElasticSearchUploaderError, DecodeError
from connectors.DialInClients import DialInClient, TLSDialInClient
from utils.utils import generate_clients


def process_and_upload_data(batch_list: List[Tuple[str, str, Optional[str], Optional[str]]],
                            log_name: str, output: Dict[str, str]):
    processor_log: Logger = getLogger(log_name)
    processor_log.debug("Creating Uploader and parser")
    es_uploader: ElasticSearchUploader = ElasticSearchUploader(output["address"],
                                                               output["port"], processor_log)
    es_parser: ElasticSearchParser = ElasticSearchParser(batch_list, log_name)
    try:
        for response_gen in es_parser.decode_and_parse_raw_responses():
            es_uploader.upload(response_gen)
    except ElasticSearchUploaderError as error:
        processor_log.error(error)
    except Exception as error:
        processor_log.error(error)


def cleanup(log):
    log.queue.put(None)
    log.join()


def main():
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", dest="config", help="Location of the configuration file", required=True)
    parser.add_argument("-b", "--batch-size", dest="batch_size",
                        help="Batch size of the upload to ElasticSearch", required=True)
    parser.add_argument("-w", "--worker-pool-size", dest="worker_pool_size", type=int,
                        help="Number of workers in the worker pool used for uploading")
    parser.add_argument("-v", "--verbose", dest="debug", help="Enable debugging", action="store_true")
    parser.add_argument("-r", "--retry", dest="retry", help="Enable retrying", action="store_true")
    args = parser.parse_args()
    try:
        inputs, outputs = generate_clients(args.config)
    except IODefinedError:
        parser.error("Need to define both an input and output in the configuraiton")
    except KeyError as error:
        parser.error(f"Error in the configuration file: No key for {error}.\nCan't parse the config file")
    except Exception as error:
        parser.error(f"Error {error}")
    output: Dict[str, str] = outputs[next(iter(outputs))]
    path: Path = Path().absolute() / "logs"
    log_queue: Queue = Queue()
    log_name: str = f"rtnm"
    log_listener, rtnm_log = init_logs(log_name, path, log_queue, args.debug)
    try:
        rtnm_log.logger.info("Creating worker pool")
        with Pool(processes=args.worker_pool_size) as worker_pool:
            client_conns: List[Union[DialInClient, TLSDialInClient]] = []
            data_queue: Queue = Queue()
            batch_list: List[Tuple[str, str, Optional[str], Optional[str]]] = []
            rtnm_log.logger.info("Starting inputs and outputs")
            for client in inputs:
                if inputs[client]["io"] == "out":
                    raise NotImplementedError("Dial Out is not implemented")
                else:
                    inputs[client]["debug"] = args.debug
                    inputs[client]["retry"] = args.retry
                    if "pem-file" in inputs[client]:
                        with open(inputs[client]["pem-file"], "rb") as file_desc:
                            pem = file_desc.read()
                        rtnm_log.logger.info(f"Creating TLS Connector for {client}")
                        client_conns.append(TLSDialInClient(pem,
                                                            Value(c_bool, False), data_queue,
                                                            log_name, **inputs[client],
                                                            name=client))
                    else:
                        rtnm_log.logger.info(f"Creating Connector for {client}")
                        client_conns.append(DialInClient(Value(c_bool, False), data_queue,
                                                         log_name, **inputs[client], name=client))

            for client in client_conns:
                rtnm_log.logger.info(f"Starting dial in client [{client.name}]")
                client.start()

            while all([client.is_alive() for client in client_conns]):
                try:
                    data: Tuple[str, str, Optional[str], Optional[str]] = data_queue.get(timeout=1)
                    # rtnm_log.logger.debug(data)
                    if data is not None:
                        batch_list.append(data)
                        if len(batch_list) >= int(args.batch_size):
                            rtnm_log.logger.debug("Uploading full batch size")
                            worker_pool.apply(process_and_upload_data,
                                              (batch_list, log_name, output))
                            batch_list.clear()
                except Empty:
                    if len(batch_list) != 0:
                        rtnm_log.logger.debug(f"Uploading data of length {len(batch_list)}")
                        worker_pool.apply(process_and_upload_data,
                                          (batch_list, log_name, output))
                        batch_list.clear()
            worker_pool.close()
    except NotImplementedError as error:
        rtnm_log.logger.error(error)
    except Exception as error:
        rtnm_log.logger.error(error)
    finally:
        cleanup(log_listener)
        for client in client_conns:
            client.kill()


if __name__ == "__main__":
    main()
