"""
.. module:: rtnm
   :platform: Unix, Windows
   :synopsis: Driver file for RTNM
.. moduleauthor:: Greg Brown <gsb5067@gmail.com>
"""
from argparse import ArgumentParser
from pathlib import Path
from typing import List, Dict, Union, Tuple, Optional
from multiprocessing import Pool, Queue
from queue import Empty
from logging import getLogger, Logger
from datetime import datetime
from parsers.Parsers import RTNMParser, ParsedResponse
from loggers.loggers import init_logs
from databases.databases import InfluxdbUploader, ElasticSearchUploader, Influxdb2Uploader
from errors.errors import ConfigError
from connectors.DialInClients import DialInClient, TLSDialInClient
from connectors.DialOutClients import DialOutClient
from utils.utils import generate_clients


def process_and_upload_data(*args):
    """Process the raw responses from gRPC/gNMI client and upload to a TSDB

    :param batch_list: The raw responses from either Cisco gRPC or gNMI clients or from both.
    :type batch_list: List[Tuple[str, str, Optional[str], Optional[str]]]
    :param log_name: Name of the logger used in RTNM to acquire
    :type log_name: str
    :param tsdb_args: The arguments of the TSDB (username, port, password, etc.)
    :type tsdb_args: Dict[str, str]

    """
    try:
        tsdb_args = args[-1]
        log_name = args[-2]
        batch_list = list(args[:-2])
        processor_log: Logger = getLogger(log_name)
        parser = RTNMParser(batch_list, log_name)
        start = datetime.now()
        parsed_responses: List[ParsedResponse] = parser.decode_and_parse_raw_responses()
        for tsdb_endpoint in tsdb_args.keys():
            tsdb_args[tsdb_endpoint]["log_name"] = log_name
            if tsdb_args[tsdb_endpoint]["type"] == "elasticsearch":
                uploader = ElasticSearchUploader(**tsdb_args[tsdb_endpoint])
            elif tsdb_args[tsdb_endpoint]["type"] == "influxdb":
                uploader = InfluxdbUploader(**tsdb_args[tsdb_endpoint])
            else:
                uploader = Influxdb2Uploader(**tsdb_args[tsdb_endpoint])
            uploader.upload(parsed_responses)
        end = datetime.now()
        total_time = end - start
        processor_log.info(f"Total Batch time took {total_time}")
    except Exception as error:
        processor_log.error(error)
        processor_log.debug(args)


def main():
    """RTNM main function used for getting the users arguements and spawns processes for each
    connection and handles dispatching of responses into a worker pool for processing
    and uploading of the data

    """
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", dest="config", help="Location of the configuration file", required=True)
    parser.add_argument("-b", "--batch-size", dest="batch_size", type=int,
                        help="Batch size of the upload to ElasticSearch", required=True)
    parser.add_argument("-w", "--worker-pool-size", dest="worker_pool_size", type=int,
                        help="Number of workers in the worker pool used for uploading")
    parser.add_argument("-v", "--verbose", dest="debug", help="Enable debugging", action="store_true")
    parser.add_argument("-r", "--retry", dest="retry", help="Enable retrying", action="store_true")
    args = parser.parse_args()
    try:
        if Path(args.config).is_file():
            inputs, outputs = generate_clients(args.config)
        else:
            raise IOError(f"File {args.config} doesn't exist")
    except ConfigError as error:
        parser.error(f"{error}")
    except KeyError as error:
        parser.error(f"Error in the configuration file: No key for {error}.\nCan't parse the config file")
    except Exception as error:
        parser.error(f"{error}")

    path: Path = Path().absolute() / "logs"
    log_queue: Queue = Queue()
    log_name: str = f"rtnm-{args.config.strip('ini').strip('.')}"
    rtnm_log = init_logs(log_name, path, log_queue, args.debug)
    try:
        client_conns: List[Union[DialInClient, TLSDialInClient]] = []
        data_queue: Queue = Queue()
        rtnm_log.logger.info("Starting inputs and outputs")
        for client in inputs:
            if inputs[client]["dial"] == "in":
                inputs[client]["debug"] = args.debug
                inputs[client]["retry"] = args.retry
                if "pem-file" in inputs[client]:
                    with open(inputs[client]["pem-file"], "rb") as file_desc:
                        pem = file_desc.read()
                    rtnm_log.logger.info(f"Creating TLS Connector for {client}")
                    client_conns.append(TLSDialInClient(pem, data_queue,
                                                        log_name, **inputs[client],
                                                        name=client))
                else:
                    rtnm_log.logger.info(f"Creating Connector for {client}")
                    client_conns.append(DialInClient(data_queue,
                                                     log_name, **inputs[client], name=client))
            else:
                client_conns.append(DialOutClient(data_queue, log_name, inputs[client], client))
        for client in client_conns:
            client.start()
        batch_list: List[Tuple[str, str, Optional[str], Optional[str], str]] = []
        with Pool(processes=args.worker_pool_size) as worker_pool:
            while all([client.is_alive() for client in client_conns]):
                try:
                    data: Tuple[str, str, Optional[str], Optional[str], str] = data_queue.get(timeout=10)
                    if data is not None:
                        batch_list.append(data)
                        if len(batch_list) >= args.batch_size:
                            rtnm_log.logger.debug("Uploading full batch size")
                            rtnm_log.logger.debug(batch_list)
                            result = worker_pool.apply_async(process_and_upload_data,
                                                             args=[*batch_list, log_name, outputs])
                            batch_list.clear()
                except Empty:
                    if len(batch_list) != 0:
                        rtnm_log.logger.debug(f"Uploading data of length {len(batch_list)}")
                        result = worker_pool.apply_async(process_and_upload_data,
                                                         args=[*batch_list, log_name, outputs])
                        batch_list.clear()
                except Exception as error:
                    rtnm_log.logger.error(error)
                    rtnm_log.logger.error("Error during worker pool, going to cleanup")
                    for client in client_conns:
                        client.terminate()
    except Exception as error:
        rtnm_log.logger.error(error)
    except KeyboardInterrupt as error:
        rtnm_log.logger.error("Shutting down due to user ctrl-c")
    finally:
        rtnm_log.logger.info("In cleanup")
        rtnm_log.queue.put(None)
        for client in client_conns:
            client.terminate()


if __name__ == "__main__":
    main()
