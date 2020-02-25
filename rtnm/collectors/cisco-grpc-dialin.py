import sys
import json
import logging
import traceback

sys.path.append("../")
from utils.connectors import DialInClient, TLSDialInClient
from argparse import ArgumentParser
from multiprocessing import Pool, Manager, Queue, Value
from queue import Empty
from requests import request
from utils.utils import (
    create_gnmi_path,
    init_logging,
    populate_index_list,
    process_batch_list,
    get_host_node,
)
from utils.configurationparser import ConfigurationParser
from ctypes import c_bool


def elasticsearch_upload(batch_list, args, lock, log_name):
    sorted_by_index = {}
    process_logger = logging.getLogger(log_name)
    try:
        converted_decode_segments = process_batch_list(batch_list, args)
    except Exception as e:
        process_logger.error(e)
        process_logger.error(traceback.format_exc())
        return False

    # Sort all segments by index
    for converted_decode_segment in converted_decode_segments:
        if not converted_decode_segment["_index"] in sorted_by_index.keys():
            sorted_by_index[converted_decode_segment["_index"]] = [
                converted_decode_segment
            ]
        else:
            sorted_by_index[converted_decode_segment["_index"]].append(
                converted_decode_segment
            )
    # Bulk upload each index to elasticsearch
    for index in sorted_by_index.keys():
        index_url = f"http://{args.elastic_server}:9200/{index}"
        index_list = populate_index_list(args.elastic_server, process_logger)
        if index_list == False:
            process_logger.error("Unable to populate index list")
            return False
        if index not in index_list:
            with lock:

                index_list = populate_index_list(args.elastic_server, process_logger)
                if index_list == False:
                    process_logger.error("Unable to repopulate index list")
                    return False

                if index not in index_list:
                    process_logger.info("Acciqured lock to put index in elasticsearch")
                    headers = {"Content-Type": "application/json"}
                    mapping = {
                        "settings": {"index.mapping.total_fields.limit": 2000},
                        "mappings": {
                            "nodes": {
                                "properties": {
                                    "type": {"type": "keyword"},
                                    "keys": {"type": "object"},
                                    "content": {"type": "object"},
                                    "timestamp": {"type": "date"},
                                }
                            }
                        },
                    }
                    index_put_response = request(
                        "PUT", index_url, headers=headers, json=mapping
                    )
                    if not index_put_response.status_code == 200:
                        process_logger.error("Error when creating index")
                        process_logger.error(index_put_response.status_code)
                        process_logger.error(index_put_response.json())
                        return False
                    index_list.append(index)
        data_url = f"http://{args.elastic_server}:9200/_bulk"
        segment_list = sorted_by_index[index]
        elastic_index = {"index": {"_index": f"{index}", "_type": "nodes"}}
        payload_list = [elastic_index]
        for segment in segment_list:
            segment.pop("_index", None)
            payload_list.append(segment)
            payload_list.append(elastic_index)
        payload_list.pop()
        data_to_post = "\n".join(json.dumps(d) for d in payload_list)
        data_to_post += "\n"
        headers = {"Content-Type": "application/x-ndjson"}
        reply = request("POST", data_url, data=data_to_post, headers=headers)
        if reply.json()["errors"]:
            process_logger.error("Error while uploading in bulk")
            process_logger.error(reply.json())
            # process_logger.logger.error(data_to_post)
            return False
    return True


def main():
    parser = ArgumentParser()
    parser.add_argument("-s", "--subscription", dest="sub", help="Subscription name")
    parser.add_argument("-u", "--username", dest="username", help="Username")
    parser.add_argument("-p", "--password", dest="password", help="Password")
    parser.add_argument("-a", "--host", dest="host", help="host")
    parser.add_argument("-r", "--port", dest="port", help="port")
    parser.add_argument("-b", "--batch_size", dest="batch_size", help="Batch size")
    parser.add_argument(
        "-e", "--elastic_server", dest="elastic_server", help="Elastic Server"
    )
    parser.add_argument(
        "-t", "--tls", dest="tls", help="TLS enabled", action="store_true"
    )
    parser.add_argument("-m", "--pem", dest="pem", help="pem file")
    parser.add_argument(
        "-g", "--gnmi", dest="gnmi", help="gnmi encoding", action="store_true"
    )
    parser.add_argument("-l", "--sample", dest="sample", help="sample poll time")
    parser.add_argument("-i", "--path", dest="path", help="path for gnmi support")
    parser.add_argument(
        "-c", "--config", dest="config", help="Name of the configuration file"
    )
    args = parser.parse_args()
    if args.config == None:
        parser.error("Need to supply a config file")
        exit(1)
    # create config class object
    config_parser = ConfigurationParser(args.config)
    config_parser.parse_arguments_and_config()
    clients = config_parser.client_configurations
    log_queue = Queue()
    data_queue = Manager().Queue()
    elastic_lock = Manager().Lock()
    connected = Value(c_bool, False)
    log_listener, main_logger = init_logging(log_name, log_queue)

    indices = populate_index_list(args.elastic_server, main_logger)
    if indices is False:
        log_listener.queue.put(None)
        log_listener.join()
        exit(0)

    client.start()
    batch_list = []
    while not client.isconnected() and client.is_alive():
        pass
    if not client.isconnected():
        client.join()
        log_listener.queue.put(None)
        log_listener.join()
        exit(1)
    if args.gnmi:
        node = get_host_node(args)
        if node is None:
            main_logger.logger.error("Can't get node name")
            log_listener.queue.put(None)
            log_listener.join()
            exit(1)
        args.node = node
    with Pool() as pool:
        while client.isconnected():
            try:
                data = data_queue.get(timeout=1)
                if data is not None:
                    batch_list.append(data)
                    if len(batch_list) >= int(args.batch_size):
                        result = pool.apply_async(
                            elasticsearch_upload,
                            (batch_list, args, elastic_lock, log_name,),
                        )
                        # print(result.get())
                        # if not result.get():
                        #    break
                        # elasticsearch_upload(batch_list, args, elastic_lock, log_name)
                        del batch_list
                        batch_list = []
            except Empty:
                if not len(batch_list) == 0:
                    main_logger.logger.info(
                        f"Flushing data of length {len(batch_list)}, due to timeout, increase batch size "
                        f"by {len(batch_list)}"
                    )
                    result = pool.apply_async(
                        elasticsearch_upload,
                        (batch_list, args, elastic_lock, log_name,),
                    )
                    # if not result.get():
                    #    break
                    # elasticsearch_upload(batch_list, args, elastic_lock, log_name)
                    del batch_list
                    batch_list = []

    client.join()
    log_listener.queue.put(None)
    log_listener.join()


if __name__ == "__main__":
    main()
