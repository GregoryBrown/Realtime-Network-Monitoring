# -*- coding: utf-8 -*-
"""
Create Metrics for the response via Open Day Light(ODL) and add to the TSDB
"""

from argparse import ArgumentParser
from utils import create_measurement_name, json_tree_traversal, make_line_format
from databases import InfluxDBUploader
from time import time
from time import sleep
import json
import logging
import re
import requests


def main():
    parser = ArgumentParser()
    parser.add_argument("-t", "--database_server", dest="db_server", help="InfluxDB server name", required=True)
    parser.add_argument("-r", "--database_port", dest="db_port", help="InfluxDB server port", required=True)
    parser.add_argument("-d", "--database", dest="database", help="InfluxDB Database to use", required=True)
    parser.add_argument("-n", "--name", dest="device", help="Device name in ODL", required=True)
    parser.add_argument("-s", "--server", dest="server", help="ODL Server", required=True)
    parser.add_argument("-p", "--port", dest="port", help="ODL Server Port", required=True)
    parser.add_argument("-y", "--yang", dest="yang", help="Yang path to poll", required=True)
    parser.add_argument("-i", "--interval", dest="interval", help="Set a timer interval in seconds, defaults to 0 sec")
    parser.add_argument("-v", "--verbose", action="store_true", help="Turn on debug logs")
    args = parser.parse_args()    
    logger = logging.getLogger("ODL-Collector")
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - ""%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if args.interval:
        interval = float(args.interval)
    else:
        interval = 0.0
    url = "http://{}:{}/restconf/operational/network-topology:network-topology/topology/topology-netconf/node/{}/yang-ext:mount/{}".format(args.server, args.port, args.device, args.yang)
    headers = {
        'authorization': "Basic YWRtaW46YWRtaW4=",
        'cache-control': "no-cache",
        'postman-token': "2c5b0e7c-9106-193f-67b3-f3f8aadacea0"
    }
    measurement_name = create_measurement_name(args.yang)
    influxdb = InfluxDBUploader(args.db_server, int(args.db_port), args.database)
    while True:
        start_time = time()
        logger.info("Getting data from ODL controller")
        response = requests.request("GET", url, headers=headers)
        status_code = response.status_code
        response_context = response.content.decode("utf-8")
        if not (status_code == requests.codes.ok) and not (status_code == requests.codes.no_content):
            logger.error(f"Error getting data from ODL controller {status_code}")
            logger.error(response_context)
            exit(1)
        logger.info("Got data from ODL controller")
        reply = json.loads(response_context)
        logger.debug(response)
        logger.info("Getting all leafs from the data")
        post_data = []
        for raw_metric in json_tree_traversal(reply):
            tags = {}
            fields = {}
            field_null = True
            for key in raw_metric.keys():
                tmp = str(raw_metric[key]).replace(',', '')
                tags[key] = re.sub('\s+', '_', tmp)
                if isinstance(raw_metric[key], int) or isinstance(raw_metric[key], float):
                    field_null = False
                    fields[key] = float(raw_metric[key])
            if field_null:
                fields['FIELDS_NULL'] = 1.0
            else:
                fields['FIELDS_NULL'] = 0.0
            post_data.append(make_line_format(measurement_name, tags, fields))
        logger.info("Got all leaves from the data")
        logger.info("Sending all metrics via POST to the database")
        for chunks in [post_data[i:i + 2000] for i in range(0, len(post_data), 2000)]:
            upload_result = influxdb.upload_to_db('\n'.join(chunks))
            if not upload_result[0]:
                logger.error("Error while posting data to database")
                logger.error(upload_result)
                exit(1)
        logger.info("Finished sending metrics to the database")
        end_time = time()
        total_time = end_time - start_time
        if not total_time > interval:
            sleep(interval-total_time)


if __name__ == '__main__':
    main()
