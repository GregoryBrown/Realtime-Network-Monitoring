# -*- coding: utf-8 -*-
"""
Create Metrics for the response via Open Day Light(ODL)
and add to prometheus to monitor per process Memory
"""

import logging
import re
from argparse import ArgumentParser
from copy import deepcopy
import requests
import json
from urllib.parse import unquote
from time import time
from time import sleep

def traverse_tree(tree):
    metric_list = sub_traverse_tree(tree)
    return metric_list

def sub_traverse_tree(root):
    metric = {}
    metrics = []
    for key in list(root):
        if not isinstance(root[key], dict) and not isinstance(root[key], list):
            metric[key] = root[key]
            root.pop(key, None)
    for key in list(root):
        if isinstance(root[key], dict):
            subdict_traverse_tree(root[key],metric, metrics)
        if isinstance(root[key], list):
            for subnode in root[key]:
                subdict_traverse_tree(subnode,metric, metrics)
    return metrics

def subdict_traverse_tree(root, metric, metrics):
    leaf = False
    for key in list(root):
        if not isinstance(root[key], dict) and not isinstance(root[key], list):
            metric[key] = root[key]
            root.pop(key, None)
            leaf = True
    for key in list(root):
        if isinstance(root[key], dict):
            leaf = False
            subdict_traverse_tree(root[key],metric, metrics)
        if isinstance(root[key], list):
            leaf = False
            for subnode in root[key]:
                subdict_traverse_tree(subnode,metric, metrics)
    if leaf:
        metrics.append(deepcopy(metric))
    return metric


def create_measurement_name(yang):
    url = unquote(yang)
    url = url.replace('/','-')
    url = url.replace(':', '-')
    url = url.strip('-')
    return url
                        
def make_line_format(measurement, tags, fields):
    rc_str = ''
    rc_str = rc_str + measurement
    for tag_key in tags.keys():
        rc_str += ','
        rc_str +='{}=\"{}\"'.format(tag_key, tags[tag_key])
    rc_str += ' '
    for field_key in fields:
        rc_str +='{}={},'.format(field_key, fields[field_key])
    return rc_str.strip(',')



def send_http_request(url, method, headers=None, data=None, params=None):
    if headers:
        headers.update({'Connection':'close'})
    else:
        headers={'Connection':'close'}
    response = requests.request(method, url, headers=headers, data=data, params=params)
    status_code = response.status_code
    response = response.content.decode("utf-8") 
    if not (status_code == requests.codes.ok or status_code == requests.codes.no_content):
        return (False, response, status_code)
    return (True, response, status_code)

    
def main():
    parser = ArgumentParser()
    parser.add_argument("-t", "--t", dest="db_server", help="InfluxDB server name", required=True)
    parser.add_argument("-d", "--database", dest="database", help="InfluxDB Database to use", required=True)
    parser.add_argument("-n", "--name", dest="device", help="Device name in ODL", required=True)
    parser.add_argument("-s", "--server", dest="server", help="ODL Server", required=True)
    parser.add_argument("-p", "--port", dest="port", help="ODL Server Port", required=True)
    parser.add_argument("-y", "--yang", dest="yang", help="Yang path to poll", required=True)
    parser.add_argument("-i", "--interval", dest="interval", help="Set a timer interval in seconds on how long to wait between collecting data, defaults to 0 second")
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
    write_url = "http://{}:8086/write".format(args.db_server)
    request_params = {"db" : args.database}
    while True:
        start_time = time()
        logger.info("Getting data from ODL controller")
        response = send_http_request( url, "GET", headers)
        if response[0]:
            logger.info("Got data from ODL controller")
            reply = json.loads(response[1])
            logger.debug(response)
            logger.info("Getting all leafs from the data")
        else:
            logger.error("Error getting data from ODL controller")
            logger.error(response[1])
            logger.error(response[2])
            exit(1)
        post_data = []
        for raw_metric in traverse_tree(reply):
            tags = {}
            fields = {}
            for key in raw_metric.keys():
                tmp  = str(raw_metric[key]).replace(',', '')
                tags[key] = re.sub('\s+', '_', tmp)
                if isinstance(raw_metric[key], int) or isinstance(raw_metric[key], float):
                    fields[key] = float(raw_metric[key])
            post_data.append(make_line_format(measurement_name, tags, fields))
        logger.info("Got all leaves from the data")
        logger.info("Sending all metrics via POST to the database")
        if len(post_data) > 5000:
            for chunks in [post_data[i:i + 2000] for i in range(0, len(post_data), 2000)]:
                reponse = send_http_request(write_url, "POST", data ='\n'.join(chunks), params=request_params)
        else:
            reponse = send_http_request(write_url, "POST", data ='\n'.join(post_data), params=request_params)
        if response[0]:
            logger.info("Finished sending metrics to the database")
            logger.debug(response)
        else:
            logger.error("Error while posting data to database")
            logger.error(response[2])
            exit(1)                                                     
        end_time = time()
        total_time = end_time - start_time
        if not total_time > interval:
            sleep(interval-total_time)

if __name__ == '__main__':
    main()
