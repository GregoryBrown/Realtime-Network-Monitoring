import sys
sys.path.append("../")

from utils.exceptions import FormatDataError
from urllib.parse import unquote
from copy import deepcopy
from collections import defaultdict
from py_protos.gnmi_pb2 import PathElem, Path
from .multi_process_logging import  MultiProcessQueueLoggingListner, MultiProcessQueueLogger
from multiprocessing import Manager
from requests import request
import re
import json





def init_logging(name, queue):
    log_listener = MultiProcessQueueLoggingListner(name, queue)
    log_listener.start()
    main_logger = MultiProcessQueueLogger(name, queue)
    return log_listener, main_logger



def populate_index_list(elastic_server, main_logger):
    sensor_list = Manager().list()
    get_all_sensors_url = f"http://{elastic_server}:9200/*"
    try:
        get_all_sensors_response = request("GET", get_all_sensors_url)
        if not get_all_sensors_response.status_code == 200:
            main_logger.logger.error("Response status wasn't 200")
            main_logger.logger.error(get_all_sensors_response.json())
            return False
    except Exception as e:
        main_logger.logger.error(e)
        return False
    for key in get_all_sensors_response.json():
        if not key.startswith('.'):
            sensor_list.append(key)
    return sensor_list


def create_gnmi_path(path):
    path_elements = []
    if path[0]=='/':
        if path[-1]=='/':
            path_list = re.split('''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)[1:-1]
        else:
            path_list =  re.split('''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)[1:]
    else:
        if path[-1]=='/':
            path_list =  re.split('''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)[:-1]
        else:
            path_list =  re.split('''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)

    for e in path_list:
        eName = e.split("[", 1)[0]
        eKeys = re.findall('\[(.*?)\]', e)
        dKeys = dict(x.split('=', 1) for x in eKeys)
        path_elements.append(PathElem(name=eName, key=dKeys))
    return Path(elem=path_elements)
    

def format_output(telemetry_jsonformat):
    telemetry_json = json.loads(telemetry_jsonformat)
    if "dataGpbkv" in telemetry_json:
        for data in telemetry_json["dataGpbkv"]:
            try:
                output = _format_fields(data["fields"])
                output["encode_path"] = telemetry_json["encodingPath"]
                output["node"] = telemetry_json["nodeIdStr"]
                output['timestamp'] = data["timestamp"]
                yield json.dumps(output)
            except Exception as e:
                msg = f"Error while trying to format data:\n {data}"
                raise FormatDataError(msg) from e
            
def _format_fields(data):
    data_dict = defaultdict(list)
    for item in data:
        if "fields"in item:
            data_dict[item["name"]].append(_format_fields(item["fields"]))
        else:
            rc_value = {'null':'null'}
            for key, value in item.items():
                if 'Value' in key:
                    if 'uint' in key:
                        #Check if is an int, and if it is a BIG INTEGER make string so it can upload to ES
                        rc_value = int(value)
                        if rc_value > sys.maxsize:
                            rc_value = str(rc_value)
                    elif 'String' in key:
                        rc_value = str(value)
                    else:
                        rc_value = value 
                    data_dict[item["name"]] = rc_value
    return data_dict


def json_tree_traversal(tree):
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
            sub_dict_traverse_tree(root[key],metric, metrics)
        if isinstance(root[key], list):
            for sub_node in root[key]:
                sub_dict_traverse_tree(sub_node,metric, metrics)
    return metrics


def sub_dict_traverse_tree(root, metric, metrics):
    leaf = False
    for key in list(root):
        if not isinstance(root[key], dict) and not isinstance(root[key], list):
            metric[key] = root[key]
            root.pop(key, None)
            leaf = True
    for key in list(root):
        if isinstance(root[key], dict):
            leaf = False
            sub_dict_traverse_tree(root[key],metric, metrics)
        if isinstance(root[key], list):
            leaf = False
            for sub_node in root[key]:
                sub_dict_traverse_tree(sub_node,metric, metrics)
    if leaf:
        metrics.append(deepcopy(metric))
    return metric


def create_measurement_name(yang):
    url = unquote(yang)
    url = url.replace('/','-').replace(':', '-').strip('-')
    return url


def make_line_format(measurement, tags, fields):
    rc_str = ''
    rc_str = rc_str + measurement
    for tag_key in tags.keys():
        rc_str += ','
        rc_str += '{}=\"{}\"'.format(tag_key, tags[tag_key])
    rc_str += ' '
    for field_key in fields:
        rc_str += '{}={},'.format(field_key, fields[field_key])
    return rc_str.strip(',')


def format_mdt_output(output):
    print(output)
    r = re.compile(r'({.*"collection_end_time":\d+})')
    for group in r.search(output).groups():
        print(group)
        print('\n\n\n\n')
    
    #print(output)
    #output = r.search(output).group(1)
    #output = json.loads(output)
    #output = output["data_json"]
    #print(output)





