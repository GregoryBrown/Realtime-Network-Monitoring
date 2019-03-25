import sys
import re
import json
import datetime
import grpc
sys.path.append("../")


from collections import defaultdict
from py_protos.gnmi_pb2 import PathElem, Path
from .multi_process_logging import MultiProcessQueueLoggingListner, MultiProcessQueueLogger
from multiprocessing import Manager
from requests import request
from py_protos.telemetry_pb2 import Telemetry
from google.protobuf import json_format


def init_logging(name, queue):
    log_listener = MultiProcessQueueLoggingListner(name, queue)
    log_listener.start()
    main_logger = MultiProcessQueueLogger(name, queue)
    return log_listener, main_logger


def populate_index_list(elastic_server, logger):
    indices = []
    get_all_sensors_url = f"http://{elastic_server}:9200/*"
    try:
        get_all_sensors_response = request("GET", get_all_sensors_url)
        if not get_all_sensors_response.status_code == 200:
            logger.error("Response status wasn't 200")
            logger.error(get_all_sensors_response.json())
            return False
    except Exception as e:
        logger.error(e)
        return False
    for key in get_all_sensors_response.json():
        if not key.startswith('.'):
            indices.append(key)
    return indices


def create_gnmi_path(path):
    path_elements = []
    if path[0] == '/':
        if path[-1] == '/':
            path_list = re.split(r'''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)[1:-1]
        else:
            path_list = re.split(r'''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)[1:]
    else:
        if path[-1] == '/':
            path_list = re.split(r'''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)[:-1]
        else:
            path_list = re.split(r'''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)

    for elem in path_list:
        elem_name = elem.split("[", 1)[0]
        elem_keys = re.findall(r'\[(.*?)\]', elem)
        dict_keys = dict(x.split('=', 1) for x in elem_keys)
        path_elements.append(PathElem(name=elem_name, key=dict_keys))
    return Path(elem=path_elements)
    

def process_batch_list(batch_list, args):
    if args.gnmi:
        return process_gnmi(batch_list, args.node)
    else:
        return process_cisco_encoding(batch_list)


def process_gnmi(batch_list, node):
    json_segments = []
    formatted_json_segments = []
    for batch in batch_list:
        json_segments.append(json.loads(json_format.MessageToJson(batch)))
    for segment in json_segments:
        header = segment["update"]
        timestamp = header["timestamp"]
        index, keys, encode_path = process_header(header["prefix"])
        content = parse_gnmi(header["update"])
        formatted_json_segments.append({'_index': index, 'keys': keys, 'content': content, 'encode_path': encode_path,
                                        'node': node, 'timestamp': int(timestamp)/1000000})

    return formatted_json_segments


def process_cisco_encoding(batch_list):
    json_segments = []
    formatted_json_segments = []
    for segment in batch_list:
        telemetry_pb = Telemetry()
        telemetry_pb.ParseFromString(segment)
        json_segments.append(json.loads(json_format.MessageToJson(telemetry_pb)))
    for segment in json_segments:
        formatted_json_segments.append(parse_cisco_encoding(segment))
    formatted_json_segments = [x for x in formatted_json_segments if x is not None]
    formatted_json_segments = [item for sublist in formatted_json_segments for item in sublist]
    return formatted_json_segments


def parse_cisco_encoding(telemetry_json):
    if "dataGpbkv" in telemetry_json:
        rc_list = []
        for data in telemetry_json["dataGpbkv"]:
            if "fields" in data:
                output = _parse_cisco_data(data["fields"])
                output["encode_path"] = telemetry_json["encodingPath"]
                output["node"] = telemetry_json["nodeIdStr"]
                output['timestamp'] = data["timestamp"]
                output['_index'] = telemetry_json["encodingPath"].replace('/', '-').lower() + '-' + get_date()
                rc_list.append(json.loads(json.dumps(output)))
        return rc_list


def _parse_cisco_data(data):
    data_dict = defaultdict(list)
    for item in data:
        if "fields"in item:
            data_dict[item["name"]].append(_parse_cisco_data(item["fields"]))
        else:
            for key, value in item.items():
                if 'Value' in key:
                    if 'uint' in key:
                        # Check if is an int, and if it is a BIG INTEGER make string so it can upload to ES
                        rc_value = int(value)
                        if rc_value > sys.maxsize:
                            rc_value = str(rc_value)
                    elif 'String' in key:
                        rc_value = str(value)
                    else:
                        rc_value = value 
                    data_dict[item["name"]] = rc_value
    return data_dict


def get_value(val_dict):
    for key, value in val_dict.items():
        if 'string' in key:
            return str(value)
        elif 'bool' in key:
            return bool(value)
        elif 'leaflistVal' in key:
            return get_value(value['element'][0])
        elif 'int' in key:
            return int(value)
        else:
            return value


def parse_gnmi(update):
    rc_dict = [{}]
    for path in update:
        current_level = rc_dict
        for index, elements in enumerate(path['path']['elem']):
            if 'key' in list(elements.keys()):
                key = list(elements['key'].keys())[0]
                value = list(elements['key'].values())[0]
                current_level[0][key] = value
            if elements['name'] in current_level[0]:
                current_level = current_level[0][elements['name']]
            else:
                if index == len(path['path']['elem'])-1:
                    current_level[0][elements['name']] = get_value(path['val'])
                else:
                    current_level[0][elements['name']] = [{}]
                    current_level = current_level[0][elements['name']]
    return rc_dict


def process_header(header):
    index = header["origin"].lower()
    keys = []
    elem_str_list = []
    for elem in header["elem"]:
        for key, value in elem.items():
            if key == "name":
                elem_str_list.append(elem[key])
            else:
                keys.append(elem[key])
    rc_keys = {}
    for elem_dict in keys:
        rc_keys.update(elem_dict)
    
    encode_path = header["origin"] + ":" + "/".join(elem_str_list)
    index = index + ":" + '-'.join(elem_str_list) + '-gnmi-' + get_date()
    return index, [rc_keys], encode_path


def get_date():
    now = datetime.datetime.now()
    month = f"{now.month:02d}"
    day = f"{now.day:02d}"
    return '.'.join([str(now.year), month, day])
    

def get_host_node(args):
    from py_protos.ems_grpc_pb2_grpc import gRPCConfigOperStub
    from py_protos.ems_grpc_pb2 import ConfigGetArgs
    target = f"{args.host}:{args.port}"
    if args.pem:
        opts = (('grpc.ssl_target_name_override', 'ems.cisco.com',),)
        creds = grpc.ssl_channel_credentials(open(args.pem, "rb").read())
        channel = grpc.secure_channel(target, creds, opts)
    else:
        channel = grpc.insecure_channel(target)
    metadata = [('username', args.username), ('password', args.password)]
    stub = gRPCConfigOperStub(channel)
    path = '{"Cisco-IOS-XR-shellutil-cfg:host-names": [null]}'
    message = ConfigGetArgs(yangpathjson=path)
    responses = stub.GetConfig(message, 10000, metadata=metadata)
    objects, err = '', ''
    for response in responses:
        objects += response.yangjson
        err += response.errors
    if err:
        return None
    return json.loads(objects)["Cisco-IOS-XR-shellutil-cfg:host-names"]["host-name"]



'''
    if args.sub and args.gnmi:
        parser.error("Only supply a subscription or gnmi path, not both")
    if args.sub is None and args.gnmi is False:
        parser.error("Need to supply gnmi flag or a subscription")
    if args.gnmi and (args.sample is None or args.path is None):
        parser.error("gnmi requires a sample time and a path")

    if args.tls and args.pem is None:
        parser.error("TLS requires a pem file")

    log_queue = Queue()
    data_queue = Queue()
    elastic_lock = Manager().Lock()
    connected = Value(c_bool, False)
    if args.gnmi:
        log_path = "-".join(args.path.split('/')[:6])
        log_path = log_path.replace('[', '-').replace(']', '-')
        log_name = f"{log_path}-{args.host.replace('[', '-').replace(']', '-')}-gnmi.log"
    else:
        log_name = f"{args.sub}-{args.host}-grpc.log"
    log_listener, main_logger = init_logging(log_name, log_queue)
    if args.tls:
        if args.pem:
            try:
                with open(args.pem, "rb") as fp:
                    pem = fp.read()
                if args.gnmi:
                    path = create_gnmi_path(args.path)
                    sample = int(args.sample) * 1000000000
                    client = TLSDialInClient(args.host, args.port, data_queue, log_name, args.sub, args.username,
                                             args.password, connected, pem, gnmi=True, path=path, sample=sample)
                else:



'''

'''
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
            sub_dict_traverse_tree(root[key], metric, metrics)
        if isinstance(root[key], list):
            for sub_node in root[key]:
                sub_dict_traverse_tree(sub_node, metric, metrics)
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
            sub_dict_traverse_tree(root[key], metric, metrics)
        if isinstance(root[key], list):
            leaf = False
            for sub_node in root[key]:
                sub_dict_traverse_tree(sub_node, metric, metrics)
    if leaf:
        metrics.append(deepcopy(metric))
    return metric


def create_measurement_name(yang):
    url = unquote(yang)
    url = url.replace('/', '-').replace(':', '-').strip('-')
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
    
    # print(output)
    # output = r.search(output).group(1)
    # output = json.loads(output)
    # output = output["data_json"]
    # print(output)
'''
