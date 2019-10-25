from errors.errors import DecodeError
from protos.telemetry_pb2 import Telemetry
from google.protobuf import json_format
from collections import defaultdict
import json
import datetime
import sys


def get_date():
    now = datetime.datetime.now()
    month = f"{now.month:02d}"
    day = f"{now.day:02d}"
    return '.'.join([str(now.year), month, day])


class DataConverter(object):
    def __init__(self, batch_list, log, encoding):
        self.batch_list = batch_list
        self.log = log
        self.encoding = encoding

    def process_batch_list(self):
        if self.encoding == "gnmi":
            return self.process_gnmi()
        else:
            return self.process_cisco_encoding()

    def process_cisco_encoding(self):
        json_segments = []
        formatted_json_segments = []
        try:
            for segment in self.batch_list:
                telemetry_pb = Telemetry()
                telemetry_pb.ParseFromString(segment)
                json_segments.append(json.loads(json_format.MessageToJson(telemetry_pb)))
            for segment in json_segments:
                formatted_json_segments.append(self.parse_cisco_encoding(segment))
            formatted_json_segments = [x for x in formatted_json_segments if x is not None]
            formatted_json_segments = [item for sublist in formatted_json_segments
                                       for item in sublist]
            return formatted_json_segments
        except Exception as e:
            self.log.error(e)
        
    def parse_cisco_encoding(self, telemetry_json):
        try:
            if "dataGpbkv" in telemetry_json:
                rc_list = []
                for data in telemetry_json["dataGpbkv"]:
                    if "fields" in data:
                        output = self._parse_cisco_data(data["fields"])
                        output["encode_path"] = telemetry_json["encodingPath"]
                        output["host"] = telemetry_json["nodeIdStr"]
                        output['@timestamp'] = data["timestamp"]
                        output['_index'] = telemetry_json["encodingPath"].replace('/', '-').lower().replace(':', '-') + '-' + get_date()
                        rc_list.append(json.loads(json.dumps(output)))
                return rc_list
        except DecodeError as e:
            self.log.error(e)
        except Exception as e:
            self.log.error(e)

    def _parse_cisco_data(self, data):
        try:
            data_dict = defaultdict(list)
            for item in data:
                if "fields"in item:
                    data_dict[item["name"]].append(self._parse_cisco_data(item["fields"]))
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
        except Exception as e:
            self.log.error(e)
            self.log.error(item)

    '''
        def process_gnmi(self, batch_list):
        formatted_json_segments = []
        for segment in batch_list:
            header = segment["update"]
            timestamp = header["timestamp"]
            index, keys, encode_path = process_header(header["prefix"])
            content = parse_gnmi(header["update"])
            formatted_json_segments.append({'_index': index, 'keys': keys, 'content': content,
                                            'encode_path': encode_path, 'host': node,
                                            'timestamp': int(timestamp)/1000000})
        return formatted_json_segments
    def get_value(self, val_dict):
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


    def parse_gnmi(self, update):
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


    def process_header(self, header):
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
                        index = index + "/" + '-'.join(elem_str_list) + '-gnmi-' + get_date()
                        return index, [rc_keys], encode_path


    def get_date(self):
        now = datetime.datetime.now()
        month = f"{now.month:02d}"
        day = f"{now.day:02d}"
        return '.'.join([str(now.year), month, day])
        
        def convert_data(self):
            try:
                converted_decode_segments = process_batch_list(batch_list, self.encoding)
            except Exception as e:
                process_logger.error(e)
                process_logger.error(traceback.format_exc())
                return False

            # Sort all segments by index
            for converted_decode_segment in converted_decode_segments:
                if not converted_decode_segment["_index"] in sorted_by_index.keys():
                    sorted_by_index[converted_decode_segment["_index"]] = [converted_decode_segment]
                else:
                    sorted_by_index[converted_decode_segment["_index"]].append(converted_decode_segment)
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
                    process_logger.info('Acciqured lock to put index in elasticsearch')
                    headers = {'Content-Type': "application/json"}
                    mapping = {"settings": {"index.mapping.total_fields.limit": 2000}, "mappings": {"nodes": {
                        "properties": {"type": {"type": "keyword"}, "keys": {"type": "object"},
                                       "content": {"type": "object"}, "timestamp": {"type": "date"}}}}}
                    index_put_response = request("PUT", index_url, headers=headers, json=mapping)
                    if not index_put_response.status_code == 200:
                        process_logger.error("Error when creating index")
                        process_logger.error(index_put_response.status_code)
                        process_logger.error(index_put_response.json())
                        return False
                    index_list.append(index)
        data_url = f"http://{args.elastic_server}:9200/_bulk"
        segment_list = sorted_by_index[index]
        elastic_index = {'index': {'_index': f'{index}', '_type': 'nodes'}}
        payload_list = [elastic_index]
        for segment in segment_list:
            segment.pop('_index', None)
            payload_list.append(segment)
            payload_list.append(elastic_index)
        payload_list.pop()
        data_to_post = '\n'.join(json.dumps(d) for d in payload_list)
        data_to_post += '\n'
        headers = {'Content-Type': "application/x-ndjson"}
        reply = request("POST", data_url, data=data_to_post, headers=headers)
        if reply.json()['errors']:
            process_logger.error("Error while uploading in bulk")
            process_logger.error(reply.json())
            # process_logger.logger.error(data_to_post)
            return False
    '''
