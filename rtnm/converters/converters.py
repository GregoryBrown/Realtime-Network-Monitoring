from errors.errors import DecodeError
from protos.telemetry_pb2 import Telemetry
from google.protobuf import json_format
from collections import defaultdict
from protos.gnmi_pb2 import SubscribeResponse
import json
import datetime
import sys


def get_date():
    now = datetime.datetime.now()
    month = f"{now.month:02d}"
    day = f"{now.day:02d}"
    return ".".join([str(now.year), month, day])


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
            formatted_json_segments = [item for sublist in formatted_json_segments for item in sublist]
            # self.log.info("CISCO ENCODING")
            # self.log.info(formatted_json_segments)
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
                        output["@timestamp"] = data["timestamp"]
                        output["_index"] = (
                            telemetry_json["encodingPath"].replace("/", "-").lower().replace(":", "-")
                            + "-"
                            + get_date()
                        )
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
                if "fields" in item:
                    data_dict[item["name"]].append(self._parse_cisco_data(item["fields"]))
                else:
                    for key, value in item.items():
                        if "Value" in key:
                            if "uint" in key:
                                # Check if is an int, and if it is a BIG INTEGER make string so it can upload to ES
                                rc_value = int(value)
                                if rc_value > sys.maxsize:
                                    rc_value = str(rc_value)
                            elif "String" in key:
                                rc_value = str(value)
                            else:
                                rc_value = value
                            data_dict[item["name"]] = rc_value
            return data_dict
        except Exception as e:
            self.log.error(e)
            self.log.error(item)

    def process_gnmi(self):
        decoded_responses = []
        formatted_json_segments = []
        for gpb in self.batch_list:
            sr = SubscribeResponse()
            sr.ParseFromString(gpb[0])
            decoded_responses.append([sr, gpb[1]])
            del sr
        for sub_response in decoded_responses:
            keys, encode_path = self.process_header(sub_response[0].update.prefix)
            index = encode_path.replace("/", "-").lower().replace(":", "-") + "-gnmi-" + get_date()
            timestamp = sub_response[0].update.timestamp
            content = self.parse_gnmi(sub_response[0].update)
            output = {
                "_index": index,
                "keys": [keys],
                "content": [content],
                "encode_path": encode_path,
                "host": sub_response[1],
                "@timestamp": int(timestamp) / 1000000,
            }

            formatted_json_segments.append(json.loads(json.dumps(output)))
            # self.log.info("GNMI ENCODING")
            # self.log.info(formatted_json_segments)
        return formatted_json_segments

    def process_header(self, header):
        index = header.origin
        keys = {}
        encode_path = []
        for elem in header.elem:
            encode_path.append(elem.name)
            if elem.key:
                keys.update(elem.key)
        return keys, f"{index}:{'/'.join(encode_path)}"

    def parse_gnmi(self, notifications):
        content_list = []
        for update in notifications.update:
            value = self.get_value(update.val)
            content_list.append(self.flatten(update.path, value))
        rc = self.convert_to_json(content_list)
        return json.loads(json.dumps(rc))

    def convert_to_json(self, content_list):
        rc = defaultdict(list)
        for content in content_list:
            self.convert_to_json_sub(content, rc)
        return rc

    def convert_to_json_sub(self, clist, rc):
        if len(clist) == 2:
            if type(rc) is list:
                if rc == []:
                    temp = defaultdict(list)
                    temp[clist[0]] = clist[1]
                    return temp
                else:
                    rc = rc[0]
                    if clist[0] in rc:
                        temp = [rc[clist[0]]]
                        temp.append(clist[1])
                        rc[clist[0]] = temp
                    else:
                        rc.update({clist[0]: clist[1]})
            else:
                rc[clist[0]] = clist[1]
        else:
            if type(rc) is list:
                if not rc == []:
                    rc = rc[0]
                else:
                    rc.append(defaultdict(list))
                    rc = rc[0]
            rc[clist[0]].append(self.convert_to_json_sub(clist[1:], rc[clist[0]]))
            rc[clist[0]] = [x for x in rc[clist[0]] if x]

    def get_value(self, type_value):
        value_type = type_value.WhichOneof("value")
        return getattr(type_value, value_type)

    def flatten(self, path, value):
        rc = []
        for elem in path.elem:
            rc.append(elem.name)
        rc.append(value)
        return rc
