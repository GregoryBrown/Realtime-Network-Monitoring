"""
.. module:: Parsers
   :platform: Unix, Windows
   :synopsis: Base classes for parsers and parsed responses
.. moduleauthor:: Greg Brown <gsb5067@gmail.com>
"""

import json
from typing import List, Union, Optional, Tuple, Dict, Any
from logging import getLogger, Logger
from protos.gnmi_pb2 import SubscribeResponse, TypedValue, Update
from protos.telemetry_pb2 import Telemetry, TelemetryField


class ParsedResponse:
    def __init__(self, yang_path: str, data: Dict[str, Any], version: str, hostname: str, encoding: str, timestamp: int, ip: str) -> None:
        self.version: str = version
        self.hostname: str = hostname
        self.yang_path: str = yang_path
        self.data: Dict[str, Any] = data
        self.encoding: str = encoding
        self.timestamp: int = timestamp
        self.ip_addr: str = ip

    def __str__(self):
        return f"{self.hostname}\n{self.version}\n{self.yang_path}\n{self.data}"


class RTNMParser:
    def __init__(self, batch_list: List[Tuple[str, str, Optional[str], Optional[str], str]],
                 log_name: str) -> None:
        self.raw_responses: List[Tuple[str, str, Optional[str], Optional[str], str]] = batch_list
        self.log: Logger = getLogger(log_name)

    def process_header(self, header: Update) -> Tuple[Dict[str, str], str]:
        """Separate the update header into keys and the starting yang path

        :param header: The top level update of the gNMI response that has the keys and yang path
        :type header: Update

        """

        keys: Dict[str, str] = {}
        yang_path: List[str] = []
        for elem in header.prefix.elem:
            yang_path.append(elem.name)
            if elem.key:
                keys.update(elem.key)
        return keys, f"{header.prefix.origin}:{'/'.join(yang_path)}"

    def get_value(self, type_value: TypedValue):
        """Using gNMI defined possible value encodings get the value in its native encoding_path

        :param type_value: The value in the response
        :param type_value: TypedValue

        """
        value_type = type_value.WhichOneof("value")

        def leaf_list_parse(value):
            value_list = []
            for element in value.element:
                value_type_leaf_parse = element.WhichOneof("value")
                func_leaf_parse = value_encodings[value_type_leaf_parse]
                value_list.append(func_leaf_parse(getattr(element, value_type_leaf_parse)))
            return value_list

        def decimal_parse(value):
            return value.digits

        value_encodings = {
            "string_val": str,
            "int_val": int,
            "uint_val": int,
            "bool_val": bool,
            "bytes_val": bytes,
            "float_val": float,
            "decimal_val": decimal_parse,
            "leaflist_val": leaf_list_parse,
            "json_val": json.loads,
            "json_ietf_val": json.loads,
            "ascii_val": str,
            "proto_bytes": bytes,
        }
        func = value_encodings[value_type]
        return func(getattr(type_value, value_type))

    def _decode(self, raw_message: Tuple[str, str, Optional[str], Optional[str]]) -> Union[SubscribeResponse, Telemetry]:
        if raw_message[0] == "gnmi":
            self.log.debug("In decode gnmi")
            self.log.debug(raw_message[1])
            sub = SubscribeResponse()
            sub.ParseFromString(raw_message[1])
            self.log.debug(sub)
            return sub
        else:
            self.log.debug("In decode ems")
            self.log.debug(raw_message[1])
            tele = Telemetry()
            tele.ParseFromString(raw_message[1])
            self.log.debug(tele)
            return tele
        
    def parse_gnmi(self, response: SubscribeResponse, hostname: str, version: str, ip: str) -> List[ParsedResponse]:
        self.log.debug("In parse_gnmi")
        keys, start_yang_path = self.process_header(response.update)
        content_list: List[Dict[str, Any]] = []
        for update in response.update.update:
            yang_paths = []
            value = self.get_value(update.val)
            for elem in update.path.elem:
                yang_paths.append(elem.name)
            leaf = yang_paths.pop()
            end_yang_path = "/".join(yang_paths)
            if yang_paths:
                content_list.append({f"{start_yang_path}/{end_yang_path}": {leaf: value}})
            else:
                content_list.append({f"{start_yang_path}{end_yang_path}": {leaf: value}})
        sorted_content: Dict[str, Any] = {}
        for content_entry in content_list:
            key = list(content_entry.keys())[0]
            if key in sorted_content:
                sorted_content[key].update(list(content_entry.values())[0])
            else:
                sorted_content[key] = {}
                sorted_content[key].update(list(content_entry.values())[0])
        rc_parsed_responses: List[ParsedResponse] = []
        for yang_path, content in sorted_content.items():
            rc_parsed_responses.append(ParsedResponse(yang_path, {"keys": keys, "content": content},
                                                      version, hostname, "gnmi", int(response.update.timestamp), ip))
        return rc_parsed_responses

    def get_ems_values(self, value_by_type, value):
        ems_values: Dict[str, Any] = {
            "bytes_value": str,
            "string_value": str,
            "bool_value": bool,
            "uint32_value": int,
            "uint64_value": int,
            "sint32_value": int,
            "sint64_value": int,
            "double_value": float,
            "float_value": float
        }
        if value_by_type is not None:
            func = ems_values[value_by_type]
            return func(getattr(value, value_by_type))
        return ""

    def parse_keys(self, key_dict: TelemetryField) -> Dict[str, Any]:
        keys: Dict[str, Any] = {}
        for field in key_dict.fields:
            keys[field.name] = self.get_ems_values(field.WhichOneof("value_by_type"), field)
        return keys

    def parse_content(self, content_tf: TelemetryField, path: str, parsed_content: Dict[str, Dict[str, Any]]) -> None:
        content_dict: Dict[str, Any] = {}
        leaf_level: bool = False
        for field in content_tf.fields:
            if field.fields:
                self.parse_content(field, f"{path}/{field.name}", parsed_content)
            else:
                leaf_level = True
                content_dict[field.name] = self.get_ems_values(field.WhichOneof("value_by_type"), field)
        if leaf_level:
            if path in parsed_content:
                parsed_content[path].append(content_dict)
            else:
                parsed_content[path] = [content_dict]

    def parse_ems(self, response: Telemetry, version: str, ip: str) -> List[ParsedResponse]:
        parsed_list: List[ParsedResponse] = []
        node_str: str = response.node_id_str
        start_yang_path: str = response.encoding_path
        keys: Dict[str, Any] = {}
        for gpbkv in response.data_gpbkv:
            timestamp: int = gpbkv.timestamp
            for telemetry_field in gpbkv.fields:
                if telemetry_field.name == "keys":
                    keys: Dict[str, Any] = self.parse_keys(telemetry_field)
                else:
                    if telemetry_field.delete:
                        parsed_content: Dict[str, Dict[str, Any]] = {'', {"delete": True}}
                    else:
                        parsed_content: Dict[str, Dict[str, Any]] = {}
                        self.parse_content(telemetry_field, "", parsed_content)
            for pc_path, pc_data in parsed_content.items():
                for data in pc_data:
                    total_yang_path = f"{start_yang_path}{pc_path}"
                    parsed_list.append(ParsedResponse(total_yang_path, {"keys": keys, "content": data},
                                                      version, node_str, "grpc", timestamp * 1000000, ip))
        return parsed_list

    def decode_and_parse_raw_responses(self) -> List[ParsedResponse]:
        self.log.debug("In decode and parse")
        parsed_list: List[ParsedResponse] = []
        try:
            for response in self.raw_responses:
                gpb_encoding = response[0]
                decoded_response = self._decode(response)
                if gpb_encoding == "gnmi":
                    parsed_list.extend(self.parse_gnmi(decoded_response,
                                                       response[2], response[3], response[4]))
                else:
                    parsed_list.extend(self.parse_ems(decoded_response, response[3], response[4]))
        except Exception as error:
            self.log.error(error)
            import traceback
            self.log.error(traceback.print_exc())
        return parsed_list
