"""
.. module:: ElasticSearchParser
   :platform: Unix, Windows
   :synopsis: Parser of gRPC/gNMI raw responses used for uploading to a TSDB 
.. moduleauthor:: Greg Brown <gsb5067@gmail.com>
"""
from typing import List, Union, Optional, Tuple, Dict, Any
import json
from parsers.Parsers import RTNMParser, ParsedResponse
from protos.gnmi_pb2 import SubscribeResponse, TypedValue, Update  # type: ignore
from protos.telemetry_pb2 import Telemetry, TelemetryField  # type: ignore
from utils.utils import yang_path_to_es_index


class ElasticSearchParser(RTNMParser):
    """Parser class to take raw responses and decode them and put them into
    yang level responses for upload

    :param args: Arguments for parsing
    :type args: List[str]
    :param kwargs: keyword arguments for parsing
    :type kwargs: Dict[str, Any]

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log.debug("Created Elasticsearch Parser Object")

    @staticmethod
    def process_header(header: Update) -> Tuple[Dict[str, str], str]:
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

    @staticmethod
    def get_value(type_value: TypedValue) -> Any:
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

        def int_parse(value):
            if value > 2**63-1:
                value = str(value)
            return value

        value_encodings: Dict[str, Any] = {
            "string_val": str,
            "int_val": int_parse,
            "uint_val": int_parse,
            "bool_val": bool,
            "bytes_val": bytes,
            "float_val": float,
            "decimal_val": decimal_parse,
            "leaflist_val": leaf_list_parse,
            "json_val": json.loads,
            "json_ietf_val": json.loads,
            "ascii_val": str,
            "proto_bytes": str,
        }
        func = value_encodings[value_type]
        return func(getattr(type_value, value_type))

    def _decode(self, raw_message: Tuple[str, str, Optional[str], Optional[str]]) -> Union[SubscribeResponse, Telemetry]:
        if raw_message[0] == "gnmi":
            self.log.debug("In decode gnmi")
            self.log.debug(raw_message[1])
            sub = SubscribeResponse()
            sub.ParseFromString(raw_message[1])
            return sub
        else:
            self.log.debug("In decode ems")
            self.log.debug(raw_message[1])
            tele = Telemetry()
            tele.ParseFromString(raw_message[1])
            self.log.debug(tele)
            return tele

    def parse_gnmi(self, response: SubscribeResponse, hostname: Optional[str], version: Optional[str]) -> List[ParsedResponse]:
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
        for key, content in sorted_content.items():
            parsed_dict = {
                "@timestamp": (int(response.update.timestamp) / 1000000),
                "keys": keys,
                "content": content,
                "yang_path": key,
                "index": yang_path_to_es_index(key, "gnmi")
            }
            rc_parsed_responses.append(ParsedResponse(parsed_dict, version, hostname))
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
        if value_by_type is None:
            return ""
        else:
            func = ems_values[value_by_type]
            return func(getattr(value, value_by_type))

    def parse_ems(self, response: Telemetry, version: Optional[str]) -> List[ParsedResponse]:
        def parse_keys(key_dict: TelemetryField) -> Dict[str, Any]:
            keys: Dict[str, Any] = {}

            for field in key_dict.fields:
                keys[field.name] = self.get_ems_values(field.WhichOneof("value_by_type"), field)
            return keys

        def parse_content(content_field_list: List[TelemetryField],
                          path: str, all_content_list: List[Dict[str, Any]]) -> None:
            # List of telemetry fields, some have multiple levels others don't
            content_dict: Dict[str, Any] = {}
            content_dict[path]: Dict[str, Any] = {}
            for field in content_field_list:
                if not field.fields:
                    if field.name in content_dict[path]:
                        if isinstance(content_dict[path][field.name], list):
                            content_dict[path][field.name].append(self.get_ems_values(
                                field.WhichOneof("value_by_type"), field))
                        else:
                            first_value = content_dict[path][field.name]
                            content_dict[path][field.name] = [first_value, self.get_ems_values(
                                field.WhichOneof("value_by_type"), field)]
                    else:
                        content_dict[path][field.name] = self.get_ems_values(
                            field.WhichOneof("value_by_type"), field)
            all_content_list.append(content_dict)
            for field in content_field_list:
                if field.fields:
                    parse_content(field.fields, f"{path}/{field.name}", all_content_list)

        def parse_telemetry_field(telemetry_field: TelemetryField) -> Tuple[Dict[str, Any],
                                                                            List[Dict[str, Any]]]:
            # Every telemetry_field has a keys and content fields
            # in the telemetry_field.fields list
            keys: Dict[str, Any] = parse_keys(telemetry_field.fields[0])
            path: str = ""
            all_content: List[Dict[str, Any]] = []
            parse_content(telemetry_field.fields[1].fields, path, all_content)
            return keys, all_content

        def parse_data_gpbkv(telemetry_fields: List[TelemetryField]) -> List[Tuple[Dict[str, Any], List[Dict[str, Any]], int]]:
            all_responses: List[Tuple[Dict[str, Any], List[Dict[str, Any]], int]] = []
            for telemetry_field in telemetry_fields:
                if telemetry_field.delete:
                    keys = parse_keys(telemetry_field.fields[0])
                    all_responses.append((keys, [{"": {"delete": True}}], telemetry_field.timestamp))
                else:
                    keys, content = parse_telemetry_field(telemetry_field)
                    all_responses.append((keys, content, telemetry_field.timestamp))
            return all_responses

        self.log.debug("In parse_ems")
        node_str = response.node_id_str
        start_yang_path = response.encoding_path
        version = version
        parsed_responses: List[Tuple[Dict[str, Any], List[Dict[str, Any]], int]] = parse_data_gpbkv(response.data_gpbkv)
        parsed_list: List[ParsedResponse] = []
        try:
            for parsed_response in parsed_responses:
                keys, content_list, timestamp = parsed_response
                for content_entry in content_list:
                    parsed_dict: Dict[str, Any] = {}
                    parsed_dict["keys"] = keys
                    parsed_dict["@timestamp"] = timestamp
                    total_yang_path = f"{start_yang_path}{list(content_entry.keys())[0]}"
                    parsed_dict["content"] = list(content_entry.values())[0]
                    parsed_dict["yang_path"] = total_yang_path
                    parsed_dict["index"] = yang_path_to_es_index(total_yang_path, "grpc")
                    parsed_list.append(ParsedResponse(parsed_dict, version, node_str))
                # yield ParsedResponse(parsed_dict, version, node_str)
            return parsed_list
        except Exception as error:
            self.log.error(error)

    def decode_and_parse_raw_responses(self) -> List[ParsedResponse]:
        self.log.debug("In decode and parse")
        parsed_list: List[ParsedResponse] = []
        try:
            for response in self.raw_responses:
                gpb_encoding = response[0]
                decoded_response = self._decode(response)
                self.log.debug(decoded_response)
                if gpb_encoding == "gnmi":
                    parsed_list.extend(self.parse_gnmi(decoded_response, response[2], response[3]))
                else:
                    parsed_list.extend(self.parse_ems(decoded_response, response[3]))
        except Exception as error:
            self.log.error(error)
            import traceback
            self.log.error(traceback.print_exc())
        return parsed_list
