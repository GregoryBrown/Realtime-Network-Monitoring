from typing import List, Union, Optional, Tuple, Iterator, Dict, Any
import json
from parsers.Parsers import RTNMParser, ParsedResponse
from protos.gnmi_pb2 import SubscribeResponse, TypedValue
from protos.telemetry_pb2 import Telemetry, TelemetryField
from utils.utils import yang_path_to_es_index


class ElasticSearchParser(RTNMParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log.debug("Created Elasticsearch Parser Object")

    @staticmethod
    def process_header(header):
        keys = {}
        yang_path = []
        for elem in header.prefix.elem:
            yang_path.append(elem.name)
            if elem.key:
                keys.update(elem.key)
        return keys, f"{header.prefix.origin}:{'/'.join(yang_path)}"

    @staticmethod
    def get_value(type_value: TypedValue):
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
            return sub
        else:
            self.log.debug("In decode ems")
            self.log.debug(raw_message[1])
            tele = Telemetry()
            tele.ParseFromString(raw_message[1])
            self.log.debug(tele)
            return tele

    def parse_gnmi(self, response: SubscribeResponse, hostname: str, version: str) -> Iterator[ParsedResponse]:
        self.log.debug("In parse_gnmi")
        for update in response.update.update:
            parsed_dict = {
                "@timestamp": (int(response.update.timestamp) / 1000000),
                "byte_size": response.ByteSize(),
            }
            keys, start_yang_path = self.process_header(response.update)
            parsed_dict["keys"] = keys
            rc = []
            value = self.get_value(update.val)
            for elem in update.path.elem:
                rc.append(elem.name)
            total_yang_path = f"{start_yang_path}/{'/'.join(rc)}"
            leaf = "-".join(total_yang_path.split("/")[-2:])
            parsed_dict[leaf] = value
            parsed_dict["index"] = yang_path_to_es_index(total_yang_path, "gnmi")
            parsed_dict["yang_path"] = total_yang_path
            yield ParsedResponse(parsed_dict, version, hostname)

    def get_ems_values(self, value_by_type, value):
        ems_values: Dict[str, Any] = {
            "bytes_value": bytes,
            "string_value": str,
            "bool_value": bool,
            "uint32_value": int,
            "uint64_value": int,
            "sint32_value": int,
            "sint64_value": int,
            "double_value": float,
            "float_value": float
        }
        func = ems_values[value_by_type]
        return func(getattr(value, value_by_type))

    def parse_ems(self, response: Telemetry, version: str) -> List[ParsedResponse]:
        self.log.debug("In parse_ems")
        node_str = response.node_id_str
        start_yang_path = response.encoding_path
        version = version

        def parse_keys(key_dict: TelemetryField) -> Dict[str, Any]:
            keys: Dict[str, Any] = {}

            for field in key_dict.fields:
                keys[field.name] = self.get_ems_values(field.WhichOneof("value_by_type"), field)
            return keys

        def parse_content(content_field_list: List[TelemetryField],
                          content_list: List[Dict[str, Any]], path: List[str]) -> None:
            # List of telemetry fields, some have multiple levels others don't
            sub_path: List[str] = path[:]
            # print(content_field_list)
            for field in content_field_list:
                # print(field)
                if not field.fields:
                    content_list.append({field.name: self.get_ems_values(
                        field.WhichOneof("value_by_type"), field), "path": sub_path})
                    # print(content_list)
                else:
                    sub_path.append(field.name)
                    parse_content(field.fields, content_list, sub_path)
                    if sub_path:
                        sub_path.pop()

        def parse_telemetry_field(telemetry_field: TelemetryField):
            # Every telemetry_field has a keys and content fields
            # in the telemetry_field.fields list
            keys = parse_keys(telemetry_field.fields[0])
            content_list: List[Dict[str, Any]] = []
            path: List[str] = []
            parse_content(telemetry_field.fields[1].fields, content_list, path)
            return keys, content_list

        def parse_data_gpbkv(telemetry_fields: List[TelemetryField]) -> List[Dict[str, str]]:
            for telemetry_field in telemetry_fields:
                keys, content = parse_telemetry_field(telemetry_field)
                return keys, content, telemetry_field.timestamp
                # if not data_gpbkv.delete:
                #keys, content = self.parse_fields(data_gpbkv.fields, start_yang_path)
                # self.log.info(keys)
                # self.log.info(data_gpbkv.timestamp)
                # self.log.info(node_str)
                # self.log.info(start_yang_path)
        keys, content, timestamp = parse_data_gpbkv(response.data_gpbkv)
        for content_dict in content:
            parsed_dict: Dict[str, Any] = {}
            parsed_dict["keys"] = keys
            parsed_dict["@timestamp"] = timestamp
            if content_dict["path"]:
                total_yang_path = f"{start_yang_path}/{'/'.join(content_dict['path'])}"
            else:
                total_yang_path = f"{start_yang_path}"
            content_dict.pop("path")
            leaf, value = next(iter(content_dict.items()))
            total_yang_path = f"{total_yang_path}/{leaf}"
            parsed_dict["yang_path"] = total_yang_path
            parsed_dict["index"] = yang_path_to_es_index(total_yang_path, "grpc")
            leaf = "-".join(total_yang_path.split("/")[-2:])
            parsed_dict[leaf] = value
            yield ParsedResponse(parsed_dict, version, node_str)

    def decode_and_parse_raw_responses(self) -> List[ParsedResponse]:
        self.log.debug("In decode and parse")
        parsed_list: List[ParsedResponse] = []
        for response in self.raw_responses:
            gpb_encoding = response[0]
            try:
                decoded_response = self._decode(response)
                if gpb_encoding == "gnmi":
                    parsed_list.append(self.parse_gnmi(decoded_response, response[2], response[3]))
                else:
                    parsed_list.append(self.parse_ems(decoded_response, response[3]))
            except Exception as error:
                self.log.error(error)
        return parsed_list
