import json
import gzip
from logging import Logger, getLogger
from requests import request, Response
from errors.errors import ElasticSearchUploaderError
from typing import Dict, Any, List
from parsers.Parsers import ParsedResponse
from datetime import datetime


class Uploader:

    def __init__(self, server: str, port: str, log_name: str) -> None:
        self.url: str = f"http://{server}:{port}"
        self.log: Logger = getLogger(log_name)
        self.log.debug(self.url)

    def upload(self, data: List[ParsedResponse]):
        raise NotImplementedError("Can't call upload in base class")


class ElasticSearchUploader(Uploader):
    """ElasticSearchUploader creates a connection to an ElasticSearch instance
    :param elastic_server: The IP of the ElasticSearch instance
    :type elastic_server: str
    :param elastic_port: The port number of the ElasticSearch instance
    :type elastic_port: str
    :param log: Logger instance to log any debug and errors
    :type log: Logger
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.log.debug("Created ElasticSearchUploader")

    def _post_parsed_response(self, data: str) -> None:
        """ Post data to an ES instance with a given index
        :param data: The data you want to post
        :type data: ParsedGetResponse
        :param index: The index to post the data to
        :type index: str
        :raises: ElasticSearchUploaderException
        """
        self.log.debug(data)
        headers: Dict[str, Any] = {"Content-Encoding": "gzip", "Content-Type": "application/x-ndjson"}
        data_to_post: bytes = gzip.compress(data.encode("utf-8"))
        post_response: Response = request("POST", f"{self.url}/_bulk?timeout=120s", data=data_to_post, headers=headers)
        if post_response.status_code not in [200, 201]:
            self.log.error(data)
            self.log.error(post_response)
            self.log.error(post_response.json())
            raise ElasticSearchUploaderError("Error while posting data to ElasticSearch")

    def upload(self, data: List[ParsedResponse]):
        """Upload operation data into Elasticsearch
        :param data: The data to upload to Elastic Search
        :type data: List[ParsedGetResponse]
        """
        start = datetime.now()
        payload_list: List[Dict[str, Any]] = []
        for parsed_response in data:
            index: str = parsed_response.dict_to_upload.pop("index")
            elastic_index: Dict[str, Any] = {"index": {"_index": f"{index}"}}
            payload_list.append(elastic_index)
            parsed_response.dict_to_upload["host"] = parsed_response.hostname
            parsed_response.dict_to_upload["version"] = parsed_response.version
            payload_list.append(parsed_response.dict_to_upload)
        data_to_post: str = "\n".join(json.dumps(d) for d in payload_list)
        if data_to_post.strip():
            data_to_post += "\n"
            self._post_parsed_response(data_to_post)
        end = datetime.now()
        total_time = end - start
        self.log.debug(f"Total Batch time took {total_time}")


class InfluxdbUploader(Uploader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log.debug("Created InfluxdbUploader")

    def post_data(self, data: List[str]):
        post_str: str = "\n".join(data)
        self.url = f"{self.url}/api/v2/write?precision=ns&bucket=devdb"
        data_to_post: bytes = gzip.compress(post_str.encode("utf-8"))
        headers = {
            'Content-Encoding': 'gzip',
            'Content-Type': 'text/plain',
            'Authorization': 'Basic YWRtaW46aW5mbHV4ZGJDIXNjbzEyMw=='
        }
        start = datetime.now()
        post_response = request("POST", self.url, headers=headers, data=data_to_post)
        if post_response.status_code not in [200, 201, 204]:
            self.log.error(post_response)
            self.log.error(post_response.json())
            raise Exception("Error uploading influxdb")
        end = datetime.now()
        total_time = end - start
        self.log.info(f"Total upload time took {total_time}")

    def upload(self, data: List[ParsedResponse]):
        influxdb_lines: List[str] = []
        timestamp_inc_counter = 0
        for entry in data:
            tag_line: Dict[str, Any] = {}
            for tag_key, tag_value in entry.data["keys"].items():
                if isinstance(tag_value, str):
                    tag_value = " ".join(tag_value.split()).strip().replace(
                        ",", "\,").replace("=", "\=").replace('"', '')
                    if tag_value == "":
                        tag_line[tag_key] = f'"{tag_value}"'  # =""
                    else:
                        tag_line[tag_key] = tag_value
                else:
                    tag_line[tag_key] = tag_value
            tag_line["encoding"] = entry.encoding
            tag_line["hostname"] = entry.hostname
            tag_line["ip"] = entry.ip_addr
            tag_line["version"] = entry.version
            field_line: List[str] = []
            for field_key, field_value in entry.data["content"].items():
                if isinstance(field_value, str):
                    field_value: str = " ".join(field_value.split()).strip().replace(" ", "\ ").replace(",", "\,").replace("=", "\=").replace('"', '')
                    if field_key not in tag_line:
                        if not field_value:
                            tag_line[field_key] = f'"{field_value}"'
                        else:
                            tag_line[field_key] = field_value
                    field_line.append(f'{field_key}="{field_value}"')
                else:
                    field_line.append(f"{field_key}={field_value}")
            field_line: str = ",".join(field_line)
            tag_line: str = ",".join([f"{key}={value}" for key, value in tag_line.items()])
            timestamp = entry.timestamp + timestamp_inc_counter
            influxdb_lines.append(f"{entry.yang_path},{tag_line} {field_line} {timestamp}")
            self.log.debug(influxdb_lines)
            timestamp_inc_counter += 1
        self.post_data(influxdb_lines)
