import json
import gzip
from logging import Logger
from requests import request, Response
from errors.errors import ElasticSearchUploaderError
from typing import Dict, Any, List
from parsers.Parsers import ParsedResponse


class ElasticSearchUploader:
    """ElasticSearchUploader creates a connection to an ElasticSearch instance
    :param elastic_server: The IP of the ElasticSearch instance
    :type elastic_server: str
    :param elastic_port: The port number of the ElasticSearch instance
    :type elastic_port: str
    :param log: Logger instance to log any debug and errors
    :type log: Logger
    """

    def __init__(self, elastic_server: str, elastic_port: str, log: Logger) -> None:
        self.url: str = f"http://{elastic_server}:{elastic_port}"
        self.log: Logger = log

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
        post_response: Response = request("POST", f"{self.url}/_bulk", data=data_to_post, headers=headers)
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
