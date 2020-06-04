from typing import List, Tuple, Optional, Dict, Any
from logging import getLogger, Logger


class ParsedResponse:
    def __init__(self, response: Dict[str, Any], version: str, hostname: str) -> None:
        self.version: str = version
        self.hostname: str = hostname
        self.dict_to_upload: Dict[str, Any] = response

    def __str__(self):
        return f"{self.hostname}\n{self.version}\n{self.dict_to_upload}"


class RTNMParser:
    def __init__(self, raw_responses: List[Tuple[str, str, Optional[str], Optional[str]]],
                 log_name: str):
        self.raw_responses: List[Tuple[str, str, Optional[str], Optional[str]]] = raw_responses
        self.log: Logger = getLogger(log_name)

    def decode_and_parse_raw_responses(self) -> List[ParsedResponse]:
        raise NotImplementedError("Can't call on parent class")
