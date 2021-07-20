"""
.. module:: utils
   :platform: Unix, Windows
   :synopsis: Utility functions used throughout RTNM
.. moduleauther:: Greg Brown <gsb5067@gmail.com>
"""
import sys
import re
from datetime import datetime
from distutils.util import strtobool
from typing import Tuple, Dict, Any, List
from configparser import ConfigParser
from errors.errors import ConfigError
from protos.gnmi_pb2 import (
    PathElem,
    Path,
    Encoding,
    SubscriptionMode,
    SubscriptionList
)


def generate_clients(in_file: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """ Generate both the input and output clients based on
    the input configuration file

    :param in_file: The name of the input config file to parse
    :type in_file: str
    :returns: A tuple of both input and output clients
    """

    config: ConfigParser = ConfigParser()
    config.read(in_file)
    input_defined: List[bool] = []
    output_defined: List[bool] = []
    for section in config.sections():
        input_defined.append(config[section]["io"] == "input")
        output_defined.append(config[section]["io"] == "output")
    io_defined = any(input_defined) and any(output_defined)
    if not io_defined:
        raise ConfigError("Config file must contain both an input and output section")
    else:
        input_clients: Dict[str, Dict[str, Any]] = {}
        output_clients: Dict[str, Dict[str, Any]] = {}
        for section in config.sections():
            if config[section]["io"] == "input":
                input_clients[section] = {}
                input_clients[section]["io"] = "in"
                if ":" in config[section]["address"]:
                    input_clients[section]["address"] = f'[{config[section]["address"]}]'
                else:
                    input_clients[section]["address"] = config[section]["address"]
                input_clients[section]["port"] = config[section]["port"]
                input_clients[section]["dial"] = "out"
                if config[section]["dial"] == "in":
                    input_clients[section]["dial"] = "in"
                    input_clients[section]["username"] = config[section]["username"]
                    input_clients[section]["password"] = config[section]["password"]
                    input_clients[section]["compression"] = bool(strtobool(config[section]["compression"]))
                    if config[section]["format"] == "gnmi":
                        input_clients[section]["format"] = "gnmi"
                        input_clients[section]["sensors"] = [
                            x.strip() for x in config[section]["sensors"].split(",")
                        ]
                        input_clients[section]["sample-interval"] = int(config[section]["sample-interval"]) * 1000000000
                        input_clients[section]["subscription-mode"] = SubscriptionMode.Value(
                            config[section]["subscription-mode"])
                        input_clients[section]["encoding"] = Encoding.Value(config[section]["encoding"])
                        input_clients[section]["stream-mode"] = SubscriptionList.Mode.Value(
                            config[section]["stream-mode"])
                    else:
                        input_clients[section]["format"] = "cisco-ems"
                        # Valid encode values- gpb:2, self-describing-gpb:3, json:4
                        encodings: Dict[str, int] = {"gpb": 2, "self-describing-gpb": 3, "json": 4}
                        input_clients[section]["encoding"] = encodings[config[section]["encoding"]]
                        input_clients[section]["subscriptions"] = [
                            x.strip() for x in config[section]["subscriptions"].split(",")
                        ]
                    if "pem-file" in config[section]:
                        input_clients[section]["pem-file"] = config[section]["pem-file"]
            else:
                output_clients[section] = {}
                output_clients[section]["address"] = config[section]["address"]
                output_clients[section]["port"] = config[section]["port"]
                output_clients[section]["type"] = config[section]["type"]
                if output_clients[section]["type"] == "influxdb":
                    output_clients[section]["database"] = config[section]["database"]
                    output_clients[section]["username"] = config[section]["username"]
                    output_clients[section]["password"] = config[section]["password"]
                elif output_clients[section]["type"] == "influxdbv2":
                    output_clients[section]["token"] = config[section]["token"]
                    output_clients[section]["org"] = config[section]["org"]
                    output_clients[section]["bucket"] = config[section]["bucket"]
        return input_clients, output_clients


def create_gnmi_path(path: str) -> Path:
    """ Take a string representation of a gNMI path and transform
    it into the gNMI Path object

    :param path: The gNMI path to be converted into Path object
    :type path: str
    :returns: A gNMI Path object that contains the path elemetns

    """
    path_elements: List[PathElem] = []
    if path[0] == "/":
        if path[-1] == "/":
            path_list: str = re.split(r"""/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)""", path)[1:-1]
        else:
            path_list: str = re.split(r"""/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)""", path)[1:]
    else:
        if path[-1] == "/":
            path_list: str = re.split(r"""/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)""", path)[:-1]
        else:
            path_list: str = re.split(r"""/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)""", path)
    for elem in path_list:
        elem_name: str = elem.split("[", 1)[0]
        elem_keys: List[str] = re.findall(r"\[(.*?)\]", elem)
        dict_keys: Dict[str, Any] = dict(x.split("=", 1) for x in elem_keys)
        path_elements.append(PathElem(name=elem_name, key=dict_keys))
    return Path(elem=path_elements)


def get_date() -> str:
    """ Get the current date in year-month-day format

    :returns: The string representation of the date

    """
    now: datetime = datetime.now()
    month: str = f"{now.month:02d}"
    day: str = f"{now.day:02d}"
    return ".".join([str(now.year), month, day])


def yang_path_to_es_index(yang_path: str) -> str:
    """ Convert a given yang path to Elastic Search index format

    :param yang_path: The yang path name to be converted to Elastic Search index format
    :type yang_path: str
    :returns: The Elastic Search index string

    """
    index: str = (yang_path.replace("/", "-").lower().replace(":", "-").replace("[", "-").replace("]", "").replace('"', ""))
    date: str = get_date()
    size_of_date: int = sys.getsizeof(date)
    while sys.getsizeof(index) + size_of_date > 255:
        index = "-".join(index.split("-")[:-1])
    return f"{index}-{get_date()}"
