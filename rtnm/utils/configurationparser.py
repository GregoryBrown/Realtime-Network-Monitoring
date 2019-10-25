import traceback
import json
import configparser
from utils.exceptions import IODefinedError
    

class ConfigurationParser(object):
    def __init__(self, in_file):
        self.config = configparser.ConfigParser()
        try:
            self.config.read(in_file)
        except Exception as e:
            raise e

    def generate_clients(self):
        io_defined = False
        input_defined = []
        output_defined = []
        for section in self.config.sections():
            try:
                input_defined.append(self.config[section]["io"] == "input")
                output_defined.append(self.config[section]["io"] == "output")
            except Exception as e:
                raise e
        io_defined = any(input_defined) and any(output_defined)
        if not io_defined:
            raise IODefinedError
        input_clients = {}
        output_clients = {}
        sub_mode = {'sample' : 2, 'on-change' : 1}
        stream_mode = {'stream' : 0, 'once': 1, 'poll' : 2}
        for section in self.config.sections():
            if self.config[section]["io"] == "input":
                try:
                    input_clients[section] = {}
                    if self.config[section]["dial"] == "in":
                        input_clients[section]["io"] = "in"
                        input_clients[section]["address"] = self.config[section]["address"]
                        input_clients[section]["port"] = self.config[section]["port"]
                        input_clients[section]["username"] = self.config[section]["username"]
                        input_clients[section]["password"] = self.config[section]["password"]
                        input_clients[section]["encoding"] = self.config[section]["encoding"]
                        if self.config[section]["format"] == "gnmi":
                            input_clients[section]["format"] = "gnmi"
                            input_clients[section]["sensors"] = [x.strip() for x in self.config[section]["sensors"].split(',')]
                            input_clients[section]["sample-interval"] = self.config[section]["sample-interval"]
                            input_clients[section]["sub-mode"] = sub_mode[self.config[section]["sub-mode"]]
                            input_clients[section]["stream-mode"] = stream_mode[self.config[section]["stream-mode"]]
                        else:
                            input_clients[section]["format"] = "cisco-ems"
                            input_clients[section]["subs"] = [x.strip() for x in self.config[section]["subs"].split(',')]
                            
                        if "pem-file" in self.config[section]:
                            input_clients[section]["pem-file"] = self.config[section]["pem-file"]
                            
                    else:
                        input_clients[section]["io"] = "out"
                        input_clients[section]["address"] = self.config[section]["address"]
                        input_clients[section]["port"] = self.config[section]["port"]
                    input_clients[section]["batch-size"] = self.config[section]["batch-size"]
                except Exception as e:
                    raise e
            else:
                try:
                    output_clients[section] = {}
                    output_clients[section]["address"] = self.config[section]["address"]
                    output_clients[section]["port"] = self.config[section]["port"]
                except Exception as e:
                    raise e
                
        return input_clients, output_clients
