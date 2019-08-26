import traceback
import json

class ConfigurationParser(object):
    def __init__(self,config_file):
        self.config_file = config_file
        self.clients = {}
        self.config_json = None

        
    def parse_config(self):
        try:
            with open(self.config_file, "rb") as fp:
                self.config_json = fp.read()
                self.config_json = json.loads(self.config_json)
                rc = self.parse_config_file()
                if not rc:
                    return False
            return True
        except Exception as e:
            print(e)
            traceback.print_tb(e.__traceback__)
            return False

            
    def parse_config_file(self):
        top_level_keywords = ["routers", "ES", "cisco-dial-in", "gnmi", "cisco-dial-out"]
        top_level_keys = list(self.config_json.keys())
        if len(top_level_keys) == 5:
            if not all([item in top_level_keywords for item in top_level_keys]):
                return False
            self.clients["cisco-dial-out"] = {}
            self.clients["cisco-dial-out"]["ES-Port"] = 9200
            self.clients["cisco-dial-out"]["DatabaseIP"] = []
            self.clients["cisco-dial-out"]["BatchSize"] = 50
            for router in self.config_json["routers"]:
                try:
                    name = router["router-name"]
                    self.clients[name] = {}
                    self.clients[name]["Username"] = router["Username"]
                    self.clients[name]["Address"] = router["Address"]
                    self.clients[name]["Password"] = router["Password"]
                    self.clients[name]["Port"] = router["Port"]
                    self.clients[name]["Subscriptions"] = []
                    self.clients[name]["Sensors"] = []
                    self.clients[name]["Sample"] = None
                    self.clients[name]["ES-Port"] = 9200
                    self.clients[name]["DatabaseIP"] = []
                    self.clients[name]["BatchSize"] = 50
                    if "TLS" in router.keys():
                        self.clients[name]["TLS"] = router["TLS"]
                        if "PemFile" in router.keys():
                            self.clients[name]["Pem"] = router["PemFile"]
                except Exception as e:
                    print(e)
                    traceback.print_tb(e.__traceback__)
                    return False
                
            for cisco_ems in self.config_json["cisco-dial-in"]:
                try:
                    if cisco_ems["router-name"] == "ALL":
                        for client in self.clients:
                            if not client == "cisco-dial-out":
                                self.clients[client]["Subscriptions"].extend(cisco_ems["Subscriptions"])
                    else:
                        self.clients[cisco_ems["router-name"]]["Subscriptions"].extend(cisco_ems["Subscriptions"])
                except Exception as e:
                    print(e)
                    traceback.print_tb(e.__traceback__)
                    return False
                
            for gnmi in self.config_json["gnmi"]:
                try:
                    if gnmi["router-name"] == "ALL":
                        for client in self.clients:
                            if not client == "cisco-dial-out":
                                self.clients[client]["Sensors"].extend(gnmi["Sensors"])
                                self.clients[client]["Sample"] = gnmi["Sample-rate"]
                    else:
                        self.clients[gnmi["router-name"]]["Sensors"].extend(gnmi["Sensors"])
                        self.clients[gnmi["router-name"]]["Sample"] = gnmi["Sample-rate"]
                except Exception as e:
                    print(e)
                    traceback.print_tb(e.__traceback__)
                    return False
            for es in self.config_json["ES"]:
                try:
                    if es["router-name"] == "ALL":
                        for client in self.clients:
                            self.clients[client]["ES-Port"] = es["ES-Port"]
                            self.clients[client]["BatchSize"] = es["BatchSize"]
                            self.clients[client]["DatabaseIP"].extend(es["DatabaseIP"])
                    else:
                        self.clients[es["router-name"]]["DatabaseIP"].extend(es["DatabaseIP"])
                        self.clients[es["router-name"]]["BatchSize"] = es["BatchSize"]
                        self.clients[es["router-name"]]["ES-Port"] = es["ES-Port"]
                except Exception as e:
                    print(e)
                    traceback.print_tb(e.__traceback__)
                    return False
            for cisco_dial_out in self.config_json["cisco-dial-out"]:
                try:
                    self.clients["cisco-dial-out"]["Port"] = cisco_dial_out["Port"]
                    self.clients["cisco-dial-out"]["Address"] = cisco_dial_out["Address"]
                except Exception as e:
                    print(e)
                    traceback.print_tb(e.__traceback__)
                    return False
            return True
                
                            
    

