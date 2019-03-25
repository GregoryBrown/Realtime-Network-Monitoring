import traceback
import json

class ConfigurationParser(object):
    def __init__(self,config_file):
        self.config_file = config_file
        self.client_configurations = {}
        self.config_json = None

        
    def parse_arguments_and_config(self):
        try:
            with open(self.config_file, "rb") as fp:
                self.config_json = fp.read()
                self.config_json = json.loads(self.config_json)
                rc = self.parse_config_file()
                if not rc:
                    print("Unable to parse config file")
                    exit(1)
        except Exception as e:
            print(e)
            traceback.print_tb(e.__traceback__)

            
    def parse_config_file(self):
        top_level_keywords = ["routers", "ES", "cisco-ems", "gnmi"]
        top_level_keys = list(self.config_json.keys())
        if len(top_level_keys) == 4:
            if not all([item in top_level_keywords for item in top_level_keys]):
                return False
            for router in self.config_json["routers"]:
                try:
                    name = router["router-name"]
                    self.client_configurations[name] = {}
                    self.client_configurations[name]["Username"] = router["Username"]
                    self.client_configurations[name]["Address"] = router["Address"]
                    self.client_configurations[name]["Password"] = router["Password"]
                    self.client_configurations[name]["Port"] = router["Port"]
                    self.client_configurations[name]["Subscriptions"] = []
                    self.client_configurations[name]["Sensors"] = []
                    self.client_configurations[name]["Sample"] = None
                    self.client_configurations[name]["ES-Port"] = 9200
                    self.client_configurations[name]["DatabaseIP"] = []
                    self.client_configurations[name]["BatchSize"] = 50
                    if "TLS" in router.keys():
                        self.client_configurations[name]["TLS"] = router["TLS"]
                        if "PemFile" in router.keys():
                            self.client_configurations[name]["Pem"] = router["PemFile"]
                except Exception as e:
                    print(e)
                    traceback.print_tb(e.__traceback__)
                    return False
                
            for cisco_ems in self.config_json["cisco-ems"]:
                try:
                    if cisco_ems["router-name"] == "ALL":
                        for client in self.client_configurations:
                            self.client_configurations[client]["Subscriptions"].extend(cisco_ems["Subscriptions"])
                    else:
                        self.client_configurations[cisco_ems["router-name"]]["Subscriptions"].extend(cisco_ems["Subscriptions"])
                except Exception as e:
                    print(e)
                    traceback.print_tb(e.__traceback__)
                    return False
                
            for gnmi in self.config_json["gnmi"]:
                try:
                    if gnmi["router-name"] == "ALL":
                        for client in self.client_configurations:
                            self.client_configurations[client]["Sensors"].extend(gnmi["Sensors"])
                            self.client_configurations[client]["Sample"] = gnmi["Sample-rate"]
                    else:
                        self.client_configurations[gnmi["router-name"]]["Sensors"].extend(gnmi["Sensors"])
                        self.client_configurations[gnmi["router-name"]]["Sample"] = gnmi["Sample-rate"]
                except Exception as e:
                    print(e)
                    traceback.print_tb(e.__traceback__)
                    return False
            for es in self.config_json["ES"]:
                try:
                    if es["router-name"] == "ALL":
                        for client in self.client_configurations:
                            self.client_configurations[client]["ES-Port"] = es["ES-Port"]
                            self.client_configurations[client]["BatchSize"] = es["BatchSize"]
                            self.client_configurations[client]["DatabaseIP"].extend(es["DatabaseIP"])
                    else:
                        self.client_configurations[es["router-name"]]["DatabaseIP"].extend(es["DatabaseIP"])
                        self.client_configurations[es["router-name"]]["BatchSize"] = es["BatchSize"]
                        self.client_configurations[es["router-name"]]["ES-Port"] = es["ES-Port"]
                except Exception as e:
                    print(e)
                    traceback.print_tb(e.__traceback__)
                    return False
            return True
                
                            
    

