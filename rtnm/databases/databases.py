from requests import request

class ElasticSearchUploader(object):
    def __init__(self, elastic_server, elastic_port, batch_size, log, index_list):
        self.elastic_server = elastic_server
        self.elastic_port = elastic_port
        self.batch_size = batch_size
        self.get_url = f"http://{self.elastic_server}:{self.elastic_port}/*"
        self.log = log
        self.index_list = index_list
        
    def populate_index_list(self):
        try:
            self.log.info("Populating index list")
            get_all_sensors_response = request("GET", self.get_url)
            if not get_all_sensors_response.status_code == 200:
                self.log.error("Response status wasn't 200")
                self.log.error(get_all_sensors_response.json())
                return False
            for key in get_all_sensors_response.json():
                if not key.startswith('.'):
                    self.index_list.append(key)
            return self.index_list
        except Exception as e:
            self.log.error(e)
            return False

