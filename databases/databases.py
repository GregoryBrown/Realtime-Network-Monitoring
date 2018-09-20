from py_protos.telemetry_pb2 import Telemetry
from multiprocessing import Manager
import requests


class DBUploader(object):
    def __init__(self, db_address, db_port, lock):
        self.address = db_address
        self.port = db_port
        self.lock = lock
        
    def upload_to_db(self, data):
        pass


class InfluxDBUploader(DBUploader):
    def __init__(self, db_name):
        self.db = db_name
        self.url = f"http://{self.address}:{self.port}/write"
        self.request_params = {"db": self.db, "u": 'admin', "p": 'admin'}
        self.header = {'Connection': 'close'}

    def upload_to_db(self, data):
        """Upload data in Line Format to InfluxDB database, returns True if uploaded, otherwise False """
        rest_response = requests.request("POST", self.url, headers=self.header, data=data, params=self.request_params)
        status_code = rest_response.status_code
        response = rest_response.content.decode("utf-8")
        if not (status_code == requests.codes.ok) and not (status_code == requests.codes.no_content):
            return False, response, status_code
        return True, response, status_code



class ElasticSearchUploader(DBUploader):
    def __init__(self, index_list, db_address='0.0.0.0', db_port=9200, lock=Manager().Lock()):
        super().__init__(self, db_address, db_port, lock)
        self.index_list = index_list
        self.db_url = f"http://{self.db_address}:{self.db_port}"

    def upload_to_db(self, data):
        """Upload data in JSON format to Elasticsearch database, returns True if uploaded, otherwise False """
        for key in data.keys():
            index = key.replace('/','-').lower()
            index_url = f"{self.db_url}/{index}"
            if not index in index_list:
                with self.lock:
                    if not index in index_list:
                        print('Acciqured lock to put index in elasticsearch')
                        headers = {'Content-Type': "application/json"}
                        mapping = {"settings": {"index.mapping.total_fields.limit": 2000},"mappings": {"nodes": {
                            "properties": {"type": {"type": "keyword"},"keys": {"type": "object"},"content": {"type": "object"},"timestamp": {"type": "date"}}}}}
                        index_put_response = request("PUT", index_url, headers=headers, json=mapping)
                        if not index_put_response.status_code == 200:
                            print("Error when creating index")
                            print(index_put_response.status_code)
                            print(index_put_response.json())
                            return False
                        index_list.append(index)
            data_url = f"http://{db_url}/_bulk"
            segment_list = sorted_by_index[key]
            elastic_index = {'index': {'_index': f'{index}', '_type': 'nodes'}}
            payload_list = []
            payload_list.append(elastic_index)
            for segment in segment_list:
                payload_list.append(segment)
                payload_list.append(elastic_index)
                payload_list.pop()
                data_to_post = '\n'.join(json.dumps(d) for d in payload_list)
                data_to_post += '\n'
                headers = {'Content-Type': "application/x-ndjson"}
                reply = request("POST", data_url, data=data_to_post, headers=headers)
                if reply.json()['errors']:
                    print("Error while uploading in bulk")
                    exit(0)
