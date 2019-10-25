import json
from requests import request
from errors.errors import GetIndexListError, PostDataError, PutIndexError

class ElasticSearchUploader(object):
    def __init__(self, elastic_server, elastic_port, lock, log):
        self.lock = lock
        self.url = f"http://{elastic_server}:{elastic_port}"
        self.log = log
        self.index_list = []

    def put_index(self, index):
         headers = {'Content-Type': "application/json"}
         mapping = {"mappings": {"properties": {"@timestamp": {"type": "date"}}}}
         index_put_response = request("PUT", f"{self.url}/{index}", headers=headers, json=mapping)
         if not index_put_response.status_code == 200:
             raise PutIndexError(index_put_response.status_code, index_put_response.json(), index, f"PUT failed to upload {index}")
         else:
             self.index_list.append(index)

    def populate_index_list(self):
        try:
            self.log.info("Populating index list")
            get_response = request("GET", f"{self.url}/*")
            if not get_response.status_code == 200:
                raise GetIndexListError(get_response.status_code, get_response.json(), "GET failed to retrieve all indices")
            for key in get_response.json():
                if not key.startswith('.'):
                    self.index_list.append(key)
        except Exception as e:
            self.log.error(e)
            exit(1)

    def post_data(self, data):
        data_to_post = '\n'.join(json.dumps(d) for d in data)
        data_to_post += '\n'
        headers = {'Content-Type': "application/x-ndjson"}
        post_response = request("POST", f"{self.url}/_bulk", data=data_to_post, headers=headers)
        if post_response.json()['errors']:
            raise PostDataError(post_response.status_code, post_response.json(), "POST failed to upload data")
    
    def upload(self, data_list):
        try:
            self.populate_index_list()
        except GetIndexListError as e:
            self.log.error(e)
            exit(1)
        except Exception as e:
            self.log.error(e)
            exit(1)
        sorted_by_index = {}
        for data in data_list:
            if not data["_index"] in sorted_by_index.keys():
                sorted_by_index[data["_index"]] = [data]
            else:
                sorted_by_index[data["_index"]].append(data)
        for index in sorted_by_index.keys():
            if index not in self.index_list:
                self.log.info('Acquired lock to put index in elasticsearch')
                with self.lock:
                    try:
                        self.populate_index_list()
                    except GetIndexListError as e:
                        self.log.error(e)
                        exit(1)
                    except Exception as e:
                        self.log.error(e)
                        exit(1)
                    if index not in self.index_list:
                        try:
                            self.put_index(index)
                        except PutIndexError as e:
                            self.log.error(e)
                            exit(1)
            segment_list = sorted_by_index[index]
            elastic_index = {'index': {'_index': f'{index}'}}
            payload_list = [elastic_index]
            for segment in segment_list:
                segment.pop('_index', None)
                payload_list.append(segment)
                payload_list.append(elastic_index)
            payload_list.pop()
            try:
                self.post_data(payload_list)
            except PostDataError as e:
                self.log.error(e)
                exit(1)
