import json
from requests import request
from utils.utils import process_batch_list

class ElasticSearchUploader(object):
    def __init__(self, elastic_server, elastic_port, batch_size, lock, log, index_list, gnmi):
        self.elastic_server = elastic_server
        self.elastic_port = elastic_port
        self.batch_size = batch_size
        self.lock = lock
        self.url = f"http://{self.elastic_server}:{self.elastic_port}"
        self.log = log
        self.index_list = index_list
        self.gnmi = gnmi
    def populate_index_list(self):
        try:
            self.log.info("Populating index list")
            get_all_sensors_response = request("GET", f"{self.url}/*")
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

    def elasticsearch_upload(self, batch_list):
        sorted_by_index = {}
        try:
            converted_decode_segments = process_batch_list(batch_list, self.gnmi)
            # Sort all segments by index
            for converted_decode_segment in converted_decode_segments:
                if not converted_decode_segment["_index"] in sorted_by_index.keys():
                    sorted_by_index[converted_decode_segment["_index"]] = [converted_decode_segment]
                else:
                    sorted_by_index[converted_decode_segment["_index"]].append(converted_decode_segment)
            # Bulk upload each index to elasticsearch
            for index in sorted_by_index.keys():
                if index not in self.index_list:
                    self.log.info('Acquired lock to put index in elasticsearch')
                    with self.lock:
                        index_list = self.populate_index_list()
                        if index_list == False:
                            self.log.error("Unable to repopulate index list")
                            return False
                        if index not in index_list:
                            index_url = f"{self.url}/{index}"
                            headers = {'Content-Type': "application/json"}
                            mapping = {"mappings": {"properties": {"@timestamp": {"type": "date"}}}}
                            index_put_response = request("PUT", index_url, headers=headers, json=mapping)
                        if not index_put_response.status_code == 200:
                            self.log.error("Error when creating index")
                            self.log.error(index_put_response.status_code)
                            self.log.error(index_put_response.json())
                            return False
                        index_list.append(index)    
                        
                data_url = f"{self.url}/_bulk"
                segment_list = sorted_by_index[index]
                elastic_index = {'index': {'_index': f'{index}'}}
                payload_list = [elastic_index]
                for segment in segment_list:
                    segment.pop('_index', None)
                    payload_list.append(segment)
                    payload_list.append(elastic_index)
                payload_list.pop()
                data_to_post = '\n'.join(json.dumps(d) for d in payload_list)
                data_to_post += '\n'
                headers = {'Content-Type': "application/x-ndjson"}
                reply = request("POST", data_url, data=data_to_post, headers=headers)
                if reply.json()['errors']:
                    self.log.error("Error while uploading in bulk")
                    self.log.error(reply.json())
                    return False
            return True
        except Exception as e:
            print(e)
