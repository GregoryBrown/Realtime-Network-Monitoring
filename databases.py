import requests


class DBUploader(object):
    def upload_to_db(self, data):
        pass


class InfluxDBUploader(DBUploader):
    def __init__(self, db_address, db_port, db_name):
        self.address = db_address
        self.port = db_port
        self.db = db_name
        self.url = f"http://{self.address}:{self.port}/write"
        self.request_params = {"db": self.db}
        self.header = {'Connection': 'close'}

    def upload_to_db(self, data):
        """Upload data in Line Format to InfluxDB database, returns True if uploaded, otherwise False """
        rest_response = requests.request("POST", self.url, headers=self.header, data=data, params=self.request_params)
        status_code = rest_response.status_code
        response = rest_response.content.decode("utf-8")
        if not (status_code == requests.codes.ok) and not (status_code == requests.codes.no_content):
            return False, response, status_code
        return True, response, status_code
