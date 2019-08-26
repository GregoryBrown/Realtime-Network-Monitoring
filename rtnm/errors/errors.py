class Error(Exception):
    pass

class GetIndexListError(Error):
    def __init__(self, code, response_json, message, traceback):
        self.code = code 
        self.response = response_json
        self.message = message
        self.traceback = traceback

class PostDataError(Error):
    def	__init__(self, code, response_json, data, message, traceback):
        self.code = code
        self.response =	response_json
        self.data = data
        self.message = message
        self.traceback = traceback


class PutIndexError(Error):
    def	__init__(self, code, response_json, index, message, traceback):
        self.code = code
        self.response =	response_json
        self.index = index
        self.message = message
        self.traceback = traceback
