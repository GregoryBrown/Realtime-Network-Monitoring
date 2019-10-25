"""
Global Real Time Network Monitoring exception and warning classes.
"""
class Error(Exception):
    pass

class GetIndexListError(Error):
    """Error while doing a Get request against ES"""
    def __init__(self, code, response_json, message):
        self.code = code 
        self.response = response_json
        self.message = message

class PostDataError(Error):
    """Error Posting data to ES"""
    def	__init__(self, code, response_json, data, message):
        self.code = code
        self.response =	response_json
        self.data = data
        self.message = message


class PutIndexError(Error):
    """Error Putting an index into ES"""
    def	__init__(self, code, response_json, index, message):
        self.code = code
        self.response =	response_json
        self.index = index
        self.message = message


class FormatDataError(Error):
    """Formatting data failed with error"""
    pass

class DeviceFailedToConnect(Error):
    """The device failed to connect"""
    pass

class DeviceDisconnected(Error):
    """The device is disconnected"""
    pass

class IODefinedError(Error):
    """User didn't define an input and output in the configuration file"""
    pass

class DecodeError(Error):
    """Error while Decoding telemetry data"""
    def __init__(self, item, full_data, message):
        self.item = item
        self.full_data = full_data
        self.message = message
