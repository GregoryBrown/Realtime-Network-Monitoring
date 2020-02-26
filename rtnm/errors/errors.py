"""
Global Real Time Network Monitoring exception and warning classes.
"""


class Error(Exception):
    pass


class TelemetryTCPDialOutServerError(Error):
    """Generic Error for TCP Dial out server"""
    pass



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
