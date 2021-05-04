class TelemetryTCPDialOutServerError(Exception):
    """Generic Error for TCP Dial out server"""
    pass


class IODefinedError(Exception):
    """User didn't define an input and output in the configuration file"""
    pass


class DecodeError(Exception):
    pass


class ElasticSearchUploaderError(Exception):
    pass


class ConfigError(Exception):
    pass
