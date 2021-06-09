from logging import getLogger, Logger, LogRecord, StreamHandler, Formatter, INFO, DEBUG
from logging.handlers import RotatingFileHandler, QueueHandler
from multiprocessing import Process, Queue
from pathlib import Path
from typing import Tuple


class RTNMRotatingFileHandler(RotatingFileHandler):
    def __init__(self, filename: str, mode: str = "a", maxBytes: int = 0, backupCount: int = 0, encoding: str = None, delay: bool = False):
        RotatingFileHandler.__init__(self, filename, maxBytes=maxBytes, backupCount=backupCount)


class MultiProcessQueueLogListener(Process):
    def __init__(self, name: str, path: Path, queue: Queue):
        super().__init__(name=f"{name}-log-listener")
        self.log_name: str = name
        self.path: Path = path
        self.path.mkdir(exist_ok=True)
        self.queue: Queue = queue

    def run(self):
        self.configure()
        rc: bool = True
        while rc:
            try:
                record: LogRecord = self.queue.get()
                if record is None:
                    rc = False
                else:
                    self.logger = getLogger(record.name)
                    self.logger.handle(record)
            except Exception as e:
                import sys
                import traceback
                if self.logger:
                    self.logger.error(traceback.print_exc())
                else:
                    print("Problem:", file=sys.stderr)
                    traceback.print_exc(file=sys.stderr)
                rc = False

    def configure(self):
        self.logger: Logger = getLogger(self.log_name)
        file_handler: RTNMRotatingFileHandler = RTNMRotatingFileHandler(f"{self.path}/{self.log_name}.log", maxBytes=536870912, backupCount=5)
        screen_handler: StreamHandler = StreamHandler()
        formatter: Formatter = Formatter(
            "[%(process)s-%(processName)s][%(threadName)-10s] %(asctime)s - %(name)20s - [%(levelname)7s] - %(message)s")
        file_handler.setFormatter(formatter)
        screen_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(screen_handler)


class MultiProcessQueueLogger(object):
    def __init__(self, name: str, queue: Queue, debug: bool = False):
        self.name: str = name
        self.queue: Queue = queue
        self.queue_handler: QueueHandler = QueueHandler(queue)
        self.logger: Logger = getLogger(name)
        self.logger.addHandler(self.queue_handler)
        if debug:
            self.logger.setLevel(DEBUG)
        else:
            self.logger.setLevel(INFO)


def init_logs(name, path: Path, queue: Queue, debug: bool = False) -> Tuple[MultiProcessQueueLogListener, MultiProcessQueueLogger]:
    log_listener: MultiProcessQueueLogListener = MultiProcessQueueLogListener(name, path, queue)
    log_listener.start()
    main_logger: MultiProcessQueueLogger = MultiProcessQueueLogger(name, queue, debug)
    return main_logger
