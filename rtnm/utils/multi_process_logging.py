import logging
from logging.handlers import RotatingFileHandler, QueueHandler
from multiprocessing import Process


class MultiProcessQueueLoggingListner(Process):
    def __init__(self, name, queue):
        super().__init__(name=name)
        self.log_name = name
        self.queue = queue

    def run(self):
        self.configure()
        while True:
            try:
                record = self.queue.get()
                if record is None:
                    break
                self.logger = logging.getLogger(record.name)
                self.logger.handle(record)
            except Exception:
                import sys, traceback

                print("Problem:", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)

    def configure(self):
        self.logger = logging.getLogger(self.log_name)
        self.file_handler = RotatingFileHandler(self.log_name, maxBytes=536870912, backupCount=2)
        self.screen_handler = logging.StreamHandler()
        self.formatter = logging.Formatter("%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s")
        self.file_handler.setFormatter(self.formatter)
        self.screen_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)
        self.logger.addHandler(self.screen_handler)


class MultiProcessQueueLogger(object):
    def __init__(self, name, queue):
        self.name = name
        self.queue = queue
        self.queue_handler = QueueHandler(queue)
        self.logger = logging.getLogger(name)
        self.logger.addHandler(self.queue_handler)
        self.logger.setLevel(logging.DEBUG)
