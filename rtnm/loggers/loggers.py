import logging
import os
from logging.handlers import RotatingFileHandler, QueueHandler
from multiprocessing import Process, get_logger

def mkdir_p(path):
    try:
        os.makedirs(path, exist_ok=True)  
    except TypeError as e:
        raise e
    except Exception as e:
        raise e
        

    
class RTNMRotatingFileHandler(RotatingFileHandler):
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=False):
        try:
            mkdir_p(os.path.dirname(filename))
            RotatingFileHandler.__init__(self, filename, maxBytes=maxBytes, backupCount=backupCount)
        except TypeError as e:
            print(e)
            print("Can't initialize logs")
            exit(1)



def init_logs(name, path, queue, debug=False):
    log_listener = MultiProcessQueueLogListner(name, path, queue)
    log_listener.start()
    main_logger = MultiProcessQueueLogger(name, queue, debug)
    return log_listener, main_logger

class MultiProcessQueueLogListner(Process):
    def __init__(self, name, path, queue):
        super().__init__(name=name)
        self.log_name = name
        self.path = path
        self.queue = queue
    def run(self):
        self.configure()
        rc = True
        while rc:
            try:
                record = self.queue.get()
                if record is None:
                    rc = False
                else:
                    self.logger = logging.getLogger(record.name)
                    self.logger.handle(record)
            except Exception as e:
                import sys, traceback
                if self.logger:
                    self.logger.error(traceback.print_exc())
                else:
                    print('Problem:', file=sys.stderr)
                    traceback.print_exc(file=sys.stderr)
                rc = False
    
    def configure(self):
        self.logger = logging.getLogger(self.log_name)
        self.file_handler = RTNMRotatingFileHandler(f"{self.path}/{self.log_name}.log", maxBytes=536870912, backupCount=2)
        self.screen_handler = logging.StreamHandler()
        self.formatter = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
        self.file_handler.setFormatter(self.formatter)
        self.screen_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)
        self.logger.addHandler(self.screen_handler)


class MultiProcessQueueLogger(object):
    def __init__(self, name, queue, debug=False):
        self.name = name
        self.queue = queue
        self.queue_handler = QueueHandler(queue)
        self.logger = logging.getLogger(name)
        self.logger.addHandler(self.queue_handler)
        if debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)
