from multi_process_logging import  MultiProcessQueueLoggingListner, MulitProcessQueueLogger
from multiprocessing import Process, Queue

def child(i, q):
    ql = MulitProcessQueueLogger('test.log', q)
    for y in range(i):
        ql.logger.info(y)

def main():
    log_queue = Queue()
    ll = MultiProcessQueueLoggingListner('test.log', log_queue)
    ll.start()
    p1 = Process(target=child, args=(10,log_queue,))
    p1.start()
    p2 = Process(target=child, args=(10,log_queue,))
    p2.start()
    p3 = Process(target=child, args=(10,log_queue,))
    p3.start()
    p1.join()
    p2.join()
    p3.join()
    log_queue.put(None)
    ll.join()

    



if __name__ == '__main__':
    main()
