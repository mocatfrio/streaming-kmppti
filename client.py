import time
import multiprocessing
import multiprocessing.managers

from streaming_kmppti.const import *

# import logging
# logger = multiprocessing.log_to_stderr()
# logger.setLevel(logging.INFO)

class MyListManager(multiprocessing.managers.BaseManager):
    pass

MyListManager.register("streaming_server")

def main():
    manager = MyListManager(address=('/tmp/mypipe'), authkey=b"")
    manager.connect()
    shared_data = manager.streaming_server()

    print("shared queue = ", shared_data[QUEUE_START_INDEX:])
    print("shared index = ", shared_data[METADATA_INDEX]["index"])
    print("shared label = ", shared_data[METADATA_INDEX]["label"])

    while True:
        print("=============================")
        print("shared queue = ", shared_data[QUEUE_START_INDEX:])
        time.sleep(TIME_SLEEP)
        
if __name__ == '__main__':
    main()