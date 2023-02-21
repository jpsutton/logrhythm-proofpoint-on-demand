#!/usr/bin/env python

import time
import configparser
import traceback
from multiprocessing import Process, Queue, Value

from ppod_client import ppod_client
from syslog_client import send_logs


QUEUE_CHECK_INTERVAL = 300
WEBHOOK_CLIENT_COUNT = 5

def main():
    counter = 0
    config = configparser.ConfigParser()
    config.read("config.ini")
    process_list = list()

    log_queue = Queue()
    exit_flag = Value('B')
    exit_flag.value = 0

    for i in range(WEBHOOK_CLIENT_COUNT):
        # Start the webhook client first so that logs don't flood the queue without being able to be forwarded
        w_client = Process(target=send_logs, args=(config['syslog_client']['SERVER'], log_queue, exit_flag))
        w_client.start()
        process_list.append(w_client)

    # Start the PPoD client to get new logs
    p_client = Process(target=ppod_client, args=(log_queue, config['ppod_client']['API_KEY'], exit_flag))
    p_client.start()
    process_list.append(p_client)

    try:
        while True:
            time.sleep(1)
            counter += 1
            
            if counter > QUEUE_CHECK_INTERVAL:
                counter = 0
                print(f"[Main thread] Queue size is approximately {log_queue.qsize()} messages.")

    except KeyboardInterrupt:
        pass
    except:
        traceback.print_exc()
    finally:
        exit_flag.value = 1

        for client in process_list:
            client.join(timeout=2)

            if client.is_alive():
                client.kill()


if __name__ == '__main__':
    main()
