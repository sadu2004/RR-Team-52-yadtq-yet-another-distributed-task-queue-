# monitor.py

from yadtq import WorkerMonitor
import time
import datetime
import threading
import sys

def main():
    monitor = WorkerMonitor()
    print(f"[{datetime.now()}] Starting worker monitor")
    try:
        monitor.start_monitoring()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"[{datetime.now()}] Shutting down worker monitor")
        monitor.shutdown()
        sys.exit(0)

if __name__ == '__main__':
    main()
