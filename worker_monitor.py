import time
from yadtq import YADTQ
import threading
from kafka import KafkaConsumer
import json
class WorkerMonitor(YADTQ):
    def _init_(self, *args, **kwargs):
        super(WorkerMonitor, self)._init_(*args, **kwargs)
        self.consumer = KafkaConsumer(
            self.heartbeat_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=1000
        )
        self.worker_last_seen = {}
        self.active_workers = set()
        self.stop_event = threading.Event()

    def start_monitoring(self):
        threading.Thread(target=self.monitor_heartbeats).start()

    def monitor_heartbeats(self):
        while not self.stop_event.is_set():
            # Consume heartbeats
            for message in self.consumer:
                heartbeat = message.value
                worker_id = heartbeat['worker_id']
                timestamp = heartbeat['timestamp']
                self.worker_last_seen[worker_id] = timestamp
                self.active_workers.add(worker_id)

            # Check for worker timeouts
            now = time.time()
            for worker_id in list(self.active_workers):
                last_seen = self.worker_last_seen.get(worker_id, 0)
                if now - last_seen > self.worker_timeout:
                    print(f"[{time.time()}] Worker {worker_id} is considered dead.")
                    self.active_workers.discard(worker_id)
                    # Handle reassignment of tasks if necessary
                    # For this implementation, Kafka will handle rebalancing

            time.sleep(5)

    def shutdown(self):
        self.stop_event.set()
        self.consumer.close()