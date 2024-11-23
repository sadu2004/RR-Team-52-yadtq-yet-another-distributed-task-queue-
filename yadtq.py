# yadtq.py
from kafka.admin import KafkaAdminClient, NewTopic
import uuid
import json
import threading
import time
import hashlib
from kafka import KafkaProducer, KafkaConsumer
import redis
from kafka.partitioner.roundrobin import RoundRobinPartitioner
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
import logging
from kafka.admin import KafkaAdminClient, NewPartitions
import time
# from kafka import KafkaConsumer
# from kafka.errors import (
#     LeaderNotAvailableError, KafkaConnectionError, 
#     NotLeaderForPartitionError, UnknownTopicOrPartitionError
# )
# RETRY_ERROR_TYPES = (
#     LeaderNotAvailableError, KafkaConnectionError,
#     NotLeaderForPartitionError, UnknownTopicOrPartitionError
# )
admin_client=KafkaAdminClient(bootstrap_servers='localhost:9092')
# monitor.py

# import redis
# import threading
# import time
# import datetime
# import json

# class WorkerMonitor:
#     def __init__(self, redis_host='localhost', redis_port=6379):
#         self.redis_client = redis.Redis(
#             host=redis_host,
#             port=redis_port,
#             db=0,
#             decode_responses=True
#         )
#         self.running = False
#         self.monitor_thread = None
#         self.heartbeat_timeout = 30  # seconds

#     def start_monitoring(self):
#         """Start the monitoring thread"""
#         self.running = True
#         self.monitor_thread = threading.Thread(target=self._monitor_loop)
#         self.monitor_thread.daemon = True
#         self.monitor_thread.start()

#     def shutdown(self):
#         """Shutdown the monitor gracefully"""
#         self.running = False
#         if self.monitor_thread:
#             self.monitor_thread.join()
#         self.redis_client.close()

#     def _monitor_loop(self):
#         """Main monitoring loop"""
#         while self.running:
#             self._check_worker_heartbeats()
#             self._update_statistics()
#             time.sleep(5)

#     def _check_worker_heartbeats(self):
#         """Check for worker heartbeats and detect failures"""
#         current_time = time.time()
#         workers = self.redis_client.hgetall('worker_heartbeats')
       
#         for worker_id, last_heartbeat in workers.items():
#             try:
#                 last_heartbeat = float(last_heartbeat)
#                 if current_time - last_heartbeat > self.heartbeat_timeout:
#                     self._record_worker_failure(worker_id, last_heartbeat)
#             except ValueError:
#                 continue

#     def _record_worker_failure(self, worker_id, last_heartbeat):
#         """Record worker failure in history"""
#         failure_info = {
#             'worker_id': worker_id,
#             'last_heartbeat': datetime.datetime.fromtimestamp(last_heartbeat).isoformat(),
#             'failed_at': datetime.datetime.now().isoformat()
#         }
#         self.redis_client.lpush('worker_failure_history', json.dumps(failure_info))
#         self.redis_client.hdel('worker_heartbeats', worker_id)

#     def _update_statistics(self):
#         """Update worker and task statistics"""
#         active_workers = self.redis_client.hgetall('worker_heartbeats')
#         for worker_id in active_workers:
#             tasks_completed = self.redis_client.get(f'worker:{worker_id}:tasks_completed') or 0
#             self.redis_client.hset('worker_statistics', worker_id, tasks_completed)

#     def get_tasks_in_queue(self):
#         """Get list of tasks waiting in queue"""
#         return self.redis_client.lrange('tasks_queue', 0, -1)

#     def get_tasks_in_progress(self):
#         """Get list of tasks currently being processed"""
#         return self.redis_client.lrange('tasks_in_progress', 0, -1)

#     def get_worker_statistics(self):
#         """Get worker performance statistics"""
#         return self.redis_client.hgetall('worker_statistics')

#     def get_worker_failure_history(self):
#         """Get history of worker failures"""
#         return self.redis_client.lrange('worker_failure_history', 0, -1)

#     def update_worker_heartbeat(self, worker_id):
#         """Update worker heartbeat timestamp"""
#         self.redis_client.hset('worker_heartbeats', worker_id, time.time())

#     def increment_worker_tasks(self, worker_id):
#         """Increment completed tasks counter for worker"""
#         self.redis_client.incr(f'worker:{worker_id}:tasks_completed')
def create_topic(topic_name, num_partitions=6, replication_factor=1):
        """
        Create a Kafka topic if it does not already exist.
        """
        try:
            # List existing topics
            existing_topics = admin_client.list_topics()
            if topic_name in existing_topics:
                print(f"Topic '{topic_name}' already exists.")
                return

            # Create the topic
            new_topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            admin_client.create_topics([new_topic])
            print(f"Topic '{topic_name}' created successfully.")
        except Exception as e:
            print(f"Failed to create topic '{topic_name}': {e}")

class YADTQ:
    def __init__(self, kafka_bootstrap_servers='localhost:9092', redis_host='localhost', redis_port=6379):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.redis_host = redis_host
        self.redis_port = redis_port
        create_topic('tasksqueue3')
        self.task_topic = 'tasksqueue3'
        print(self.task_topic)
        self.heartbeat_topic = 'heartbeats'
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
        self.producer = KafkaProducer(
        bootstrap_servers=self.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        partitioner=RoundRobinPartitioner()
    )
    def update_partitions(self, num_partitions):
        """
        Update the number of partitions for the given topic dynamically and print topic information for debugging.
        """
        try:
            # Describe topics and get the response
            topic_info = admin_client.describe_topics([self.task_topic])

            # Print the topic_info to inspect its structure (already seen from your response)
            # print(f"Topic information for '{self.task_topic}': {topic_info}")

            # Ensure topic_info is not empty and access the first dictionary in the list
            if not topic_info or not isinstance(topic_info, list) or len(topic_info) == 0:
                print(f"Could not retrieve information for topic '{self.task_topic}'.")
                return

            # Access the first element of the list (assuming it corresponds to your topic)
            topic_data = topic_info[0]

            # Extract and print the current number of partitions
            partitions_info = topic_data.get('partitions', [])
            current_partitions = len(partitions_info)
            # print(f"Current number of partitions for topic '{self.task_topic}': {current_partitions}")

            if num_partitions > current_partitions:
                admin_client.create_partitions({
                    self.task_topic: NewPartitions(total_count=num_partitions)
                })
                print(f"Updated topic '{self.task_topic}' to {num_partitions} partitions.")
            else:
                print(f"Topic '{self.task_topic}' already has {current_partitions} partitions.")
        except Exception as e:
            print(f"Failed to update partitions for topic '{self.task_topic}': {e}")

    def track_heartbeats(self, timeout=10):
        consumer = KafkaConsumer(
        self.heartbeat_topic,
        bootstrap_servers=self.kafka_bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        worker_status = {}  # {worker_id: last_heartbeat_time}

        print("Started heartbeat monitoring...")

        while True:
            # Poll messages with a timeout (e.g., 1000ms)
            messages = consumer.poll(timeout_ms=1000)

            # Process incoming heartbeats
            for topic_partition, records in messages.items():
                for record in records:
                    # print(record.value)
                    heartbeat = record.value
                    worker_id = heartbeat['worker_id']
                    timestamp = heartbeat['timestamp']

                    # Update last heartbeat time for the worker
                    worker_status[worker_id] = timestamp
                    self.redis_client.hset('worker_status', worker_id, 'active')
                    self.redis_client.expire(f"worker:{worker_id}:heartbeat", timeout)
                    # print(worker_status)

            # Detect failed workers
            current_time = time.time()
            failed_workers = []

            for worker_id, last_heartbeat in list(worker_status.items()):
                if current_time - last_heartbeat > timeout:  # Worker timeout
                    failed_workers.append(worker_id)

            # Handle the failed workers
            for worker_id in failed_workers:
                print(f"Worker {worker_id} failed. Requeuing tasks...")
                worker_status.pop(worker_id)  # Remove from active workers
                self.redis_client.hset('worker_status', worker_id, 'failed')  # Mark worker as failed

                # Requeue tasks assigned to the failed worker
                for task_id in self.redis_client.scan_iter():
                    # task_info = self.redis_client.hgetall(task_id)
                    if self.redis_client.type(task_id) == 'hash':
                        task_info = self.redis_client.hgetall(task_id)
                    else:
                        # print(f"Warning: Task {task_id} is not stored as a hash in Redis")
                        continue# Check if key exists and is a hash before getting all fields
                    
                    if task_info.get('status') == 'processing' and task_info.get('worker') == worker_id:
                        # Requeue the task
                        task = {
                            'task_id': task_id,
                            'task': task_info['task'],
                            'args': json.loads(task_info['args'])
                        }
                        self.producer.send(self.task_topic, task)
                        self.redis_client.hset(task_id, mapping={'status': 'queued', 'worker': ''})
                        print(f"Task {task_id} requeued")

            # Add a small sleep to avoid busy looping
            active_worker_count=len(worker_status)
            # print(worker_status)
            self.redis_client.set('active_worker_count',active_worker_count)
            self.update_partitions(active_worker_count+3)
            time.sleep(1)


class YADTQClient(YADTQ):
    def __init__(self, *args, **kwargs):
        super(YADTQClient, self).__init__(*args, **kwargs)
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            partitioner=RoundRobinPartitioner()
       )
    # def _get_partition(self, key):
    #     """Returns the partition number for a given key (task_id or worker_id)."""
    #     hash_value = hashlib.md5(key.encode('utf-8')).hexdigest()  # Hash the key
    #     partition = int(hash_value, 16) % 6  # Modulo with number of partitions (e.g., 6)
    #     return partition
    def submit_task(self, task_name, args):
        task_id = str(uuid.uuid4())
        task = {
            'task_id': task_id,
            'task': task_name,
            'args': args
        }
        # partition_no=self._get_partition(task_id)
        # Store initial status in Redis
        self.redis_client.hset(task_id, mapping={'status': 'queued'})
        # Send task to Kafka
        self.producer.send(self.task_topic, task)
        self.producer.flush()
        return task_id

    def get_task_status(self, task_id):
        status_info = self.redis_client.hgetall(task_id)
        if status_info:
            return status_info
        else:
            return {'status': 'unknown'}

class YADTQWorker(YADTQ):
    def __init__(self, group_id, *args, **kwargs):
        super(YADTQWorker, self).__init__(*args, **kwargs)
        self.group_id = group_id
        self.consumer = KafkaConsumer(
            self.task_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=self.group_id,
            enable_auto_commit=False,
            heartbeat_interval_ms=3000,
            session_timeout_ms=10000,
            partition_assignment_strategy=[RoundRobinPartitionAssignor]
        )
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.worker_id = str(uuid.uuid4())
        self.stop_event = threading.Event()

    def send_heartbeat(self):
        while not self.stop_event.is_set():
            heartbeat = {'worker_id': self.worker_id, 'timestamp': time.time()}
            self.producer.send(self.heartbeat_topic, heartbeat)
            print("heartbeat sent")
            self.producer.flush()
            time.sleep(5)  # Send heartbeat every 5 seconds

    def start_heartbeat(self):
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.start()

    def stop_heartbeat(self):
        self.stop_event.set()
        self.heartbeat_thread.join()
    # def process_message(self,message,task_functions):
    #     task = message.value
    #     task_id = task['task_id']
    #     task_name = task['task']
    #     args = task['args']
    #     logging.info(f"Worker {self.worker_id} processing task {task_id}: {task_name}({args})")
    #     self.redis_client.hset(task_id, mapping={'status': 'processing'})
        
    #                 # Execute the task
    #     func = task_functions.get(task_name)
    #     if func:
    #         result = func(*args)
    #                     # Update status to success and store result
    #         self.redis_client.hset(task_id, mapping={'status': 'success', 'result': str(result)})
    #                     # Clear retry count after success
    
    #     else:
    #                     # Task not found
    #         raise ValueError(f"Unknown task '{task_name}'")
    #     return func(*args)
    
    # def handle_retry(self,e, task, task_id, max_retries, retry_delay):
    #     retry_count_key = f"task:{task_id}:retries"
    #     retry_count = int(self.redis_client.get(retry_count_key) or 0) + 1
    #     self.redis_client.set(retry_count_key, retry_count)

    #     if retry_count <= max_retries:
    #         logging.warning(f"Task {task_id} failed ({retry_count}/{max_retries}). Retrying in {retry_delay}s...")
    #         time.sleep(retry_delay)
    #         self.producer.send(self.task_topic, task)
    #         self.producer.flush()
    #         self.redis_client.hset(task_id, mapping={'status': 'queued'})
    #     else:
    #         logging.error(f"Task {task_id} permanently failed after {max_retries} retries.")
    #         self.redis_client.hset(task_id, mapping={'status': 'failed', 'error': str(e)})
    # def consume_with_retries(self,task_functions, max_retries=5, retry_backoff=1):
        
    #     self.start_heartbeat()
    #     while True:
    #         try:
    #             for message in self.consumer:
    #                 task = message.value  # Extract task from message
    #                 task_id = task['task_id']  # Unique task ID
    #                 retry_count_key = f"task:{task_id}:retries"
    #                 try:

    #                     self.process_message(message,task_functions)
    #                 except RETRY_ERROR_TYPES as e:
    #                     self.handle_retry(e,task, task_id, max_retries, retry_backoff)
    #                 except Exception as e:
    #                     logging.error(f"Task {task_id} has a non -retryable error")
    #                     self.redis_client.hset(task_id, mapping={'status': 'failed', 'error': str(e)})
    #                     print(f"Non-retryable error occurred: {e}")
    #                 self.consumer.commit()
    #         except RETRY_ERROR_TYPES as e:
    #             print(f"Retryable error in consumer loop: {e}. Retrying connection...")
    #             time.sleep(retry_backoff)
    #         except Exception as e:
    #             print(f"Non-retryable consumer error: {e}")
    #             break
    #         finally:
    #             self.stop_heartbeat()

        

    def consume_tasks(self, task_functions, max_retries=3, retry_delay=1):

        """
        Consume tasks from Kafka and execute them with a retry mechanism.

        task_functions: a dictionary mapping task names to functions.
        max_retries: Maximum number of retries for failed tasks.
        retry_delay: Time (in seconds) to wait before retrying a failed task.
        """
        self.start_heartbeat()
        try:
            for message in self.consumer:
                task = message.value
                task_id = task['task_id']
                print(task_id)
                task_name = task['task']
                args = task['args']

                logging.info(f"Worker {self.worker_id} processing task {task_id}: {task_name}({args})")
                print(f"Worker {self.worker_id} processing task {task_id}: {task_name}({args})")
                # Update status to processing
                self.redis_client.hset(task_id, mapping={'status': 'processing'})

                retry_count_key = f"task:{task_id}:retries"
                try:
                    # Execute the task
                    func = task_functions.get(task_name)
                    if func:
                        result = func(*args)
                        # Update status to success and store result
                        self.redis_client.hset(task_id, mapping={'status': 'success', 'result': str(result)})
                        # Clear retry count after success
                        self.redis_client.delete(retry_count_key)
                    else:
                        # Task not found
                        raise ValueError(f"Unknown task '{task_name}'")
                except Exception as e:
                    # Increment retry count
                    retry_count = int(self.redis_client.hget(retry_count_key, 'count') or 0) + 1
                    self.redis_client.hset(retry_count_key, mapping={'count': retry_count})

                    if retry_count <= max_retries:
                        # Requeue the task
                        logging.warning(f"Task {task_id} failed ({retry_count}/{max_retries}). Retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                        self.producer.send(self.task_topic, task)
                        self.producer.flush()
                        self.redis_client.hset(task_id, mapping={'status': 'queued'})
                    else:
                        # Mark as permanently failed
                        logging.error(f"Task {task_id} permanently failed after {max_retries} retries.")
                        self.redis_client.hset(task_id, mapping={'status': 'failed', 'error': str(e)})

                # Manually commit the message
                self.consumer.commit()
        except Exception as e:
            logging.critical(f"Worker encountered an error: {e}")
        finally:
            self.stop_heartbeat()
