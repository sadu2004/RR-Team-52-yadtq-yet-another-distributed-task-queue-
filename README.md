# RR-Team-52-yadtq-yet-another-distributed-task-queue-
 Distributed task queue using kafka and redis

Commit-1 creates a simple simulation for 1 worker where the client submits a simple addition task. The worker performs the said tasks in a few seconds and client's query is returned as a successful response from the redis backend.


Commit-2 : Decoupled the kafka broker from the workers and clients. Added support for multiple clients and workers. Added functionality for heartbeats for workers.

Commit-5: Used round robin scheduling to ensure better load distribution
Implemented round robin scheduling instead of hashing and manually assigning partitions thereby distributing the tasks uniformly among available workers. This was accomplished using kafka's round-robin partitioners and assigners. Added support for monitoring workers that are running.
Implemented robust task retry mechanism along with logging.

Commit-41: Added worker Monitoring to check on their current task, status, and last_heartbeat.
