# Distributed Fault-Tolerant Task Scheduler

A master-worker architecture built with TCP sockets designed for high reliability and observability.

## Key Technical Features
* Worker Fault Tolerance: Implements a persistent heartbeat mechanism to detect crashes and automatically reassign orphaned tasks.
* Chandy-Lamport Snapshot: Captures consistent global system states (queues and worker statuses) without halting computation.
* Hybrid Bully-Raft Consensus: A custom leader election algorithm that ensures linear message complexity and prevents "split-brain" scenarios.
