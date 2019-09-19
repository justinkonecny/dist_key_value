# Distributed Key Value Store

The high-level approach was to first implement heartbeat timeouts and election timeouts. Once elections were working correctly, we were able to start implementing the Raft functionality. From a general overview, all client requests are redirected to the current leader. The current leader immediately responds to any get request with the appropriate value from its 'durable' data store. 

All put requests are first added to the log, where they are sent out to all replicas. Any replica that correctly appends the entry to its log, responds to the leader with a success message. If the leader reaches quorum, the leader adds the entry to its durable data store and increments its commit index. The leader's commit index is always sent with AppendEntry messages, and replicas process their log and commit entries as 'durable', according to the given leader index. Upon leader death, a new election will occur after a timeout, and the process will continue as normal.
