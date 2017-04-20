## Abstract
Hinted Handoff is a feature that allows replaying failed writes. The mutation and the destination replica are saved in a log and replayed later according to the feature configuration. 

## Hinted Handoff Configuration Parameters 
 * _hinted_handoff_enabled_: Enables or disables the Hinted Handoff feature completely or enumerate DCs for which hints are allowed.
 * _max_hint_window_in_ms_: Don't generate hints if the destination Node has been down for more than this value. The hints should resume once the Node is seen up (and went down).
 * _hints_directory_: Directory where scylla will store hints. By default `$SCYLLA_HOME/hints`
 * _hints_compression_: Compression to apply to hints files. By default, hints files are stored uncompressed.
 
## Future configuration
 * We should define the fairness configuration between the regular WRITES and hints WRITES.
   Since we don't have either CPU scheduler or (and) Network scheduler at the moment we can't give any guarantee regarding the runtime and/or networking bandwidth fairness.
   Once we have tools to define it we should give the hints sender some low runtime priority and some small share of the bandwidth.

## Hints generation
 * Once the WRITE mutation fails with a time out we create a _hints_queue_ for this Node.
   * The queue is specified by a destination Node IP.

 * Each hint is specified by:
   * Mutation.

Hints are appended to the _hints_queue_ until (all this should be done using the existing or slightly modified _commitlog_ API):
 * The destination Node is DOWN for more than _max_hint_window_in_ms_ time.

As long as hints are appended to the queue the files are closed and flushed to the disk once they reach the maximum allowed size (32MB) or when the queue is forcefully flushed (see "Hints sending" below).

We are going to reuse the commitlog infrastructure for wrinting hints to disk - it provides both the internal buffering and the memory consumption control.

Hints to the specific destination are going to be stored under the _hints_directory_/\<shard ID>/\<Node IP> directory.

We are not going to save a new hint if there are more than 128 hints that are pending to be stored to their queues on the current shard. 

### Redistribution of hints when Node boots.
 * When Node boots all present hints files are redistributed equally between all present shards.

## Hints sending
 * Hints are sent from each shard by each _hints_queue_ independently.
 * Each shard sends the hints that it owns (according to the hint file location).
 * Hints sending is triggered by the following events: 
   * Timer: every X seconds (Origin does it every 10s).
     * For each queue:
       * Forcefully close the queues.
       * If \<queue directory>/upload is empty, move all files generated so far into the \<queue directory>/upload directory.
     * If the destination Node is UP and there are pending hints to it in the \<queue directory>/upload directory start sending hints to it:
       * If hint's timestamp is older than mutation.gc_grace_seconds() drop this hint. The hint's timestamp is evaluated as _hints_file_ last modification time minus the hints timer period (10s).
       * Hints are sent using a MUTATE verb:
         * Each mutation is sent in a separate message.
           * If the Node in the hint is a valid mutation replica - send the mutation to it.
           * Otherwise execute the original mutation with CL=ANY.
       * Once the complete hints file is processed it's deleted and we move to the next file.
   * Local Node is decommissioned - in this case we forward hints to some other Node so that it would send them to the destination later on.

## Hints streaming
 * Streaming is performed using a new HINT_STREAMING verb:
   * When Node is decommissioned it would stream its hints to other Nodes of the cluster:
     * Shard X would send its hints to the Node[Yx], where Yx = X mod N, where N is number of Nodes in the cluster without the Node that is being decommissioned.
   * Hints are streamed in chunks of 64KB consisting of full mutations or a single mutation if its size is greater than 64KB.
   * Receiver distributes received hints equally among local shards: pushes them to the corresponding _hint_queue_s (see "Hints generation" above).



