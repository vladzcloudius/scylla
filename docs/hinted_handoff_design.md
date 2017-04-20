## Abstract
Hinted Handoff is a feature that allows replaying failed writes. The mutation and the destination replica are saved in a log and replayed later according to the feature configuration. 

## Hinted Handoff Configuration Parameters 
 * _hinted_handoff_enabled_: Enabled or disables the Hinted Handoff feature completely.
 * _hinted_handoff_disabled_datacenters_: Disable the feature for the given DCs.
 * _max_hint_window_in_ms_: Don't generate hints if the destination Node has been down for more than this value. The hints should resume once the Node is seen up (and went down).
 * _hints_directory_: Directory where scylla will store hints. By default `$SCYLLA_HOME/hints`
 * _max_hints_file_size_in_mb_: Maximum hints file size.
 * _hints_compression_: Compression to apply to hints files. By default, hints files are stored uncompressed.
 
## Future configuration
 * We should define the fairness configuration between the regular WRITES and hints WRITES.
   Since we don't have either CPU scheduler or (and) Network scheduler at the moment we can't give any guarantee regarding the runtime and/or networking bandwidth fairness.
   Once we have tools to define it we should give the hints sender some low runtime priority and some small share of the bandwidth.

## Hints generation
 * Once the WRITE mutation fails with a time out we create a _hints_queue_ for this Node.
   * The queue is specified by a _hints_descriptor_:
     * Destination Node UUID.
     * Timestamp (used in the corresponding file name).

 * Each hint is specified by:
   * Timestamp.
   * Mutation.
   * Minimal mutation GC_GS (the minimal GC_GS among all involved partitions).

Hints are appended to the _hints_queue_ until (all this should be done using the existing or slightly modified _commitlog_ API):
 * The destination Node goes UP:
   * The _hints_queue_ is flushed to the disk and destroyed.
   * The hints dispatcher context starts sending hints collected so far to the destination Node.
 * The current Node goes DOWN for some reason (shutdown, decommission, etc.).

We are going to reuse the commitlog infrastructure for wrinting hints to disk - it provides both the internall buffering and the memory consumption control.

The info that is going to be encoded into the hints file's name:
 * The destination Node UUID
 * Source shard ID
 * Timestamp

### Redistribution of hints when Node boots.
 * When Node boots all present hints files are redistributed equally between all present shards.

## Hints sending
 * Hints are sent from each shard.
 * Each shard sends the hints that it owns (according to the hint file name).
 * If hint's timestamp is older than (now() - min_GC_GS), where min_GC_GS = min(hint.min_GC_SC, mutation.min_GC_GS) drop this hint.
 * Hints are sent using a MUTATE verb:
   * Each mutation is sent in a separate message.
     * If the Node in the hint is a valid mutation replica - send the hint to it.
     * Otherwise send the hint to all valid replicas of mutation partitions.
 * Hints sending is triggered by the following events: 
   * Timer: every X seconds (Origin does it every 10s).
   * Local Node is decommissioned - in this case we forward hints to some other Node so that it would send them to the destination later on.

## Hints streaming
 * Streaming is performed using a new HINT_STREAMING verb:
   * When Node is decommissioned it would stream its hints to other Nodes of the cluster:
     * Shard X would send its hints to the Node[Yx], where Yx = X mod N, where N is number of Nodes in the cluster without the Node that is being decommissioned.
   * Hints are streamed in chunks of 64KB consisting of full mutations or a single mutation if its size is greater than 64KB.
   * Receiver distributes received hints equally among local shards: pushes them to the corresponding _hint_queue_s (see "Hints generation" above).



