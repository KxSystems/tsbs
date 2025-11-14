include "schema/cpu-only.q";

// column timestamp comes as a long integer!
schema[`disk]: `c`t!(`timestamp`hostname`region`datacenter`rack`os`arch`team`service`service_version`service_environment`path`fstype`total`free`used`used_percent`inodes_total`inodes_free`inodes_used;
                   "PSSSISSSIISSSJJJIJJJ");

// column timestamp comes as a long integer!
schema[`diskio]: `c`t!(`timestamp`hostname`region`datacenter`rack`os`arch`team`service`service_version`service_environment`serial`reads`writes`read_bytes`write_bytes`read_time`write_time`io_time;
                   "PSSSISSSIISSIIIIIII");

// column timestamp comes as a long integer!
schema[`kernel]: `c`t!(`timestamp`hostname`region`datacenter`rack`os`arch`team`service`service_version`service_environment`boot_time`interrupts`context_switches`processes_forked`disk_pages_in`disk_pages_out;
                   "PSSSISSSIISIIIIII");

// column timestamp comes as a long integer!
schema[`mem]: `c`t!(`timestamp`hostname`region`datacenter`rack`os`arch`team`service`service_version`service_environment`total`available`used`free`cached`buffered`used_percent`available_percent`buffered_percent;
                   "PSSSISSSIISJJJJJJFFF");

// column timestamp comes as a long integer!
schema[`net]: `c`t!(`timestamp`hostname`region`datacenter`rack`os`arch`team`service`service_version`service_environment`interface`bytes_sent`bytes_recv`packets_sent`packets_recv`err_in`err_out`drop_in`drop_out;
                   "PSSSISSSIISSIIIIIIII");

// column timestamp comes as a long integer!
schema[`nginx]: `c`t!(`timestamp`hostname`region`datacenter`rack`os`arch`team`service`service_version`service_environment`port`server`accepts`active`handled`reading`requests`waiting`writing;
                   "PSSSISSSIISJSIIIIIII");

// column timestamp comes as a long integer!
schema[`postgresl]: `c`t!(`timestamp`hostname`region`datacenter`rack`os`arch`team`service`service_version`service_environment`numbackends`xact_commit`xact_rollback`blks_read`blks_hit`tup_returned`tup_fetched`tup_inserted`tup_updated`tup_deleted`conflicts`temp_files`temp_bytes`deadlocks`blk_read_time`blk_write_time;
                   "PSSSISSSIISIIIIIIIIIIIIJIII");

// column timestamp comes as a long integer!
schema[`redis]: `c`t!(`timestamp`hostname`region`datacenter`rack`os`arch`team`service`service_version`service_environment`port`server`uptime_in_seconds`total_connections_received`expired_keys`evicted_keys`keyspace_hits`keyspace_misses`instantaneous_ops_per_sec`instantaneous_input_kbps`instantaneous_output_kbps`connected_clients`used_memory`used_memory_rss`used_memory_peak`used_memory_lua`rdb_changes_since_last_save`sync_full`sync_partial_ok`sync_partial_err`pubsub_channels`pubsub_patterns`latest_fork_usec`connected_slaves`master_repl_offset`repl_backlog_active`repl_backlog_size`repl_backlog_histlen`mem_fragmentation_ratio`used_cpu_sys`used_cpu_user`used_cpu_sys_children`used_cpu_user_children;
                   "PSSSISSSIISISIIIIIIIIIIJJJJIIIIIIIIIIIIIIIII");

