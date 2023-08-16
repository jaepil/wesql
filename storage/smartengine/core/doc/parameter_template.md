<h2>Parameter Template</h2>
The parameters of smartengine are divided into basic parameters, non-resource-related parameters and resource-related parameters.

<h3>Basic parameters</h3>

| Parameter Name| Default Value | Description |
| ------------- | ------------- | ----------- |
| loose_smartengine | 1 | enable smartengine |
| default_storage_engine | smartengine | default use smartengine for storage engine |
| log_error_verbosity | 3 | print smartengine info log |
| innodb_buffer_pool_size | 128M | minimize innodb buffer pool size |
| binlog-format | row | smartengine only support row format for SBR |

<h3>Non-resource-related parameters</h3>

| Parameter Name| Default Value | Value Range | Description |
| ------------- | ------------- | ----------- | ----------- |
| loose_smartengine_datadir | ${datadir}/smartengine | | smartengine data directory |
| loose_smartengine_wal_dir | ${datadir}/smartengine | | smartengine wal directory |
| loose_smartengine_flush_log_at_trx_commit | 1 | {0,1,2} | smartengine sync log strategy on transaction commit.</br>0: not sync until wal_bytes_per_sync</br>1:  sync on commit</br>2:  syc per second|
| loose_smartengine_enable_2pc | 1 | {0, 1} | smartengine enable two-phase commit |
| loose_smartengine_batch_group_slot_array_size | 5 | [1, INT64_MAX) | smartengine group commit slot size |
|loose_smartengine_batch_group_max_group_size | 15 | [1, INT64_MAX) | smartengine max transaction size per slot can group |
| loose_smartengine_batch_group_max_leader_wait_time_us | 50 | [1, INT64_MAX) | smartengine max wait time for group transactions |
| loose_smartengine_block_size | 16384 | [1, INT64_MAX) | smartengine data block size |
| loose_smartengine_disable_auto_compactions | 0 | {0, 1} | Disable auto compaction scheduler |
| loose_smartengine_dump_memtable_limit_size | 0 | [0, INT64_MAX) |  The memtable which size less than it can directly dump to disk |
| loose_smartengine_min_write_buffer_number_to_merge | 1 | [1, INT64_MAX) | Min number immutable memtables to flush |
| loose_smartengine_level0_file_num_compaction_trigger | 64 | [1, INT64_MAX) | Level0 extents number to trigger minor compaction |
| loose_smartengine_level0_layer_num_compaction_trigger | 2 | [1, INT64_MAX) | Level0 layers number to trigger minor compaction |
| loose_smartengine_level1_extents_major_compaction_trigger | 1000 | [1, INT64_MAX) | Level1 extents number to trigger major compaction |
| loose_smartengine_level2_usage_percent | 70 | [0, 100] | Level2 valid data percent to trigger self major compaction |
| loose_smartengine_flush_delete_percent | 70 | [0, 100] | Memtable delete records percent to trigger delete compaction |
| loose_smartengine_compaction_delete_percent | 50 | [0, 100] | Extent valid data percent to trigger delete major self compaction |
| loose_smartengine_flush_delete_percent_trigger | 700000 | [10, 1 << 30) | Min memtable size for calculate smartengine_flush_delete_percent |
| loose_smartengine_flush_delete_record_trigger | 700000 | [1, 1 << 30] | Memtable delete records number to trigger delete compaction |
| loose_smartengine_scan_add_blocks_limit | 100 | [0, INT64_MAX) | Max data blocks number to add into block cache in per query |
| loose_smartengine_compression_per_level | kZSTD:kZSTD:kZSTD | | Compression method for per level |

<h3>Resource-related parameters</h3>
Q_MEM: memory size(MB)</br>
Q_CPU: cpu size</br>

| Parameter Name| Recommended Value | Description |
| ------------- | ----------------- | ----------- |
| loose_smartengine_write_buffer_size | min(max(32, Q_MEM * 0.01), 256) | smartengine single memtable size |
| loose_smartengine_db_write_buffer_size | Q_MEM * 0.3 | smartengine total active memtable size |
| loose_smartengine_db_total_write_buffer_size | Q_MEM * 0.3 | smartengine total memtable size |
| loose_smartengine_block_cache_size | Q_MEM * 0.3 | smartengine block cache size |
| loose_smartengine_row_cache_size | Q_MEM * 0.1 | smartengine row cache size |
| loose_smartengine_max_total_wal_size | min(Q_MEM * 0.3, 12 * 1024) | smartengine max active wal size |
| loose_smartengine_max_background_flushes | max(1, min(Q_CPU / 2,  8)) | smartengine background flush threads number |
| loose_smartengine_base_background_compactions | max(1, min(Q_CPU/2, 8)) | smartengine normal background compaction threads number |
| loose_smartengine_max_background_compactions | max(1, min(Q_CPU/2, 12)) | smartengine max background compaction threads number |

