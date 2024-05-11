# These are the sources from which librocksdb.a is built:
LIB_SOURCES =                                                   \
  backup/hotbackup_impl.cc                                      \
  cache/cache_entry.cc                                          \
  cache/clock_cache.cc                                          \
  cache/lru_cache.cc                                            \
  cache/row_cache.cc                                            \
  cache/sharded_cache.cc                                        \
  compact/reuse_block_merge_iterator.cc                         \
  compact/range_iterator.cc                                     \
  compact/compaction_job.cc                                     \
  compact/compaction.cc                                         \
	compact/flush_iterator.cc                                     \
  compact/split_compaction.cc                                   \
  compact/mt_ext_compaction.cc                                  \
  compact/compaction_tasks_picker.cc                            \
  compact/task_type.cc                                          \
  compact/compaction_iterator.cc                                \
  compact/new_compaction_iterator.cc                            \
  compact/compaction_stats.cc                                   \
  db/column_family.cc                                           \
  db/db_filesnapshot.cc                                         \
  db/db_impl.cc                                                 \
  db/db_impl_write.cc                                           \
  db/db_impl_compaction_flush.cc                                \
  db/db_impl_files.cc                                           \
  db/db_impl_open.cc                                            \
  db/db_impl_debug.cc                                           \
  db/db_info_dumper.cc                                          \
  db/db_iter.cc                                                 \
  db/dbformat.cc                                                \
  db/replay_task.cc                                             \
  db/replay_thread_pool.cc                                      \
  db/replay_threadpool_executor.cc                              \
  db/dump_job.cc                                                \
  db/flush_job.cc                                               \
  db/flush_scheduler.cc                                         \
  db/internal_stats.cc                                          \
  db/log_reader.cc                                              \
  db/log_writer.cc                                              \
  db/recovery_point.cc                                          \
  db/snapshot_impl.cc                                           \
  db/table_cache.cc                                             \
  db/version_set.cc                                             \
  db/wal_manager.cc                                             \
  db/batch_group.cc                                             \
  db/pipline_queue_manager.cc                                   \
  env/env.cc                                                    \
  env/env_posix.cc                                              \
  env/io_posix.cc                                               \
  env/memenv.cc                                                 \
  logger/logger.cc                                              \
  memory/chunk_allocator.cc                                     \
  memory/alloc_mgr.cc                                           \
  memory/mod_info.cc                                            \
  memtable/memtable.cc                                          \
  memtable/memtable_allocator.cc                                \
  memtable/memtable_list.cc                                     \
  memtable/skiplistrep.cc                                       \
  memtable/art.cc                                               \
  memtable/art_node.cc                                          \
  memtable/artrep.cc                                            \
  monitoring/histogram.cc                                       \
  monitoring/histogram_windowing.cc                             \
  monitoring/instrumented_mutex.cc                              \
  monitoring/iostats_context.cc                                 \
  monitoring/perf_level.cc                                      \
  monitoring/query_perf_context.cc                              \
  monitoring/statistics.cc                                      \
  monitoring/thread_status_impl.cc                              \
  monitoring/thread_status_updater.cc                           \
  monitoring/thread_status_updater_debug.cc                     \
  monitoring/thread_status_util.cc                              \
  monitoring/thread_status_util_debug.cc                        \
  options/cf_options.cc                                         \
  options/db_options.cc                                         \
  options/options.cc                                            \
  options/options_helper.cc                                     \
  port/port_posix.cc                                            \
  port/stack_trace.cc                                           \
  storage/change_info.cc                                        \
  storage/data_file.cc                                          \
  storage/extent_meta_manager.cc                                \
  storage/extent_space_file.cc                                  \
  storage/extent_space_manager.cc                               \
  storage/extent_space_obj.cc                                   \
  storage/io_extent.cc                                          \
  storage/large_object_extent_manager.cc                        \
  storage/shrink_job.cc                                         \
  storage/storage_common.cc                                     \
  storage/storage_logger.cc                                     \
  storage/storage_log_entry.cc                                  \
  storage/storage_manager.cc                                    \
  storage/storage_meta_struct.cc                                \
  storage/table_space.cc                                        \
  storage/multi_version_extent_meta_layer.cc                    \
  table/block_struct.cc                                         \
  table/extent_table_factory.cc                                 \
  table/bloom_block.cc                                          \
  table/extent_reader.cc                                        \
  table/extent_struct.cc                                        \
  table/extent_writer.cc                                        \
  table/full_filter_block.cc                                    \
  table/filter_manager.cc                                       \
  table/get_context.cc                                          \
  table/index_block_reader.cc                                   \
  table/index_block_writer.cc                                   \
  table/iterator.cc                                             \
  table/large_object.cc                                         \
  table/merging_iterator.cc                                     \
  table/parallel_read.cc                                        \
  table/row_block.cc                                            \
  table/row_block_iterator.cc                                   \
  table/row_block_writer.cc                                     \
  table/sst_file_writer.cc                                      \
  table/table_properties.cc                                     \
  table/two_level_iterator.cc                                   \
  table/sstable_scan_iterator.cc                                \
  util/aio_wrapper.cc                                           \
  util/arena.cc                                                 \
  util/bloom.cc                                                 \
  util/build_version.cc                                         \
  util/coding.cc                                                \
  util/comparator.cc                                            \
  util/compress/compressor.cc                                   \
  util/compress/compressor_factory.cc                           \
  util/compress/compressor_helper.cc                            \
  util/compress/lz4_compressor.cc                               \
  util/compress/zlib_compressor.cc                              \
  util/compress/zstd_compressor.cc                              \
  util/concurrent_arena.cc                                      \
  util/crc32c.cc                                                \
  util/dio_helper.cc                                            \
  util/dynamic_bloom.cc                                         \
  util/ebr.cc                                                   \
  util/file_reader_writer.cc                                    \
  util/concurrent_direct_file_writer.cc                         \
  util/file_util.cc                                             \
  util/filename.cc                                              \
  util/filter_policy.cc                                         \
  util/hash.cc                                                  \
  util/murmurhash.cc                                            \
  util/random.cc                                                \
  util/rate_limiter.cc                                          \
  util/slice.cc                                                 \
  util/status.cc                                                \
  util/status_message.cc                                        \
  util/string_util.cc                                           \
  util/sync_point.cc                                            \
  util/thread_local.cc                                          \
  util/threadpool_imp.cc                                        \
  util/transaction_test_util.cc                                 \
  util/xxhash.cc                                                \
  util/memory_stat.cc                                           \
  util/to_string.cc                                             \
  util/misc_utility.cc                                          \
  transactions/optimistic_transaction_db_impl.cc                \
  transactions/optimistic_transaction_impl.cc                   \
  transactions/transaction_base.cc                              \
  transactions/transaction_db_impl.cc                           \
  transactions/transaction_db_mutex_impl.cc                     \
  transactions/transaction_impl.cc                              \
  transactions/transaction_log_impl.cc                          \
  transactions/transaction_lock_mgr.cc                          \
  transactions/transaction_util.cc                              \
  write_batch/write_batch.cc                                    \
  write_batch/write_batch_base.cc                               \
  write_batch/write_batch_with_index.cc                         \
  write_batch/write_batch_with_index_internal.cc

TOOL_LIB_SOURCES = \
  tools/ldb_cmd.cc                                               \
  tools/ldb_tool.cc                                              \
  tools/sst_dump_tool.cc                                         \

MOCK_LIB_SOURCES = \
  env/mock_env.cc \
  util/fault_injection_test_env.cc

BENCH_LIB_SOURCES = \
  tools/db_bench_tool.cc                                        \

TEST_LIB_SOURCES = \
  util/testharness.cc                                                   \
  util/testutil.cc                                                      \
  db/db_test_util.cc

MAIN_SOURCES =                                                    \
  cache/cache_bench.cc                                                   \
  unittest/cache/cache_test.cc                                                    \
  unittest/env/env_basic_test.cc                                                 \
  unittest/env/env_test.cc                                                       \
  unittest/env/mock_env_test.cc                                                  \
  unittest/memtable/memtablerep_bench.cc                                         \
  unittest/memory/alloc_mgr_test.cc                                      \
  unittest/monitoring/histogram_test.cc                                          \
  unittest/monitoring/iostats_context_test.cc                                    \
  unittest/monitoring/statistics_test.cc                                         \
  unittest/options/options_test.cc                                               \
  unittest/table/block_test.cc                                                   \
  unittest/table/full_filter_block_test.cc                                       \
  unittest/table/merger_test.cc                                                  \
  unittest/table/extent_table_test.cc                                            \
  unittest/third-party/gtest-1.7.0/fused-src/gtest/gtest-all.cc                  \
  unittest/tools/db_bench.cc                                                     \
  unittest/tools/db_bench_tool_test.cc                                           \
  unittest/tools/db_sanity_test.cc                                               \
  unittest/tools/ldb_cmd_test.cc                                                 \
  unittest/tools/reduce_levels_test.cc                                           \
  unittest/tools/sst_dump_test.cc                                                \
  unittest/util/arena_test.cc                                                    \
  unittest/util/autovector_test.cc                                               \
  unittest/util/bloom_test.cc                                                    \
  unittest/util/coding_test.cc                                                   \
  unittest/util/crc32c_test.cc                                                   \
  unittest/util/dio_helper_test.cc                                               \
  unittest/util/dynamic_bloom_test.cc                                            \
  unittest/util/event_logger_test.cc                                             \
  unittest/util/filelock_test.cc                                                 \
  unittest/util/log_write_bench.cc                                               \
  unittest/util/rate_limiter_test.cc                                             \
  unittest/util/thread_list_test.cc                                              \
  unittest/util/thread_local_test.cc                                             \
  unittest/util/concurrent_direct_file_writer_test.cc                            \
  utilities/checkpoint/checkpoint_test.cc                               \
  utilities/column_aware_encoding_exp.cc                                \
  utilities/column_aware_encoding_test.cc                               \
  unittest/backup/hotbackup_test.cc                                     \
  unittest/db/column_family_test.cc                                     \
  unittest/db/compaction_job_stats_test.cc                              \
  unittest/db/compaction_job_test.cc                                    \
  unittest/db/compaction_picker_test.cc                                 \
  unittest/db/comparator_db_test.cc                                     \
  unittest/db/corruption_test.cc                                        \
  unittest/db/db_basic_test.cc                                          \
  unittest/db/db_block_cache_test.cc                                    \
  unittest/db/db_bloom_filter_test.cc                                   \
  unittest/db/db_compaction_filter_test.cc                              \
  unittest/db/db_compaction_test.cc                                     \
  unittest/db/db_dynamic_level_test.cc                                  \
  unittest/db/db_flush_test.cc                                          \
  unittest/db/db_inplace_update_test.cc                                 \
  unittest/db/db_io_failure_test.cc                                     \
  unittest/db/db_iter_test.cc                                           \
  unittest/db/db_iterator_test.cc                                       \
  unittest/db/db_log_iter_test.cc                                       \
  unittest/db/db_options_test.cc                                        \
  unittest/db/db_sst_test.cc                                            \
  unittest/db/db_tailing_iter_test.cc                                   \
  unittest/db/db_test.cc                                                \
  unittest/db/db_universal_compaction_test.cc                           \
  unittest/db/db_wal_test.cc                                            \
  unittest/db/dbformat_test.cc                                          \
  unittest/db/deletefile_test.cc                                        \
  unittest/db/external_sst_file_basic_test.cc                           \
  unittest/db/external_sst_file_test.cc                                 \
  unittest/db/fault_injection_test.cc                                   \
  unittest/db/filename_test.cc                                          \
  unittest/db/flush_job_test.cc                                         \
  unittest/db/log_test.cc                                               \
  unittest/db/manual_compaction_test.cc                                 \
  unittest/db/merge_test.cc                                             \
  unittest/db/options_file_test.cc                                      \
  unittest/db/perf_context_test.cc                                      \
  unittest/db/prefix_test.cc                                            \
  unittest/db/shrink_job_test.cc                                        \
  unittest/db/wal_manager_test.cc                                       \
  unittest/db/write_batch_test.cc                                       \
  unittest/db/write_callback_test.cc                                    \
  unittest/memtable/art_test.cc                                         \
  unittest/memtable/inlineskiplist_test.cc                              \
  unittest/memtable/skiplist_test.cc                                    \
  unittest/table/parallel_read_test.cc                         					\
  unittest/transactions/optimistic_transaction_test.cc                  \
  unittest/transactions/transaction_test.cc                             \
  unittest/write_batch/write_batch_with_index_test.cc                   \
  unittest/write_batch/write_batch_test.cc                              \
