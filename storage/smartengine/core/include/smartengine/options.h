/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_ROCKSDB_INCLUDE_OPTIONS_H_
#define STORAGE_ROCKSDB_INCLUDE_OPTIONS_H_

#include <stddef.h>
#include <stdint.h>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "smartengine/advanced_options.h"
#include "smartengine/comparator.h"
#include "smartengine/env.h"
#include "smartengine/write_buffer_manager.h"

#ifdef max
#undef max
#endif

namespace smartengine {

namespace memtable {
class MemTableRepFactory;
}

namespace db {
class InternalKeyComparator;
class Snapshot;
class WalFilter;
}

namespace table {
class TableFactory;
class FilterPolicy;
}

namespace cache {
class Cache;
class RowCache;
}

namespace util {
class Comparator;
class Env;
class SstFileManager;
class RateLimiter;
enum InfoLogLevel : unsigned char;
}

namespace monitor {
class Statistics;
}

namespace table {
class TableFactory;
}

namespace common {

class Slice;

// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.
enum CompressionType : unsigned char {
  // NOTE: do not change the values of existing entries, as these are
  // part of the persistent format on disk.
  kNoCompression = 0x0,
  kSnappyCompression = 0x1,
  kZlibCompression = 0x2,
  kBZip2Compression = 0x3,
  kLZ4Compression = 0x4,
  kLZ4HCCompression = 0x5,
  kXpressCompression = 0x6,
  kZSTD = 0x7,

  // Only use kZSTDNotFinalCompression if you have to use ZSTD lib older than
  // 0.8.0 or consider a possibility of downgrading the service or copying
  // the database files to another service running with an older version of
  // RocksDB that doesn't have kZSTD. Otherwise, you should use kZSTD. We will
  // eventually remove the option from the public API.
  kZSTDNotFinalCompression = 0x40,

  // kDisableCompressionOption is used to disable some compression options.
  kDisableCompressionOption = 0xff,
};

struct Options;

struct ColumnFamilyOptions : public AdvancedColumnFamilyOptions {

  // Some functions that make it easier to optimize RocksDB
  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  ColumnFamilyOptions* OptimizeForSmallDb();


  // -------------------
  // Parameters that affect behavior

  // Comparator used to define the order of keys in the table.
  // Default: a comparator that uses lexicographic byte-wise ordering
  //
  // REQUIRES: The client must ensure that the comparator supplied
  // here has the same name and orders keys *exactly* the same as the
  // comparator provided to previous open calls on the same DB.
  const util::Comparator* comparator = util::BytewiseComparator();

  // -------------------
  // Parameters that affect performance

  // Amount of data to build up in memory (backed by an unsorted log
  // on disk) before converting to a sorted on-disk file.
  //
  // Larger values increase performance, especially during bulk loads.
  // Up to max_write_buffer_number write buffers may be held in memory
  // at the same time,
  // so you may wish to adjust this parameter to control memory usage.
  // Also, a larger write buffer will result in a longer recovery time
  // the next time the database is opened.
  //
  // Note that write_buffer_size is enforced per column family.
  // See db_write_buffer_size for sharing memory across column families.
  //
  // Default: 64MB
  //
  // Dynamically changeable through SetOptions() API
  size_t write_buffer_size = 64 << 20;

  // Flush memtable when the deletion records reach this percent.
  //
  // Note this limitation could take effect only after the total entries
  // exceed flush_delete_percent_trigger(700000 by default), in case small memtable.
  //
  // Default: 70
  //
  // Dynamically changeable through SetOptions() API
  int flush_delete_percent = 70;

  // trigger a Delete Compaction when the deletion records of one output extent
  // reach this percent.
  //
  // Default: 50
  //
  // Dynamically changeable through SetOptions() API
  int compaction_delete_percent = 50;

  // Switch memtable when the total records exceed this number.
  //
  // Default: 700000 
  //
  // Dynamically changeable through SetOptions() API
  int flush_delete_percent_trigger = 700000;

  // Switch memtable when the delete records exceed this number.
  //
  // Default: 700000 
  //
  // Dynamically changeable through SetOptions() API
  int flush_delete_record_trigger = 700000;

  // Compress blocks using the specified compression algorithm.  This
  // parameter can be changed dynamically.
  //
  // Default: kSnappyCompression, if it's supported. If snappy is not linked
  // with the library, the default is kNoCompression.
  //
  // Typical speeds of kSnappyCompression on an Intel(R) Core(TM)2 2.4GHz:
  //    ~200-500MB/s compression
  //    ~400-800MB/s decompression
  // Note that these speeds are significantly faster than most
  // persistent storage speeds, and therefore it is typically never
  // worth switching to kNoCompression.  Even if the input data is
  // incompressible, the kSnappyCompression implementation will
  // efficiently detect that and will switch to uncompressed mode.
  CompressionType compression;

  // Compression algorithm that will be used for the bottommost level that
  // contain files. If level-compaction is used, this option will only affect
  // levels after base level.
  //
  // Default: kDisableCompressionOption (Disabled)
  CompressionType bottommost_compression = kDisableCompressionOption;

  // different options for compression algorithms
  CompressionOptions compression_opts;

  // Number of files to trigger level-0 compaction. A value <0 means that
  // level-0 compaction will not be triggered by number of files at all.
  //
  // Default: 4
  //
  // Dynamically changeable through SetOptions() API
  int level0_file_num_compaction_trigger = 4;

  // Number of layers to trigger intra level-0 compaction.
  //
  // Default: 16
  //
  int level0_layer_num_compaction_trigger = 16;

  // Number of L1 extents to trigger major comapction.
  //
  // Default: 1000
  //
  int level1_extents_major_compaction_trigger = 1000;

  // Usage percent of L2 extents, to trigger auto major self task
  //
  // Default: 70
  //
  int64_t level2_usage_percent = 70;

  // Control maximum total data size for a level.
  // max_bytes_for_level_base is the max total for level-1.
  // Maximum number of bytes for level L can be calculated as
  // (max_bytes_for_level_base) * (max_bytes_for_level_multiplier ^ (L-1))
  // For example, if max_bytes_for_level_base is 200MB, and if
  // max_bytes_for_level_multiplier is 10, total data size for level-1
  // will be 200MB, total file size for level-2 will be 2GB,
  // and total file size for level-3 will be 20GB.
  //
  // Default: 256MB.
  //
  // Dynamically changeable through SetOptions() API
  uint64_t max_bytes_for_level_base = 256 * 1048576;

  // Disable automatic compactions. Manual compactions can still
  // be issued on this column family
  //
  // Dynamically changeable through SetOptions() API
  bool disable_auto_compactions = false;

  // This is a factory that provides TableFactory objects.
  // Default: a block-based table factory that provides a default
  // implementation of TableBuilder and TableReader with default
  // BlockBasedTableOptions.
  std::shared_ptr<table::TableFactory> table_factory;

  // Control the block count when add scan data to block cache
  // add scan_add_blocks_limit blocks each scan for one layer
  // Default: 100
  uint64_t scan_add_blocks_limit = 100;

  // Control the layers of sst
  // Default: 2
  int bottommost_level = 2;
  // Control the size of single compaction task
  // Default: 1000
  int compaction_task_extents_limit = 1000;
  // Create ColumnFamilyOptions with default values for all fields
  ColumnFamilyOptions();
  // Create ColumnFamilyOptions from Options
  explicit ColumnFamilyOptions(const Options& options);

  void Dump() const;
};

enum class WALRecoveryMode : char {
  // Original levelDB recovery
  // We tolerate incomplete record in trailing data on all logs
  // Use case : This is legacy behavior (default)
  kTolerateCorruptedTailRecords = 0x00,
  // Recover from clean shutdown
  // We don't expect to find any corruption in the WAL
  // Use case : This is ideal for unit tests and rare applications that
  // can require high consistency guarantee
  kAbsoluteConsistency = 0x01,
  // Recover to point-in-time consistency
  // We stop the WAL playback on discovering WAL inconsistency
  // Use case : Ideal for systems that have disk controller cache like
  // hard disk, SSD without super capacitor that store related data
  kPointInTimeRecovery = 0x02,
  // Recovery after a disaster
  // We ignore any corruption in the WAL and try to salvage as much data as
  // possible
  // Use case : Ideal for last ditch effort to recover data or systems that
  // operate with low grade unrelated data
  kSkipAnyCorruptedRecords = 0x03,
};

struct DbPath {
  std::string path;
  uint64_t target_size;  // Target size of total files under the path, in byte.

  DbPath() : target_size(0) {}
  DbPath(const std::string& p, uint64_t t) : path(p), target_size(t) {}
};

struct DBOptions {
  // Some functions that make it easier to optimize RocksDB

  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  DBOptions* OptimizeForSmallDb();

#ifndef ROCKSDB_LITE
  // By default, RocksDB uses only one background thread for flush and
  // compaction. Calling this function will set it up such that total of
  // `total_threads` is used. Good value for `total_threads` is the number of
  // cores. You almost definitely want to call this function if your system is
  // bottlenecked by RocksDB.
  DBOptions* IncreaseParallelism(int total_threads = 16);
#endif  // ROCKSDB_LITE

  // If true, the database will be created if it is missing.
  // Default: false
  bool create_if_missing = false;

  // If true, missing column families will be automatically created.
  // Default: false
  bool create_missing_column_families = false;

  // If true, an error is raised if the database already exists.
  // Default: false
  bool error_if_exists = false;

  // If true, RocksDB will aggressively check consistency of the data.
  // Also, if any of the  writes to the database fails (Put, Delete,
  // Write), the database will switch to read-only mode and fail all other
  // Write operations.
  // In most cases you want this to be set to true.
  // Default: true
  bool paranoid_checks = true;

  // Use the specified object to interact with the environment,
  // e.g. to read/write files, schedule background work, etc.
  // Default: Env::Default()
  util::Env* env = util::Env::Default();

  // Use to control write rate of flush and compaction. Flush has higher
  // priority than compaction. Rate limiting is disabled if nullptr.
  // If rate limiter is enabled, bytes_per_sync is set to 1MB by default.
  // Default: nullptr
  std::shared_ptr<util::RateLimiter> rate_limiter = nullptr;

  // Use to track SST files and control their file deletion rate.
  //
  // Features:
  //  - Throttle the deletion rate of the SST files.
  //  - Keep track the total size of all SST files.
  //  - Set a maximum allowed space limit for SST files that when reached
  //    the DB wont do any further flushes or compactions and will set the
  //    background error.
  //  - Can be shared between multiple dbs.
  // Limitations:
  //  - Only track and throttle deletes of SST files in
  //    first db_path (db_name if db_paths is empty).
  //
  // Default: nullptr
  std::shared_ptr<util::SstFileManager> sst_file_manager = nullptr;

#ifdef NDEBUG
  util::InfoLogLevel info_log_level = util::INFO_LEVEL;
#else
  util::InfoLogLevel info_log_level = util::DEBUG_LEVEL;
#endif  // NDEBUG

  // Number of open files that can be used by the DB.  You may need to
  // increase this if your database has a large working set. Value -1 means
  // files opened are always kept open. You can estimate number of files based
  // on target_file_size_base and target_file_size_multiplier for level-based
  // compaction. For universal-style compaction, you can usually set it to -1.
  // Default: -1
  int max_open_files = -1;

  // If max_open_files is -1, DB will open all files on DB::Open(). You can
  // use this option to increase the number of threads used to open the files.
  // Default: 16
  int max_file_opening_threads = 16;

  // Once write-ahead logs exceed this size, we will start forcing the flush of
  // column families whose memtables are backed by the oldest live WAL file
  // (i.e. the ones that are causing all the space amplification). If set to 0
  // (default), we will dynamically choose the WAL size limit to be
  // [sum of all write_buffer_size * max_write_buffer_number] * 4
  // Default: 0
  uint64_t max_total_wal_size = 0;

  // If non-null, then we should collect metrics about database operations
  std::shared_ptr<monitor::Statistics> statistics = nullptr;

  // default to 1 minute
  uint64_t monitor_interval_ms = 60'000;

  // If true, then every store to stable storage will issue a fsync.
  // If false, then every store to stable storage will issue a fdatasync.
  // This parameter should be set to true while storing data to
  // filesystem like ext3 that can lose files after a reboot.
  // Default: false
  // Note: on many platforms fdatasync is defined as fsync, so this parameter
  // would make no difference. Refer to fdatasync definition in this code base.
  bool use_fsync = false;

  // A list of paths where SST files can be put into, with its target size.
  // Newer data is placed into paths specified earlier in the vector while
  // older data gradually moves to paths specified later in the vector.
  //
  // For example, you have a flash device with 10GB allocated for the DB,
  // as well as a hard drive of 2TB, you should config it to be:
  //   [{"/flash_path", 10GB}, {"/hard_drive", 2TB}]
  //
  // The system will try to guarantee data under each path is close to but
  // not larger than the target size. But current and future file sizes used
  // by determining where to place a file are based on best-effort estimation,
  // which means there is a chance that the actual size under the directory
  // is slightly more than target size under some workloads. User should give
  // some buffer room for those cases.
  //
  // If none of the paths has sufficient room to place a file, the file will
  // be placed to the last path anyway, despite to the target size.
  //
  // Placing newer data to earlier paths is also best-efforts. User should
  // expect user files to be placed in higher levels in some extreme cases.
  //
  // If left empty, only one path will be used, which is db_name passed when
  // opening the DB.
  // Default: empty
  std::vector<DbPath> db_paths;

  // This specifies the info LOG dir.
  // If it is empty, the log files will be in the same dir as data.
  // If it is non empty, the log files will be in the specified dir,
  // and the db data dir's absolute path will be used as the log file
  // name's prefix.
  std::string db_log_dir = "";

  // This specifies the absolute dir path for write-ahead logs (WAL).
  // If it is empty, the log files will be in the same dir as data,
  //   dbname is used as the data dir by default
  // If it is non empty, the log files will be in kept the specified dir.
  // When destroying the db,
  //   all log files in wal_dir and the dir itself is deleted
  std::string wal_dir = "";

  // The periodicity when obsolete files get deleted. The default
  // value is 6 hours. The files that get out of scope by compaction
  // process will still get automatically delete on every compaction,
  // regardless of this setting
  uint64_t delete_obsolete_files_period_micros = 6ULL * 60 * 60 * 1000000;

  // The number of threads working background
  //
  // Default: 2
  int32_t filter_building_threads = 2;

  // The number of stripes for the request, the request is distributed in
  // different stripes according to hash(filter_block_cache_key)
  //
  // default: 16
  int32_t filter_queue_stripes = 16;

  // Suggested number of concurrent background compaction jobs, submitted to
  // the default LOW priority thread pool.
  //
  // Default: 1
  int base_background_compactions = 1;

  // Maximum number of concurrent background compaction jobs, submitted to
  // the default LOW priority thread pool.
  // We first try to schedule compactions based on
  // `base_background_compactions`. If the compaction cannot catch up , we
  // will increase number of compaction threads up to
  // `max_background_compactions`.
  //
  // If you're increasing this, also consider increasing number of threads in
  // LOW priority thread pool. For more information, see
  // Env::SetBackgroundThreads
  // Default: 1
  int max_background_compactions = 1;

  // Maximum number of concurrent background memtable flush jobs, submitted to
  // the HIGH priority thread pool.
  //
  // By default, all background jobs (major compaction and memtable flush) go
  // to the LOW priority pool. If this option is set to a positive number,
  // memtable flush jobs will be submitted to the HIGH priority pool.
  // It is important when the same Env is shared by multiple db instances.
  // Without a separate pool, long running major compaction jobs could
  // potentially block memtable flush jobs of other db instances, leading to
  // unnecessary Put stalls.
  //
  // If you're increasing this, also consider increasing number of threads in
  // HIGH priority thread pool. For more information, see
  // Env::SetBackgroundThreads
  // Default: 1
  int max_background_flushes = 1;

  // Maximum number of concurrent background memtable dump jobs, submitted to
  // the LOW priority thread pool.
  int max_background_dumps = 1;

  // when handle wal full, if memtable's size is smaller than dump_memtable_limit_size,
  // will do dump task , other will do flush task
  // default: 64M
  uint64_t dump_memtable_limit_size = 64 * 1024 * 1024;

  // Specify the maximal size of the info log file. If the log file
  // is larger than `max_log_file_size`, a new info log file will
  // be created.
  // If max_log_file_size == 0, all logs will be written to one
  // log file.
  size_t max_log_file_size = 0;

  // Time for the info log file to roll (in seconds).
  // If specified with non-zero value, log file will be rolled
  // if it has been active longer than `log_file_time_to_roll`.
  // Default: 0 (disabled)
  // Not supported in ROCKSDB_LITE mode!
  size_t log_file_time_to_roll = 0;

  // Maximal info log files to be kept.
  // Default: 1000
  size_t keep_log_file_num = 1000;

  // Recycle log files.
  // If non-zero, we will reuse previously written log files for new
  // logs, overwriting the old data.  The value indicates how many
  // such files we will keep around at any point in time for later
  // use.  This is more efficient because the blocks are already
  // allocated and fdatasync does not need to update the inode after
  // each write.
  // Default: 0
  size_t recycle_log_file_num = 0;

  // manifest file is rolled over on reaching this limit.
  // The older manifest file be deleted.
  // The default value is MAX_INT so that roll-over does not take place.
  uint64_t max_manifest_file_size = std::numeric_limits<uint64_t>::max();

  // Number of shards used for table cache.
  int table_cache_numshardbits = 7;

  // NOT SUPPORTED ANYMORE
  // int table_cache_remove_scan_count_limit;

  // The following two fields affect how archived logs will be deleted.
  // 1. If both set to 0, logs will be deleted asap and will not get into
  //    the archive.
  // 2. If WAL_ttl_seconds is 0 and WAL_size_limit_MB is not 0,
  //    WAL files will be checked every 10 min and if total size is greater
  //    then WAL_size_limit_MB, they will be deleted starting with the
  //    earliest until size_limit is met. All empty files will be deleted.
  // 3. If WAL_ttl_seconds is not 0 and WAL_size_limit_MB is 0, then
  //    WAL files will be checked every WAL_ttl_secondsi / 2 and those that
  //    are older than WAL_ttl_seconds will be deleted.
  // 4. If both are not 0, WAL files will be checked every 10 min and both
  //    checks will be performed with ttl being first.
  uint64_t WAL_ttl_seconds = 0;
  uint64_t WAL_size_limit_MB = 0;

  // Number of bytes to preallocate (via fallocate) the manifest
  // files.  Default is 4mb, which is reasonable to reduce random IO
  // as well as prevent overallocation for mounts that preallocate
  // large amounts of data (such as xfs's allocsize option).
  size_t manifest_preallocation_size = 4 * 1024 * 1024;

  // Allow the OS to mmap file for reading sst tables. Default: false
  bool allow_mmap_reads = false;

  // Allow the OS to mmap file for writing.
  // DB::SyncWAL() only works if this is set to false.
  // Default: false
  bool allow_mmap_writes = false;

  // Enable direct I/O mode for read/write
  // they may or may not improve performance depending on the use case
  //
  // Files will be opened in "direct I/O" mode
  // which means that data r/w from the disk will not be cached or
  // bufferized. The hardware buffer of the devices may however still
  // be used. Memory mapped files are not impacted by these parameters.

  // Use O_DIRECT for user reads
  // Default: false
  // Not supported in ROCKSDB_LITE mode!
  bool use_direct_reads = false;

  // Use O_DIRECT for both reads and writes in background flush and compactions
  // When true, we also force new_table_reader_for_compaction_inputs to true.
  // Default: false
  // Not supported in ROCKSDB_LITE mode!
  bool use_direct_io_for_flush_and_compaction = false;

  // If false, fallocate() calls are bypassed
  bool allow_fallocate = true;

  // Disable child process inherit open files. Default: true
  bool is_fd_close_on_exec = true;

  // NOT SUPPORTED ANYMORE -- this options is no longer used
  bool skip_log_error_on_recovery = false;

  // if not zero, dump rocksdb.stats to LOG every stats_dump_period_sec
  // Default: 600 (10 min)
  unsigned int stats_dump_period_sec = 600;

  // If set true, will hint the underlying file system that the file
  // access pattern is random, when a sst file is opened.
  // Default: true
  bool advise_random_on_open = true;

  // Amount of data to build up in memtables across all column
  // families before writing to disk.
  //
  // This is distinct from write_buffer_size, which enforces a limit
  // for a single memtable.
  //
  // This feature is disabled by default. Specify a non-zero value
  // to enable it.
  //
  // Default: 0 (disabled)
  size_t db_write_buffer_size = 0;

  // control the overall memtable memory usage
  size_t db_total_write_buffer_size = 0;

  // The memory usage of memtable will report to this object. The same object
  // can be passed into multiple DBs and it will track the sum of size of all
  // the DBs. If the total size of all live memtables of all the DBs exceeds
  // a limit, a flush will be triggered in the next DB to which the next write
  // is issued.
  //
  // If the object is only passed to on DB, the behavior is the same as
  // db_write_buffer_size. When write_buffer_manager is set, the value set will
  // override db_write_buffer_size.
  //
  // This feature is disabled by default. Specify a non-zero value
  // to enable it.
  //
  // Default: null
  std::shared_ptr<db::WriteBufferManager> write_buffer_manager = nullptr;

  // Specify the file access pattern once a compaction is started.
  // It will be applied to all input files of a compaction.
  // Default: NORMAL
  enum AccessHint { NONE, NORMAL, SEQUENTIAL, WILLNEED };
  AccessHint access_hint_on_compaction_start = NORMAL;

  // If true, always create a new file descriptor and new table reader
  // for compaction inputs. Turn this parameter on may introduce extra
  // memory usage in the table reader, if it allocates extra memory
  // for indexes. This will allow file descriptor prefetch options
  // to be set for compaction input files and not to impact file
  // descriptors for the same file used by user queries.
  // Suggest to enable BlockBasedTableOptions.cache_index_and_filter_blocks
  // for this mode if using block-based table.
  //
  // Default: false
  bool new_table_reader_for_compaction_inputs = false;

  // If non-zero, we perform bigger reads when doing compaction. If you're
  // running RocksDB on spinning disks, you should set this to at least 2MB.
  // That way RocksDB's compaction is doing sequential instead of random reads.
  //
  // When non-zero, we also force new_table_reader_for_compaction_inputs to
  // true.
  //
  // Default: 0
  size_t compaction_readahead_size = 0;

  // This is a maximum buffer size that is used by WinMmapReadableFile in
  // unbuffered disk I/O mode. We need to maintain an aligned buffer for
  // reads. We allow the buffer to grow until the specified value and then
  // for bigger requests allocate one shot buffers. In unbuffered mode we
  // always bypass read-ahead buffer at ReadaheadRandomAccessFile
  // When read-ahead is required we then make use of compaction_readahead_size
  // value and always try to read ahead. With read-ahead we always
  // pre-allocate buffer to the size instead of growing it up to a limit.
  //
  // This option is currently honored only on Windows
  //
  // Default: 1 Mb
  //
  // Special value: 0 - means do not maintain per instance buffer. Allocate
  //                per request buffer and avoid locking.
  size_t random_access_max_buffer_size = 1024 * 1024;

  // This is the maximum buffer size that is used by WritableFileWriter.
  // On Windows, we need to maintain an aligned buffer for writes.
  // We allow the buffer to grow until it's size hits the limit.
  //
  // Default: 1024 * 1024 (1 MB)
  size_t writable_file_max_buffer_size = 1024 * 1024;

  // Use adaptive mutex, which spins in the user space before resorting
  // to kernel. This could reduce context switch when the mutex is not
  // heavily contended. However, if the mutex is hot, we could end up
  // wasting spin time.
  // Default: false
  bool use_adaptive_mutex = false;

  // Create DBOptions with default values for all fields
  DBOptions();
  // Create DBOptions from Options
  explicit DBOptions(const Options& options);

  void Dump() const;

  // Allows OS to incrementally sync files to disk while they are being
  // written, asynchronously, in the background. This operation can be used
  // to smooth out write I/Os over time. Users shouldn't rely on it for
  // persistency guarantee.
  // Issue one request for every bytes_per_sync written. 0 turns it off.
  // Default: 0
  //
  // You may consider using rate_limiter to regulate write rate to device.
  // When rate limiter is enabled, it automatically enables bytes_per_sync
  // to 1MB.
  //
  // This option applies to table files
  uint64_t bytes_per_sync = 0;

  // Same as bytes_per_sync, but applies to WAL files
  // Default: 0, turned off
  uint64_t wal_bytes_per_sync = 0;

  // If true, then the status of the threads involved in this DB will
  // be tracked and available via GetThreadList() API.
  //
  // Default: false
  bool enable_thread_tracking = false;

  // The limited write rate to DB if soft_pending_compaction_bytes_limit or
  // level0_slowdown_writes_trigger is triggered, or we are writing to the
  // last mem table allowed and we allow more than 3 mem tables. It is
  // calculated using size of user write requests before compression.
  // RocksDB may decide to slow down more if the compaction still
  // gets behind further.
  // Unit: byte per second.
  //
  // Default: 16MB/s
  uint64_t delayed_write_rate = 16 * 1024U * 1024U;

  // If true, allow multi-writers to update mem tables in parallel.
  // Only some memtable_factory-s support concurrent writes; currently it
  // is implemented only for SkipListFactory.  Concurrent memtable writes
  // are not compatible with inplace_update_support or filter_deletes.
  // It is strongly recommended to set enable_write_thread_adaptive_yield
  // if you are going to use this feature.
  //
  // Default: true
  bool allow_concurrent_memtable_write = true;

  // If true, threads synchronizing with the write batch group leader will
  // wait for up to write_thread_max_yield_usec before blocking on a mutex.
  // This can substantially improve throughput for concurrent workloads,
  // regardless of whether allow_concurrent_memtable_write is enabled.
  //
  // Default: true
  bool enable_write_thread_adaptive_yield = true;

  // The maximum number of microseconds that a write operation will use
  // a yielding spin loop to coordinate with other write threads before
  // blocking on a mutex.  (Assuming write_thread_slow_yield_usec is
  // set properly) increasing this value is likely to increase RocksDB
  // throughput at the expense of increased CPU usage.
  //
  // Default: 100
  uint64_t write_thread_max_yield_usec = 100;

  // The latency in microseconds after which a std::this_thread::yield
  // call (sched_yield on Linux) is considered to be a signal that
  // other processes or threads would like to use the current core.
  // Increasing this makes writer threads more likely to take CPU
  // by spinning, which will show up as an increase in the number of
  // involuntary context switches.
  //
  // Default: 3
  uint64_t write_thread_slow_yield_usec = 3;

  // If true, then DB::Open() will not update the statistics used to optimize
  // compaction decision by loading table properties from many files.
  // Turning off this feature will improve DBOpen time especially in
  // disk environment.
  //
  // Default: false
  bool skip_stats_update_on_db_open = false;

  // Recovery mode to control the consistency while replaying WAL
  // Default: kAbsoluteConsistency
  WALRecoveryMode wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;

  // If true, using aio interface to read wal files
  // Default: true
  bool enable_aio_wal_reader = true;

  // If true, db will replay wals in parallel to speed up recovery
  // Default: true
  bool parallel_wal_recovery = true;

  // thread num of parallel recovery thread pool
  // Is set to 0, the thread_num will be cpu_core_num - 2
  // Default: 0
  uint32_t parallel_recovery_thread_num = 0;

  // if set to false then recovery will fail when a prepared
  // transaction is encountered in the WAL
  bool allow_2pc = false;

  // A global cache for table-level rows.
  // Default: nullptr (disabled)
  // Not supported in ROCKSDB_LITE mode!
  std::shared_ptr<cache::RowCache> row_cache = nullptr;

#ifndef ROCKSDB_LITE
  // A filter object supplied to be invoked while processing write-ahead-logs
  // (WALs) during recovery. The filter provides a way to inspect log
  // records, ignoring a particular record or skipping replay.
  // The filter is invoked at startup and is invoked from a single-thread
  // currently.
  db::WalFilter* wal_filter = nullptr;
#endif  // ROCKSDB_LITE

  // If true, then DB::Open / CreateColumnFamily / DropColumnFamily
  // / SetOptions will fail if options file is not detected or properly
  // persisted.
  //
  // DEFAULT: false
  bool fail_if_options_file_error = false;

  // If true, then print malloc stats together with rocksdb.stats
  // when printing to LOG.
  // DEFAULT: false
  bool dump_malloc_stats = false;

  // By default RocksDB replay WAL logs and flush them on DB open, which may
  // create very small SST files. If this option is enabled, RocksDB will try
  // to avoid (but not guarantee not to) flush during recovery. Also, existing
  // WAL logs will be kept, so that if crash happened before flush, we still
  // have logs to recover from.
  //
  // DEFAULT: false
  bool avoid_flush_during_recovery = false;

  // By default RocksDB will flush all memtables on DB close if there are
  // unpersisted data (i.e. with WAL disabled) The flush can be skip to speedup
  // DB close. Unpersisted data WILL BE LOST.
  //
  // DEFAULT: false
  //
  // Dynamically changeable through SetDBOptions() API.
  bool avoid_flush_during_shutdown = false;

  uint64_t batch_group_slot_array_size = 5;

  uint64_t batch_group_max_group_size = 80;

  uint64_t batch_group_max_leader_wait_time_us = 200;

  uint64_t concurrent_writable_file_buffer_num = 4;

  uint64_t concurrent_writable_file_single_buffer_size = 4 * 1024U * 1024U;

  uint64_t concurrent_writable_file_buffer_switch_limit = 512 * 1024U;

  bool use_direct_write_for_wal = true;

  bool query_trace_enable_count = true;
  bool query_trace_print_stats = false;
  uint64_t mutex_backtrace_threshold_ns = 100000000; // 100ms
  bool auto_shrink_enabled = true;
  uint64_t max_free_extent_percent = 10; //10%
  uint64_t shrink_allocate_interval = 60 * 60; //1 hour
  uint64_t max_shrink_extent_count = 512;
  uint64_t total_max_shrink_extent_count = 15 * 512;
  uint64_t idle_tasks_schedule_time = 60; // 60s
  uint64_t table_cache_size = 1 * 1024 * 1024 * 1024; // 1GB
  uint64_t auto_shrink_schedule_interval = 60 * 60; // 1 hour
  uint64_t estimate_cost_depth = 0;
};

// Options to control the behavior of a database (passed to DB::Open)
struct Options : public DBOptions, public ColumnFamilyOptions {
  // Create an Options object with default values for all fields.
  Options() : DBOptions(), ColumnFamilyOptions() {}

  Options(const DBOptions& db_options,
          const ColumnFamilyOptions& column_family_options)
      : DBOptions(db_options), ColumnFamilyOptions(column_family_options) {}

  void Dump() const;

  void DumpCFOptions() const;

  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  Options* OptimizeForSmallDb();
};

//
// An application can issue a read request (via Get/Iterators) and specify
// if that read should process data that ALREADY resides on a specified cache
// level. For example, if an application specifies kBlockCacheTier then the
// Get call will process data that is already processed in the memtable or
// the block cache. It will not page in data from the OS cache or data that
// resides in storage.
enum ReadTier {
  kReadAllTier = 0x0,     // data in memtable, block cache, OS cache or storage
  kBlockCacheTier = 0x1,  // data in memtable or block cache
  kPersistedTier = 0x2,   // persisted data.  When WAL is disabled, this option
                          // will skip data in memtable.
                          // Note that this ReadTier currently only supports
                          // Get and MultiGet and does not support iterators.
  kMemtableTier = 0x3     // data in memtable. used for memtable-only iterators.
};
enum ReadLevel{
  kAll = 0x0, // read all level data
  kOnlyL2 ,   // only read L2 level data
  kExcludeL2  // read all data except L2 data
};
// Options that control read operations
struct ReadOptions {
  // If true, all data read from underlying storage will be
  // verified against corresponding checksums.
  // Default: true
  bool verify_checksums;

  // Should the "data block"/"index block"/"filter block" read for this
  // iteration be cached in memory?
  // Callers may wish to set this field to false for bulk scans.
  // Default: true
  bool fill_cache;

  // If this option is set and memtable implementation allows, Seek
  // might only return keys with the same prefix as the seek-key
  //
  // ! NOT SUPPORTED ANYMORE: prefix_seek is on by default when prefix_extractor
  // is configured
  // bool prefix_seek;

  // If "snapshot" is non-nullptr, read as of the supplied snapshot
  // (which must belong to the DB that is being read and which must
  // not have been released).  If "snapshot" is nullptr, use an implicit
  // snapshot of the state at the beginning of this read operation.
  // Default: nullptr
  const db::Snapshot* snapshot;

  // "iterate_upper_bound" defines the extent upto which the forward iterator
  // can returns entries. Once the bound is reached, Valid() will be false.
  // "iterate_upper_bound" is exclusive ie the bound value is
  // not a valid entry.  If iterator_extractor is not null, the Seek target
  // and iterator_upper_bound need to have the same prefix.
  // This is because ordering is not guaranteed outside of prefix domain.
  // There is no lower bound on the iterator. If needed, that can be easily
  // implemented
  //
  // Default: nullptr
  const Slice* iterate_upper_bound;

  // Specify if this read request should process data that ALREADY
  // resides on a particular cache. If the required data is not
  // found at the specified cache, then Status::Incomplete is returned.
  // Default: kReadAllTier
  ReadTier read_tier;

  // Enable a total order seek regardless of index format (e.g. hash index)
  // used in the table. Some table format (e.g. plain table) may not support
  // this option.
  // If true when calling Get(), we also skip prefix bloom when reading from
  // block based table. It provides a way to read existing data after
  // changing implementation of prefix extractor.
  bool total_order_seek;

  // Keep the blocks loaded by the iterator pinned in memory as long as the
  // iterator is not deleted, If used when reading from tables created with
  // BlockBasedTableOptions::use_delta_encoding = false,
  // Iterator's property "smartengine.iterator.is-key-pinned" is guaranteed to
  // return 1.
  // Default: false
  bool pin_data;

  // If true, when PurgeObsoleteFile is called in CleanupIteratorState, we
  // schedule a background job in the flush job queue and delete obsolete files
  // in background.
  // Default: false
  bool background_purge_on_iterator_cleanup;

  // If non-zero, NewIterator will create a new table reader which
  // performs reads of the given size. Using a large size (> 2MB) can
  // improve the performance of forward iteration on spinning disks.
  // Default: 0
  size_t readahead_size;

  // A threshold for the number of keys that can be skipped before failing an
  // iterator seek as incomplete. The default value of 0 should be used to
  // never fail a request as incomplete, even on skipping too many keys.
  // Default: 0
  uint64_t max_skippable_internal_keys;

  // When checking uk seq validation, should not skip deleted record
  bool skip_del_;
  bool unique_check_ = false;

  ReadLevel read_level_;

  ReadOptions();
  ReadOptions(bool cksum, bool cache);
};

// Options that control write operations
struct WriteOptions {
  // If true, the write will be flushed from the operating system
  // buffer cache (by calling WritableFile::Sync()) before the write
  // is considered complete.  If this flag is true, writes will be
  // slower.
  //
  // If this flag is false, and the machine crashes, some recent
  // writes may be lost.  Note that if it is just the process that
  // crashes (i.e., the machine does not reboot), no writes will be
  // lost even if sync==false.
  //
  // In other words, a DB write with sync==false has similar
  // crash semantics as the "write()" system call.  A DB write
  // with sync==true has similar crash semantics to a "write()"
  // system call followed by "fdatasync()".
  //
  // Default: false
  bool sync;

  // If true, writes will not first go to the write ahead log,
  // and the write may got lost after a crash.
  bool disableWAL;

  // If true and if user is trying to write to column families that don't exist
  // (they were dropped),  ignore the write (don't return an error). If there
  // are multiple writes in a WriteBatch, other writes will succeed.
  // Default: false
  bool ignore_missing_column_families;

  // If true and we need to wait or sleep for the write request, fails
  // immediately with Status::Incomplete().
  bool no_slowdown;

  bool async_commit;

  WriteOptions()
      : sync(false),
        disableWAL(false),
        ignore_missing_column_families(false),
        no_slowdown(false),
        async_commit(false) {}
};

// Options that control flush operations
struct FlushOptions {
  // If true, the flush will wait until the flush is done.
  // Default: true
  bool wait;

  FlushOptions() : wait(true) {}
};

}  // namespace common
}  // namespace smartengine

#endif  // STORAGE_ROCKSDB_INCLUDE_OPTIONS_H_
