/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Currently we support two types of tables: plain table and block-based table.
//   1. Block-based table: this is the default table type that we inherited from
//      LevelDB, which was designed for storing data in hard disk or flash
//      device.
//   2. Plain table: it is one of RocksDB's SST file format optimized
//      for low query latency on pure-memory or really low-latency media.
//
// A tutorial of rocksdb table formats is available here:
//   https://github.com/facebook/rocksdb/wiki/A-Tutorial-of-RocksDB-SST-formats
//
// Example code is also available
//   https://github.com/facebook/rocksdb/wiki/A-Tutorial-of-RocksDB-SST-formats#wiki-examples

#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include "cache/cache.h"
#include "env/env.h"
#include "options/options.h"

namespace smartengine {

namespace db {
class MiniTables;
}

namespace util {
class WritableFileWriter;
class RandomAccessFile;
struct EnvOptions;
class RandomAccessFileReader;
}

namespace common {
struct Options;
}

namespace table {
using std::unique_ptr;

struct TableReaderOptions;
struct TableBuilderOptions;
class TableBuilder;
class TableReader;
class FlushBlockPolicyFactory;
class FilterPolicy;

enum ChecksumType : char {
  kNoChecksum = 0x0,  // not yet supported. Will fail
  kCRC32c = 0x1,
  kxxHash = 0x2,
};

// For advanced user only
struct BlockBasedTableOptions {
  // @flush_block_policy_factory creates the instances of flush block policy.
  // which provides a configurable way to determine when to flush a block in
  // the block based tables.  If not set, table builder will use the default
  // block flush policy, which cut blocks by block size (please refer to
  // `FlushBlockBySizePolicy`).
  std::shared_ptr<FlushBlockPolicyFactory> flush_block_policy_factory;

  // TODO(kailiu) Temporarily disable this feature by making the default value
  // to be false.
  //
  // Indicating if we'd put index/filter blocks to the block cache.
  // If not specified, each "table reader" object will pre-load index/filter
  // block during table initialization.
  bool cache_index_and_filter_blocks = true;

  // If cache_index_and_filter_blocks is enabled, cache index and filter
  // blocks with high priority. If set to true, depending on implementation of
  // block cache, index and filter blocks may be less likely to be eviected
  // than data blocks.
  bool cache_index_and_filter_blocks_with_high_priority = true;

  // if cache_index_and_filter_blocks is true and the below is true, then
  // filter and index blocks are stored in the cache, but a reference is
  // held in the "table reader" object so the blocks are pinned and only
  // evicted from cache when the table reader is freed.
  bool pin_l0_filter_and_index_blocks_in_cache = false;

  // if flush_fill_block_cache is enabled, blocks will be added to cache
  // during flushing
  bool flush_fill_block_cache = true;


  // This option is now deprecated. No matter what value it is set to,
  // it will behave as if hash_index_allow_collision=true.
  bool hash_index_allow_collision = true;

  // Use the specified checksum type. Newly created table files will be
  // protected with this checksum type. Old table files will still be readable,
  // even though they have different checksum type.
  ChecksumType checksum = kCRC32c;

  // Disable block cache. If this is set to true,
  // then no block cache should be used, and the block_cache should
  // point to a nullptr object.
  bool no_block_cache = false;

  // If non-NULL use the specified cache for blocks.
  // If NULL, rocksdb will automatically create and use an 8MB internal cache.
  std::shared_ptr<cache::Cache> block_cache = nullptr;

  // If non-NULL use the specified cache for compressed blocks.
  // If NULL, rocksdb will not use a compressed block cache.
  std::shared_ptr<cache::Cache> block_cache_compressed = nullptr;

  // Approximate size of user data packed per block.  Note that the
  // block size specified here corresponds to uncompressed data.  The
  // actual size of the unit read from disk may be smaller if
  // compression is enabled.  This parameter can be changed dynamically.
  size_t block_size = 4 * 1024;

  // This is used to close a block before it reaches the configured
  // 'block_size'. If the percentage of free space in the current block is less
  // than this specified number and adding a new record to the block will
  // exceed the configured block size, then this block will be closed and the
  // new record will be written to the next block.
  int block_size_deviation = 10;

  // Number of keys between restart points for delta encoding of keys.
  // This parameter can be changed dynamically.  Most clients should
  // leave this parameter alone.  The minimum value allowed is 1.  Any smaller
  // value will be silently overwritten with 1.
  int block_restart_interval = 16;

  // Same as block_restart_interval but used for the index block.
  int index_block_restart_interval = 1;

  // Block size for partitioned metadata. Currently applied to indexes when
  // kTwoLevelIndexSearch is used and to filters when partition_filters is used.
  // Note: Since in the current implementation the filters and index partitions
  // are aligned, an index/filter block is created when eitehr index or filter
  // block size reaches the specified limit.
  // Note: this limit is currently applied to only index blocks; a filter
  // partition is cut right after an index block is cut
  // TODO(myabandeh): remove the note above when filter partitions are cut
  // separately
  uint64_t metadata_block_size = 4096;

  // Note: currently this option requires kTwoLevelIndexSearch to be set as
  // well.
  // TODO(myabandeh): remove the note above once the limitation is lifted
  // TODO(myabandeh): this feature is in experimental phase and shall not be
  // used in production; either remove the feature or remove this comment if
  // it is ready to be used in production.
  // Use partitioned full filters for each SST file
  bool partition_filters = false;

  // Use delta encoding to compress keys in blocks.
  // ReadOptions::pin_data requires this option to be disabled.
  //
  // Default: true
  bool use_delta_encoding = true;

  // If non-nullptr, use the specified filter policy to reduce disk reads.
  // Many applications will benefit from passing the result of
  // NewBloomFilterPolicy() here.
  std::shared_ptr<const FilterPolicy> filter_policy = nullptr;

  // If true, place whole keys in the filter (not just prefixes).
  // This must generally be true for gets to be efficient.
  bool whole_key_filtering = true;

  // Verify that decompressing the compressed block gives back the input. This
  // is a verification mode that we use to detect bugs in compression
  // algorithms.
  bool verify_compression = false;

  // If used, For every data block we load into memory, we will create a bitmap
  // of size ((block_size / `read_amp_bytes_per_bit`) / 8) bytes. This bitmap
  // will be used to figure out the percentage we actually read of the blocks.
  //
  // When this feature is used Tickers::READ_AMP_ESTIMATE_USEFUL_BYTES and
  // Tickers::READ_AMP_TOTAL_READ_BYTES can be used to calculate the
  // read amplification using this formula
  // (READ_AMP_TOTAL_READ_BYTES / READ_AMP_ESTIMATE_USEFUL_BYTES)
  //
  // value  =>  memory usage (percentage of loaded blocks memory)
  // 1      =>  12.50 %
  // 2      =>  06.25 %
  // 4      =>  03.12 %
  // 8      =>  01.56 %
  // 16     =>  00.78 %
  //
  // Note: This number must be a power of 2, if not it will be sanitized
  // to be the next lowest power of 2, for example a value of 7 will be
  // treated as 4, a value of 19 will be treated as 16.
  //
  // Default: 0 (disabled)
  uint32_t read_amp_bytes_per_bit = 0;

  // We currently have three versions:
  // 0 -- This version is currently written out by all RocksDB's versions by
  // default.  Can be read by really old RocksDB's. Doesn't support changing
  // checksum (default is CRC32).
  // 1 -- Can be read by RocksDB's versions since 3.0. Supports non-default
  // checksum, like xxHash. It is written by RocksDB when
  // BlockBasedTableOptions::checksum is something other than kCRC32c. (version
  // 0 is silently upconverted)
  // 2 -- Can be read by RocksDB's versions since 3.10. Changes the way we
  // encode compressed blocks with LZ4, BZip2 and Zlib compression. If you
  // don't plan to run RocksDB before version 3.10, you should probably use
  // this.
  // This option only affects newly written tables. When reading exising tables,
  // the information about version is read from the footer.
  uint32_t format_version = 3;
};

// A base class for table factories.
class TableFactory {
 public:
  virtual ~TableFactory() {}

  // The type of the table.
  //
  // The client of this package should switch to a new name whenever
  // the table format implementation changes.
  //
  // Names starting with "smartengine." are reserved and should not be used
  // by any clients of this package.
  virtual const char* Name() const = 0;

  // Returns a Table object table that can fetch data from file specified
  // in parameter file. It's the caller's responsibility to make sure
  // file is in the correct format.
  //
  // NewTableReader() is called in three places:
  // (1) TableCache::FindTable() calls the function when table cache miss
  //     and cache the table object returned.
  // (2) SstFileReader (for SST Dump) opens the table and dump the table
  //     contents using the interator of the table.
  // (3) DBImpl::AddFile() calls this function to read the contents of
  //     the sst file it's attempting to add
  //
  // table_reader_options is a TableReaderOptions which contain all the
  //    needed parameters and configuration to open the table.
  // file is a file handler to handle the file for the table.
  // file_size is the physical file size of the file.
  // table_reader is the output table reader.
  virtual common::Status NewTableReader(
      const TableReaderOptions& table_reader_options,
      util::RandomAccessFileReader *file, uint64_t file_size,
      TableReader *&table_reader,
      bool prefetch_index_and_filter_in_cache = true,
      memory::SimpleAllocator *arena = nullptr) const = 0;

  // Return a table builder to write to a file for this table type.
  //
  // It is called in several places:
  // (1) When flushing memtable to a level-0 output file, it creates a table
  //     builder (In DBImpl::WriteLevel0Table(), by calling BuildTable())
  // (2) During compaction, it gets the builder for writing compaction output
  //     files in DBImpl::OpenCompactionOutputFile().
  // (3) When recovering from transaction logs, it creates a table builder to
  //     write to a level-0 output file (In DBImpl::WriteLevel0TableForRecovery,
  //     by calling BuildTable())
  // (4) When running Repairer, it creates a table builder to convert logs to
  //     SST files (In Repairer::ConvertLogToTable() by calling BuildTable())
  //
  // Multiple configured can be acceseed from there, including and not limited
  // to compression options. file is a handle of a writable file.
  // It is the caller's responsibility to keep the file open and close the file
  // after closing the table builder. compression_type is the compression type
  // to use in this table.
  virtual TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, util::WritableFileWriter* file) const = 0;

  // suffix Ext means Extent based
  virtual TableBuilder* NewTableBuilderExt(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, db::MiniTables* mtables) const {
    return nullptr;
  }

  // Sanitizes the specified DB Options and common::ColumnFamilyOptions.
  //
  // If the function cannot find a way to sanitize the input DB Options,
  // a non-ok Status will be returned.
  virtual common::Status SanitizeOptions(
      const common::DBOptions& db_opts,
      const common::ColumnFamilyOptions& cf_opts) const = 0;

  // Return a string that contains printable format of table configurations.
  // RocksDB prints configurations at DB Open().
  virtual std::string GetPrintableTableOptions() const = 0;

  // Returns the raw pointer of the table options that is used by this
  // TableFactory, or nullptr if this function is not supported.
  // Since the return value is a raw pointer, the TableFactory owns the
  // pointer and the caller should not delete the pointer.
  //
  // In certan case, it is desirable to alter the underlying options when the
  // TableFactory is not used by any open DB by casting the returned pointer
  // to the right class.   For instance, if BlockBasedTableFactory is used,
  // then the pointer can be casted to BlockBasedTableOptions.
  //
  // Note that changing the underlying TableFactory options while the
  // TableFactory is currently used by any open DB is undefined behavior.
  // Developers should use DB::SetOption() instead to dynamically change
  // options while the DB is open.
  virtual void* GetOptions() { return nullptr; }
};

#ifndef ROCKSDB_LITE
// Create a special table factory that can open either of the supported
// table formats, based on setting inside the SST files. It should be used to
// convert a DB from one table format to another.
// @table_factory_to_write: the table factory used when writing to new files.
// @block_based_table_factory:  block based table factory to use. If NULL, use
//                              a default one.
// @plain_table_factory: plain table factory to use. If NULL, use a default one.
// @cuckoo_table_factory: cuckoo table factory to use. If NULL, use a default
// one.
extern TableFactory* NewAdaptiveTableFactory(
    std::shared_ptr<TableFactory> table_factory_to_write = nullptr,
    std::shared_ptr<TableFactory> block_based_table_factory = nullptr,
    std::shared_ptr<TableFactory> plain_table_factory = nullptr,
    std::shared_ptr<TableFactory> cuckoo_table_factory = nullptr);

#endif  // ROCKSDB_LITE

}  // namespace table
}  // namespace smartengine

namespace smartengine {
namespace table {
extern TableFactory* NewExtentBasedTableFactory(
    const BlockBasedTableOptions& table_options = BlockBasedTableOptions());
}
}
