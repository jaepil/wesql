/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "options/db_options.h"
#include "util/compression.h"
#include "smartengine/options.h"

namespace smartengine {

namespace table {
class FilterManager;
}

namespace common {

// ImmutableCFOptions is a data struct used by smartengine internal. It contains a
// subset of Options that should not be changed during the entire lifetime
// of DB. Raw pointers defined in this struct do not have ownership to the data
// they point to. Options contains shared_ptr to these data.
struct ImmutableCFOptions {
  ImmutableCFOptions();
  explicit ImmutableCFOptions(const Options& options);

  ImmutableCFOptions(const ImmutableDBOptions& db_options,
                     const ColumnFamilyOptions& cf_options);

  const util::Comparator* user_comparator;
  db::InternalKeyComparator internal_comparator;


  int min_write_buffer_number_to_merge;

  int max_write_buffer_number_to_maintain;

  monitor::Statistics* statistics;

  util::RateLimiter* rate_limiter;

  util::InfoLogLevel info_log_level;

  util::Env* env;

  // Allow the OS to mmap file for reading sst tables. Default: false
  bool allow_mmap_reads;

  // Allow the OS to mmap file for writing. Default: false
  bool allow_mmap_writes;

  std::vector<DbPath> db_paths;

  memtable::MemTableRepFactory* memtable_factory;

  table::TableFactory* table_factory;

  Options::TablePropertiesCollectorFactories
      table_properties_collector_factories;

  bool advise_random_on_open;

  // This options is required by PlainTableReader. May need to move it
  // to PlainTalbeOptions just like bloom_bits_per_key
  uint32_t bloom_locality;

  bool purge_redundant_kvs_while_flush;

  bool use_fsync;

  std::vector<CompressionType> compression_per_level;

  CompressionType bottommost_compression;

  CompressionOptions compression_opts;

  bool level_compaction_dynamic_level_bytes;

  Options::AccessHint access_hint_on_compaction_start;

  bool new_table_reader_for_compaction_inputs;

  size_t compaction_readahead_size;

  int num_levels;

  bool optimize_filters_for_hits;

  bool force_consistency_checks;

  std::shared_ptr<cache::RowCache> row_cache;

  std::shared_ptr<table::FilterManager> filter_manager;
};

struct MutableCFOptions {
  explicit MutableCFOptions(const ColumnFamilyOptions& options)
      : write_buffer_size(options.write_buffer_size),
        flush_delete_percent(options.flush_delete_percent),
        compaction_delete_percent(options.compaction_delete_percent),
        flush_delete_percent_trigger(options.flush_delete_percent_trigger),
        flush_delete_record_trigger(options.flush_delete_record_trigger),
        max_write_buffer_number(options.max_write_buffer_number),
        arena_block_size(options.arena_block_size),
        memtable_huge_page_size(options.memtable_huge_page_size),
        disable_auto_compactions(options.disable_auto_compactions),
        soft_pending_compaction_bytes_limit(
            options.soft_pending_compaction_bytes_limit),
        hard_pending_compaction_bytes_limit(
            options.hard_pending_compaction_bytes_limit),
        level0_file_num_compaction_trigger(
            options.level0_file_num_compaction_trigger),
        level0_layer_num_compaction_trigger(
            options.level0_layer_num_compaction_trigger),
        level1_extents_major_compaction_trigger(
            options.level1_extents_major_compaction_trigger),
        level2_usage_percent(options.level2_usage_percent),
        max_compaction_bytes(options.max_compaction_bytes),
        target_file_size_base(options.target_file_size_base),
        target_file_size_multiplier(options.target_file_size_multiplier),
        max_bytes_for_level_base(options.max_bytes_for_level_base),
        max_bytes_for_level_multiplier(options.max_bytes_for_level_multiplier),
        max_sequential_skip_in_iterations(
            options.max_sequential_skip_in_iterations),
        paranoid_file_checks(options.paranoid_file_checks),
        report_bg_io_stats(options.report_bg_io_stats),
        compression(options.compression),
        background_disable_merge(false),
        scan_add_blocks_limit(options.scan_add_blocks_limit),
        bottommost_level(options.bottommost_level),
        compaction_task_extents_limit(options.compaction_task_extents_limit)
        {
  }

  MutableCFOptions()
      : write_buffer_size(0),
        flush_delete_percent(100),
        compaction_delete_percent(100),
        flush_delete_percent_trigger(700000),
        flush_delete_record_trigger(700000),
        max_write_buffer_number(0),
        arena_block_size(0),
        memtable_huge_page_size(0),
        disable_auto_compactions(false),
        soft_pending_compaction_bytes_limit(0),
        hard_pending_compaction_bytes_limit(0),
        level0_file_num_compaction_trigger(0),
        level0_layer_num_compaction_trigger(0),
        level1_extents_major_compaction_trigger(0),
        level2_usage_percent(0),
        max_compaction_bytes(0),
        target_file_size_base(0),
        target_file_size_multiplier(0),
        max_bytes_for_level_base(0),
        max_bytes_for_level_multiplier(0),
        max_sequential_skip_in_iterations(0),
        paranoid_file_checks(false),
        report_bg_io_stats(false),
        compression(util::Snappy_Supported() ? kSnappyCompression
                                             : kNoCompression),
        background_disable_merge(false),
        scan_add_blocks_limit(0),
        bottommost_level(2),
        compaction_task_extents_limit(512) {}

  void Dump() const;

  // Memtable related options
  size_t write_buffer_size;
  int flush_delete_percent;
  int compaction_delete_percent;
  int flush_delete_percent_trigger;
  int flush_delete_record_trigger;
  int max_write_buffer_number;
  size_t arena_block_size;
  size_t memtable_huge_page_size;

  // Compaction related options
  bool disable_auto_compactions;
  uint64_t soft_pending_compaction_bytes_limit;
  uint64_t hard_pending_compaction_bytes_limit;
  int level0_file_num_compaction_trigger;
  int level0_layer_num_compaction_trigger;
  int level1_extents_major_compaction_trigger;
  int64_t level2_usage_percent;
  uint64_t max_compaction_bytes;
  uint64_t target_file_size_base;
  int target_file_size_multiplier;
  uint64_t max_bytes_for_level_base;
  double max_bytes_for_level_multiplier;
  std::vector<int> max_bytes_for_level_multiplier_additional;

  // Misc options
  uint64_t max_sequential_skip_in_iterations;
  bool paranoid_file_checks;
  bool report_bg_io_stats;
  CompressionType compression;
  bool background_disable_merge;

  uint64_t scan_add_blocks_limit;
  int bottommost_level;
  int compaction_task_extents_limit;
};

}  // namespace common
}  // namespace smartengine
