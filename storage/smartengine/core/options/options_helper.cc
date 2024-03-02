/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#include "options/options_helper.h"

#include <cassert>
#include <cctype>
#include <cstdlib>
#include <unordered_set>
#include <vector>
#include "memory/mod_info.h"
#include "memtable/memtablerep.h"
#include "options/options.h"
#include "table/extent_table_factory.h"
#include "table/filter_policy.h"
#include "util/rate_limiter.h"
#include "util/string_util.h"

namespace smartengine {
using namespace table;
using namespace util;
using namespace db;
using namespace storage;
using namespace memtable;
using namespace cache;

namespace common {

DBOptions BuildDBOptions(const ImmutableDBOptions& immutable_db_options,
                         const MutableDBOptions& mutable_db_options) {
  DBOptions options;

  options.create_if_missing = immutable_db_options.create_if_missing;
  options.create_missing_column_families =
      immutable_db_options.create_missing_column_families;
  options.error_if_exists = immutable_db_options.error_if_exists;
  options.paranoid_checks = immutable_db_options.paranoid_checks;
  options.env = immutable_db_options.env;
  options.rate_limiter = immutable_db_options.rate_limiter;
  options.sst_file_manager = immutable_db_options.sst_file_manager;
  options.info_log_level = immutable_db_options.info_log_level;
  options.max_open_files = immutable_db_options.max_open_files;
  options.max_file_opening_threads =
      immutable_db_options.max_file_opening_threads;
  options.max_total_wal_size = mutable_db_options.max_total_wal_size;
  options.statistics = immutable_db_options.statistics;
  options.use_fsync = immutable_db_options.use_fsync;
  options.db_paths = immutable_db_options.db_paths;
  options.db_log_dir = immutable_db_options.db_log_dir;
  options.wal_dir = immutable_db_options.wal_dir;
  options.delete_obsolete_files_period_micros =
      mutable_db_options.delete_obsolete_files_period_micros;
  options.base_background_compactions =
      mutable_db_options.base_background_compactions;
  options.max_background_compactions =
      mutable_db_options.max_background_compactions;
  options.max_background_flushes = immutable_db_options.max_background_flushes;
  options.max_background_dumps = mutable_db_options.max_background_dumps;
  options.dump_memtable_limit_size = mutable_db_options.dump_memtable_limit_size;
  options.max_log_file_size = immutable_db_options.max_log_file_size;
  options.log_file_time_to_roll = immutable_db_options.log_file_time_to_roll;
  options.keep_log_file_num = immutable_db_options.keep_log_file_num;
  options.recycle_log_file_num = immutable_db_options.recycle_log_file_num;
  options.max_manifest_file_size = immutable_db_options.max_manifest_file_size;
  options.table_cache_numshardbits =
      immutable_db_options.table_cache_numshardbits;
  options.WAL_ttl_seconds = immutable_db_options.wal_ttl_seconds;
  options.WAL_size_limit_MB = immutable_db_options.wal_size_limit_mb;
  options.manifest_preallocation_size =
      immutable_db_options.manifest_preallocation_size;
  options.allow_mmap_reads = immutable_db_options.allow_mmap_reads;
  options.allow_mmap_writes = immutable_db_options.allow_mmap_writes;
  options.use_direct_reads = immutable_db_options.use_direct_reads;
  options.use_direct_io_for_flush_and_compaction =
      immutable_db_options.use_direct_io_for_flush_and_compaction;
  options.allow_fallocate = immutable_db_options.allow_fallocate;
  options.is_fd_close_on_exec = immutable_db_options.is_fd_close_on_exec;
  options.advise_random_on_open = immutable_db_options.advise_random_on_open;
  options.db_write_buffer_size = immutable_db_options.db_write_buffer_size;
  options.db_total_write_buffer_size = immutable_db_options.db_total_write_buffer_size;
  options.write_buffer_manager = immutable_db_options.write_buffer_manager;
  options.access_hint_on_compaction_start =
      immutable_db_options.access_hint_on_compaction_start;
  options.new_table_reader_for_compaction_inputs =
      immutable_db_options.new_table_reader_for_compaction_inputs;
  options.compaction_readahead_size =
      immutable_db_options.compaction_readahead_size;
  options.random_access_max_buffer_size =
      immutable_db_options.random_access_max_buffer_size;
  options.writable_file_max_buffer_size =
      immutable_db_options.writable_file_max_buffer_size;
  options.use_adaptive_mutex = immutable_db_options.use_adaptive_mutex;
  options.bytes_per_sync = immutable_db_options.bytes_per_sync;
  options.wal_bytes_per_sync = immutable_db_options.wal_bytes_per_sync;
  options.enable_thread_tracking = immutable_db_options.enable_thread_tracking;
  options.delayed_write_rate = mutable_db_options.delayed_write_rate;
  options.allow_concurrent_memtable_write =
      immutable_db_options.allow_concurrent_memtable_write;
  options.enable_write_thread_adaptive_yield =
      immutable_db_options.enable_write_thread_adaptive_yield;
  options.write_thread_max_yield_usec =
      immutable_db_options.write_thread_max_yield_usec;
  options.write_thread_slow_yield_usec =
      immutable_db_options.write_thread_slow_yield_usec;
  options.skip_stats_update_on_db_open =
      immutable_db_options.skip_stats_update_on_db_open;
  options.wal_recovery_mode = immutable_db_options.wal_recovery_mode;
  options.enable_aio_wal_reader = immutable_db_options.enable_aio_wal_reader;
  options.parallel_wal_recovery = immutable_db_options.parallel_wal_recovery;
  options.parallel_recovery_thread_num = immutable_db_options.parallel_recovery_thread_num;
  options.allow_2pc = immutable_db_options.allow_2pc;
  options.row_cache = immutable_db_options.row_cache;
  options.fail_if_options_file_error =
      immutable_db_options.fail_if_options_file_error;
  options.avoid_flush_during_recovery =
    immutable_db_options.avoid_flush_during_recovery;

  options.avoid_flush_during_shutdown =
      mutable_db_options.avoid_flush_during_shutdown;
  options.dump_malloc_stats = mutable_db_options.dump_malloc_stats;
  options.stats_dump_period_sec = mutable_db_options.stats_dump_period_sec;
  options.batch_group_slot_array_size =
      mutable_db_options.batch_group_slot_array_size;
  options.batch_group_max_group_size =
      mutable_db_options.batch_group_max_group_size;
  options.batch_group_max_leader_wait_time_us =
      mutable_db_options.batch_group_max_leader_wait_time_us;
  options.concurrent_writable_file_buffer_num =
      mutable_db_options.concurrent_writable_file_buffer_num;
  options.concurrent_writable_file_single_buffer_size =
      mutable_db_options.concurrent_writable_file_single_buffer_size;
  options.concurrent_writable_file_buffer_switch_limit =
      mutable_db_options.concurrent_writable_file_buffer_switch_limit;
  options.use_direct_write_for_wal =
      mutable_db_options.use_direct_write_for_wal;
  options.auto_shrink_enabled = mutable_db_options.auto_shrink_enabled;
  options.max_free_extent_percent = mutable_db_options.max_free_extent_percent;
  options.shrink_allocate_interval = mutable_db_options.shrink_allocate_interval;
  options.total_max_shrink_extent_count = mutable_db_options.total_max_shrink_extent_count;
  options.auto_shrink_schedule_interval = mutable_db_options.auto_shrink_schedule_interval;
  options.max_shrink_extent_count = mutable_db_options.max_shrink_extent_count;
  options.estimate_cost_depth = mutable_db_options.estimate_cost_depth;

  return options;
}

ColumnFamilyOptions BuildColumnFamilyOptions(
    const ColumnFamilyOptions& options,
    const MutableCFOptions& mutable_cf_options) {
  ColumnFamilyOptions cf_opts(options);

  // Memtable related options
  cf_opts.write_buffer_size = mutable_cf_options.write_buffer_size;
  cf_opts.flush_delete_percent = mutable_cf_options.flush_delete_percent;
  cf_opts.compaction_delete_percent = mutable_cf_options.compaction_delete_percent;
  cf_opts.flush_delete_percent_trigger = mutable_cf_options.flush_delete_percent_trigger;
  cf_opts.flush_delete_record_trigger = mutable_cf_options.flush_delete_record_trigger;
  cf_opts.max_write_buffer_number = mutable_cf_options.max_write_buffer_number;
  cf_opts.arena_block_size = mutable_cf_options.arena_block_size;
  cf_opts.memtable_huge_page_size = mutable_cf_options.memtable_huge_page_size;

  // Compaction related options
  cf_opts.disable_auto_compactions =
      mutable_cf_options.disable_auto_compactions;
  cf_opts.level0_file_num_compaction_trigger =
      mutable_cf_options.level0_file_num_compaction_trigger;
  cf_opts.level0_layer_num_compaction_trigger =
      mutable_cf_options.level0_layer_num_compaction_trigger;
  cf_opts.level1_extents_major_compaction_trigger =
      mutable_cf_options.level1_extents_major_compaction_trigger;
  cf_opts.level2_usage_percent =
      mutable_cf_options.level2_usage_percent;
  cf_opts.max_compaction_bytes = mutable_cf_options.max_compaction_bytes;
  cf_opts.target_file_size_base = mutable_cf_options.target_file_size_base;
  cf_opts.target_file_size_multiplier =
      mutable_cf_options.target_file_size_multiplier;
  cf_opts.max_bytes_for_level_base =
      mutable_cf_options.max_bytes_for_level_base;
  cf_opts.max_bytes_for_level_multiplier =
      mutable_cf_options.max_bytes_for_level_multiplier;

  // Misc options
  cf_opts.max_sequential_skip_in_iterations =
      mutable_cf_options.max_sequential_skip_in_iterations;
  cf_opts.paranoid_file_checks = mutable_cf_options.paranoid_file_checks;
  cf_opts.report_bg_io_stats = mutable_cf_options.report_bg_io_stats;
  cf_opts.compression = mutable_cf_options.compression;

  cf_opts.table_factory = options.table_factory;
  cf_opts.scan_add_blocks_limit = options.scan_add_blocks_limit;
  cf_opts.bottommost_level = options.bottommost_level;
  cf_opts.compaction_task_extents_limit = options.compaction_task_extents_limit;
  return cf_opts;
}

#ifndef ROCKSDB_LITE

namespace {
template <typename T>
bool ParseEnum(const std::unordered_map<std::string, T>& type_map,
               const std::string& type, T* value) {
  auto iter = type_map.find(type);
  if (iter != type_map.end()) {
    *value = iter->second;
    return true;
  }
  return false;
}

template <typename T>
bool SerializeEnum(const std::unordered_map<std::string, T>& type_map,
                   const T& type, std::string* value) {
  for (const auto& pair : type_map) {
    if (pair.second == type) {
      *value = pair.first;
      return true;
    }
  }
  return false;
}

bool SerializeVectorCompressionType(const std::vector<CompressionType>& types,
                                    std::string* value) {
  std::stringstream ss;
  bool result;
  for (size_t i = 0; i < types.size(); ++i) {
    if (i > 0) {
      ss << ':';
    }
    std::string string_type;
    result = SerializeEnum<CompressionType>(compression_type_string_map,
                                            types[i], &string_type);
    if (result == false) {
      return result;
    }
    ss << string_type;
  }
  *value = ss.str();
  return true;
}

bool ParseVectorCompressionType(
    const std::string& value,
    std::vector<CompressionType>* compression_per_level) {
  compression_per_level->clear();
  size_t start = 0;
  while (start < value.size()) {
    size_t end = value.find(':', start);
    bool is_ok;
    CompressionType type;
    if (end == std::string::npos) {
      is_ok = ParseEnum<CompressionType>(compression_type_string_map,
                                         value.substr(start), &type);
      if (!is_ok) {
        return false;
      }
      compression_per_level->emplace_back(type);
      break;
    } else {
      is_ok = ParseEnum<CompressionType>(
          compression_type_string_map, value.substr(start, end - start), &type);
      if (!is_ok) {
        return false;
      }
      compression_per_level->emplace_back(type);
      start = end + 1;
    }
  }
  return true;
}

bool ParseCompressionOptions(
    const std::string& value, CompressionOptions* compression_options) {
  size_t start = 0;
  size_t end = value.find(':');
  if (end == std::string::npos) {
    return false;
  }
  compression_options->window_bits = ParseInt(value.substr(start, end - start));
  start = end + 1;
  end = value.find(':', start);
  if (end == std::string::npos) {
    return false;
  }
  compression_options->level = ParseInt(value.substr(start, end - start));
  start = end + 1;
  if (start >= value.size()) {
    return false;
  }
  end = value.find(':', start);
  compression_options->strategy = ParseInt(value.substr(start, value.size() - start));
  // max_dict_bytes is optional for backwards compatibility
  if (end != std::string::npos) {
    start = end + 1;
    if (start >= value.size()) {
      return false;
    }
    compression_options->max_dict_bytes = ParseInt(value.substr(start, value.size() - start));
  }
  return true;
}

bool ParseOptionHelper(char* opt_address, const OptionType& opt_type,
                       const std::string& value) {
  switch (opt_type) {
    case OptionType::kBoolean:
      *reinterpret_cast<bool*>(opt_address) = ParseBoolean("", value);
      break;
    case OptionType::kInt:
      *reinterpret_cast<int*>(opt_address) = ParseInt(value);
      break;
    case OptionType::kVectorInt:
      *reinterpret_cast<std::vector<int>*>(opt_address) = ParseVectorInt(value);
      break;
    case OptionType::kUInt:
      *reinterpret_cast<unsigned int*>(opt_address) = ParseUint32(value);
      break;
    case OptionType::kUInt32T:
      *reinterpret_cast<uint32_t*>(opt_address) = ParseUint32(value);
      break;
    case OptionType::kUInt64T:
      *reinterpret_cast<uint64_t*>(opt_address) = ParseUint64(value);
      break;
    case OptionType::kSizeT:
      *reinterpret_cast<size_t*>(opt_address) = ParseSizeT(value);
      break;
    case OptionType::kString:
      *reinterpret_cast<std::string*>(opt_address) = value;
      break;
    case OptionType::kDouble:
      *reinterpret_cast<double*>(opt_address) = ParseDouble(value);
      break;
    case OptionType::kCompressionType:
      return ParseEnum<CompressionType>(
          compression_type_string_map, value,
          reinterpret_cast<CompressionType*>(opt_address));
    case OptionType::kVectorCompressionType:
      return ParseVectorCompressionType(
          value, reinterpret_cast<std::vector<CompressionType>*>(opt_address));
    case OptionType::kChecksumType:
      return ParseEnum<ChecksumType>(
          checksum_type_string_map, value,
          reinterpret_cast<ChecksumType*>(opt_address));
    case OptionType::kBlockBasedTableIndexType:
      return ParseEnum<BlockBasedTableOptions::IndexType>(
          block_base_table_index_type_string_map, value,
          reinterpret_cast<BlockBasedTableOptions::IndexType*>(opt_address));
    case OptionType::kEncodingType:
      return ParseEnum<EncodingType>(
          encoding_type_string_map, value,
          reinterpret_cast<EncodingType*>(opt_address));
    case OptionType::kWALRecoveryMode:
      return ParseEnum<WALRecoveryMode>(
          wal_recovery_mode_string_map, value,
          reinterpret_cast<WALRecoveryMode*>(opt_address));
    case OptionType::kAccessHint:
      return ParseEnum<DBOptions::AccessHint>(
          access_hint_string_map, value,
          reinterpret_cast<DBOptions::AccessHint*>(opt_address));
    case OptionType::kInfoLogLevel:
      return ParseEnum<InfoLogLevel>(
          info_log_level_string_map, value,
          reinterpret_cast<InfoLogLevel*>(opt_address));
    default:
      return false;
  }
  return true;
}

}  // anonymouse namespace

bool GetTableFilterPolicy(const std::string& value,
                          std::shared_ptr<const table::FilterPolicy>* policy) {
  // default empty string
  if (value.empty())
    return true;
  const std::string kName = "bloomfilter:";
  const auto sz = kName.size();
  if (value.compare(0, sz, kName) != 0) {
    return false;
  }
  size_t pos = value.find(':', sz);
  if (pos == std::string::npos) {
    return false;
  }
  int bits_per_key = ParseInt(trim(value.substr(sz, pos - sz)));
  bool use_block_based_builder =
    ParseBoolean("use_block_based_builder", trim(value.substr(pos + 1)));
  policy->reset(NewBloomFilterPolicy(bits_per_key, use_block_based_builder));
  return true;
}

CompressionType get_compression_type(const ImmutableCFOptions &ioptions,
                                     const MutableCFOptions &mutable_cf_options,
                                     const int level)
{
  // If the user has specified a different compression level for each level,
  // then pick the compression for that level.
  if (!ioptions.compression_per_level.empty()) {
    assert(level >= 0 && level <= 2);
    const int n = static_cast<int>(ioptions.compression_per_level.size()) - 1;
    return ioptions.compression_per_level[std::max(0, std::min(level, n))];
  } else {
    return mutable_cf_options.compression;
  }  
}

bool GetVectorCompressionType(const std::string& value,
                              std::vector<CompressionType>* out) {
  if (nullptr == out) {
    return false;
  }
  return ParseVectorCompressionType(value, out);
}

bool GetCompressionOptions(const std::string& value, CompressionOptions* out) {
  if (nullptr == out) {
    return false;
  }
  return ParseCompressionOptions(value, out);
}

bool SerializeSingleOptionHelper(const char* opt_address,
                                 const OptionType opt_type,
                                 std::string* value) {
  assert(value);
  switch (opt_type) {
    case OptionType::kBoolean:
      *value = *(reinterpret_cast<const bool*>(opt_address)) ? "true" : "false";
      break;
    case OptionType::kInt:
      *value = ToString(*(reinterpret_cast<const int*>(opt_address)));
      break;
    case OptionType::kVectorInt:
      return SerializeIntVector(
          *reinterpret_cast<const std::vector<int>*>(opt_address), value);
    case OptionType::kUInt:
      *value = ToString(*(reinterpret_cast<const unsigned int*>(opt_address)));
      break;
    case OptionType::kUInt32T:
      *value = ToString(*(reinterpret_cast<const uint32_t*>(opt_address)));
      break;
    case OptionType::kUInt64T:
      *value = ToString(*(reinterpret_cast<const uint64_t*>(opt_address)));
      break;
    case OptionType::kSizeT:
      *value = ToString(*(reinterpret_cast<const size_t*>(opt_address)));
      break;
    case OptionType::kDouble:
      *value = ToString(*(reinterpret_cast<const double*>(opt_address)));
      break;
    case OptionType::kString:
      *value = EscapeOptionString(
          *(reinterpret_cast<const std::string*>(opt_address)));
      break;
    case OptionType::kCompressionType:
      return SerializeEnum<CompressionType>(
          compression_type_string_map,
          *(reinterpret_cast<const CompressionType*>(opt_address)), value);
    case OptionType::kVectorCompressionType:
      return SerializeVectorCompressionType(
          *(reinterpret_cast<const std::vector<CompressionType>*>(opt_address)),
          value);
      break;
    case OptionType::kTableFactory: {
      const auto* table_factory_ptr =
          reinterpret_cast<const std::shared_ptr<const TableFactory>*>(
              opt_address);
      *value = table_factory_ptr->get() ? table_factory_ptr->get()->Name()
                                        : kNullptrString;
      break;
    }
    case OptionType::kComparator: {
      // it's a const pointer of const Comparator*
      const auto* ptr = reinterpret_cast<const Comparator* const*>(opt_address);
      // Since the user-specified comparator will be wrapped by
      // InternalKeyComparator, we should persist the user-specified one
      // instead of InternalKeyComparator.
      const auto* internal_comparator =
          dynamic_cast<const InternalKeyComparator*>(*ptr);
      if (internal_comparator != nullptr) {
        *value = internal_comparator->user_comparator()->Name();
      } else {
        *value = *ptr ? (*ptr)->Name() : kNullptrString;
      }
      break;
    }
    case OptionType::kMemTableRepFactory: {
      const auto* ptr =
          reinterpret_cast<const std::shared_ptr<MemTableRepFactory>*>(
              opt_address);
      *value = ptr->get() ? ptr->get()->Name() : kNullptrString;
      break;
    }
    case OptionType::kFilterPolicy: {
      const auto* ptr =
          reinterpret_cast<const std::shared_ptr<FilterPolicy>*>(opt_address);
      *value = ptr->get() ? ptr->get()->Name() : kNullptrString;
      break;
    }
    case OptionType::kChecksumType:
      return SerializeEnum<ChecksumType>(
          checksum_type_string_map,
          *reinterpret_cast<const ChecksumType*>(opt_address), value);
    case OptionType::kBlockBasedTableIndexType:
      return SerializeEnum<BlockBasedTableOptions::IndexType>(
          block_base_table_index_type_string_map,
          *reinterpret_cast<const BlockBasedTableOptions::IndexType*>(
              opt_address),
          value);
    case OptionType::kFlushBlockPolicyFactory: {
      const auto* ptr =
          reinterpret_cast<const std::shared_ptr<FlushBlockPolicyFactory>*>(
              opt_address);
      *value = ptr->get() ? ptr->get()->Name() : kNullptrString;
      break;
    }
    case OptionType::kEncodingType:
      return SerializeEnum<EncodingType>(
          encoding_type_string_map,
          *reinterpret_cast<const EncodingType*>(opt_address), value);
    case OptionType::kWALRecoveryMode:
      return SerializeEnum<WALRecoveryMode>(
          wal_recovery_mode_string_map,
          *reinterpret_cast<const WALRecoveryMode*>(opt_address), value);
    case OptionType::kAccessHint:
      return SerializeEnum<DBOptions::AccessHint>(
          access_hint_string_map,
          *reinterpret_cast<const DBOptions::AccessHint*>(opt_address), value);
    case OptionType::kInfoLogLevel:
      return SerializeEnum<InfoLogLevel>(
          info_log_level_string_map,
          *reinterpret_cast<const InfoLogLevel*>(opt_address), value);
    default:
      return false;
  }
  return true;
}

Status GetMutableOptionsFromStrings(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    MutableCFOptions* new_options) {
  assert(new_options);
  *new_options = base_options;
  for (const auto& o : options_map) {
    try {
      auto iter = cf_options_type_info.find(o.first);
      if (iter == cf_options_type_info.end()) {
        return Status::InvalidArgument("Unrecognized option: " + o.first);
      }
      const auto& opt_info = iter->second;
      if (!opt_info.is_mutable) {
        return Status::InvalidArgument("Option not changeable: " + o.first);
      }
      bool is_ok = ParseOptionHelper(
          reinterpret_cast<char*>(new_options) + opt_info.mutable_offset,
          opt_info.type, o.second);
      if (!is_ok) {
        return Status::InvalidArgument("Error parsing " + o.first);
      }
    } catch (std::exception& e) {
      return Status::InvalidArgument("Error parsing " + o.first + ":" +
                                     std::string(e.what()));
    }
  }
  return Status::OK();
}

Status GetMutableDBOptionsFromStrings(
    const MutableDBOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    MutableDBOptions* new_options) {
  assert(new_options);
  *new_options = base_options;
  for (const auto& o : options_map) {
    try {
      auto iter = db_options_type_info.find(o.first);
      if (iter == db_options_type_info.end()) {
        return Status::InvalidArgument("Unrecognized option: " + o.first);
      }
      const auto& opt_info = iter->second;
      if (!opt_info.is_mutable) {
        return Status::InvalidArgument("Option not changeable: " + o.first);
      }
      bool is_ok = ParseOptionHelper(
          reinterpret_cast<char*>(new_options) + opt_info.mutable_offset,
          opt_info.type, o.second);
      if (!is_ok) {
        return Status::InvalidArgument("Error parsing " + o.first);
      }
    } catch (std::exception& e) {
      return Status::InvalidArgument("Error parsing " + o.first + ":" +
                                     std::string(e.what()));
    }
  }
  return Status::OK();
}

bool SerializeSingleDBOption(std::string* opt_string,
                             const DBOptions& db_options,
                             const std::string& name,
                             const std::string& delimiter) {
  auto iter = db_options_type_info.find(name);
  if (iter == db_options_type_info.end()) {
    return false;
  }
  auto& opt_info = iter->second;
  const char* opt_address =
      reinterpret_cast<const char*>(&db_options) + opt_info.offset;
  std::string value;
  bool result = SerializeSingleOptionHelper(opt_address, opt_info.type, &value);
  if (result) {
    *opt_string = name + "=" + value + delimiter;
  }
  return result;
}

bool SerializeSingleColumnFamilyOption(std::string* opt_string,
                                       const ColumnFamilyOptions& cf_options,
                                       const std::string& name,
                                       const std::string& delimiter) {
  auto iter = cf_options_type_info.find(name);
  if (iter == cf_options_type_info.end()) {
    return false;
  }
  auto& opt_info = iter->second;
  const char* opt_address =
      reinterpret_cast<const char*>(&cf_options) + opt_info.offset;
  std::string value;
  bool result = SerializeSingleOptionHelper(opt_address, opt_info.type, &value);
  if (result) {
    *opt_string = name + "=" + value + delimiter;
  }
  return result;
}

bool SerializeSingleBlockBasedTableOption(
    std::string* opt_string, const BlockBasedTableOptions& bbt_options,
    const std::string& name, const std::string& delimiter) {
  auto iter = block_based_table_type_info.find(name);
  if (iter == block_based_table_type_info.end()) {
    return false;
  }
  auto& opt_info = iter->second;
  const char* opt_address =
      reinterpret_cast<const char*>(&bbt_options) + opt_info.offset;
  std::string value;
  bool result = SerializeSingleOptionHelper(opt_address, opt_info.type, &value);
  if (result) {
    *opt_string = name + "=" + value + delimiter;
  }
  return result;
}

Status GetStringFromBlockBasedTableOptions(
    std::string* opt_string, const BlockBasedTableOptions& bbt_options,
    const std::string& delimiter) {
  assert(opt_string);
  opt_string->clear();
  for (auto iter = block_based_table_type_info.begin();
       iter != block_based_table_type_info.end(); ++iter) {
    if (iter->second.verification == OptionVerificationType::kDeprecated) {
      // If the option is no longer used and marked as deprecated,
      // we skip it in the serialization.
      continue;
    }
    std::string single_output;
    bool result = SerializeSingleBlockBasedTableOption(
        &single_output, bbt_options, iter->first, delimiter);
    assert(result);
    if (result) {
      opt_string->append(single_output);
    }
  }
  return Status::OK();
}


Status ParseDBOption(const std::string& name, const std::string& org_value,
                     DBOptions* new_options,
                     bool input_strings_escaped = false) {
  const std::string& value =
      input_strings_escaped ? UnescapeOptionString(org_value) : org_value;
  try {
    if (name == "rate_limiter_bytes_per_sec") {
      new_options->rate_limiter.reset(
          NewGenericRateLimiter(static_cast<int64_t>(ParseUint64(value))));
    } else {
      auto iter = db_options_type_info.find(name);
      if (iter == db_options_type_info.end()) {
        return Status::InvalidArgument("Unrecognized option DBOptions:", name);
      }
      const auto& opt_info = iter->second;
      if (opt_info.verification != OptionVerificationType::kDeprecated &&
          ParseOptionHelper(
              reinterpret_cast<char*>(new_options) + opt_info.offset,
              opt_info.type, value)) {
        return Status::OK();
      }
      switch (opt_info.verification) {
        case OptionVerificationType::kByName:
        case OptionVerificationType::kByNameAllowNull:
          return Status::NotSupported("Deserializing the specified DB option " +
                                      name + " is not supported");
        case OptionVerificationType::kDeprecated:
          return Status::OK();
        default:
          return Status::InvalidArgument(
              "Unable to parse the specified DB option " + name);
      }
    }
  } catch (const std::exception&) {
    return Status::InvalidArgument("Unable to parse DBOptions:", name);
  }
  return Status::OK();
}

std::string ParseBlockBasedTableOption(const std::string& name,
                                       const std::string& org_value,
                                       BlockBasedTableOptions* new_options,
                                       bool input_strings_escaped = false) {
  const std::string& value =
      input_strings_escaped ? UnescapeOptionString(org_value) : org_value;
  if (!input_strings_escaped) {
    // if the input string is not escaped, it means this function is
    // invoked from SetOptions, which takes the old format.
    if (name == "block_cache") {
      new_options->block_cache = NewLRUCache(ParseSizeT(value), -1, false, 0, 0,
                                             memory::ModId::kDefaultBlockCache);
      return "";
    } else if (name == "block_cache_compressed") {
      new_options->block_cache_compressed =
          NewLRUCache(ParseSizeT(value), -1, false, 0, 0, memory::ModId::kBkCacheCompress);
      return "";
    } else if (name == "filter_policy") {
      // Expect the following format
      // bloomfilter:int:bool
      const std::string kName = "bloomfilter:";
      if (value.compare(0, kName.size(), kName) != 0) {
        return "Invalid filter policy name";
      }
      size_t pos = value.find(':', kName.size());
      if (pos == std::string::npos) {
        return "Invalid filter policy config, missing bits_per_key";
      }
      int bits_per_key =
          ParseInt(trim(value.substr(kName.size(), pos - kName.size())));
      bool use_block_based_builder =
          ParseBoolean("use_block_based_builder", trim(value.substr(pos + 1)));
      new_options->filter_policy.reset(
          NewBloomFilterPolicy(bits_per_key, use_block_based_builder));
      return "";
    }
  }
  const auto iter = block_based_table_type_info.find(name);
  if (iter == block_based_table_type_info.end()) {
    return "Unrecognized option";
  }
  const auto& opt_info = iter->second;
  if (!ParseOptionHelper(reinterpret_cast<char*>(new_options) + opt_info.offset,
                         opt_info.type, value)) {
    return "Invalid value";
  }
  return "";
}

std::string ParsePlainTableOptions(const std::string& name,
                                   const std::string& org_value,
                                   PlainTableOptions* new_option,
                                   bool input_strings_escaped = false) {
  const std::string& value =
      input_strings_escaped ? UnescapeOptionString(org_value) : org_value;
  const auto iter = plain_table_type_info.find(name);
  if (iter == plain_table_type_info.end()) {
    return "Unrecognized option";
  }
  const auto& opt_info = iter->second;
  if (!ParseOptionHelper(reinterpret_cast<char*>(new_option) + opt_info.offset,
                         opt_info.type, value)) {
    return "Invalid value";
  }
  return "";
}

#endif  // !ROCKSDB_LITE

}  // namespace common
}  // namespace smartengine
