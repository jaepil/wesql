/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "table/extent_writer.h"

#include "cache/row_cache.h"
#include "memory/base_malloc.h"
#include "storage/change_info.h"
#include "storage/extent_meta_manager.h"
#include "storage/extent_space_manager.h"
#include "storage/storage_meta_struct.h"
#include "table/large_object.h"
#include "util/crc32c.h"

namespace smartengine
{
using namespace cache;
using namespace common;
using namespace db;
using namespace memory;
using namespace monitor;
using namespace storage;
using namespace util;

namespace table
{
ExtentWriterArgs::ExtentWriterArgs()
    : index_id_(0),
      table_space_id_(0),
      block_size_(0),
      block_restart_interval_(0),
      extent_space_type_(storage::FILE_EXTENT_SPACE),
      internal_key_comparator_(nullptr),
      output_position_(),
      block_cache_(nullptr),
      row_cache_(nullptr),
      compress_type_(kNoCompression),
      change_info_(nullptr)
{}

ExtentWriterArgs::ExtentWriterArgs(const int64_t index_id,
                                    const int64_t table_space_id,
                                    const int64_t block_size,
                                    const int64_t block_restart_interval,
                                    const int32_t extent_space_type,
                                    const InternalKeyComparator *internal_key_comparator,
                                    const LayerPosition &output_position,
                                    cache::Cache *block_cache,
                                    cache::RowCache *row_cache,
                                    const CompressionType compress_type,
                                    ChangeInfo *change_info)
    : index_id_(index_id),
      table_space_id_(table_space_id),
      block_size_(block_size),
      block_restart_interval_(block_restart_interval),
      extent_space_type_(extent_space_type),
      internal_key_comparator_(internal_key_comparator),
      output_position_(output_position),
      block_cache_(block_cache),
      row_cache_(row_cache),
      compress_type_(compress_type),
      change_info_(change_info)
{}

ExtentWriterArgs::~ExtentWriterArgs()
{}

bool ExtentWriterArgs::is_valid() const
{
  //TODO(Zhao Dongsheng) : check compress type valid and row cache maybe nullptr
  //and block cache also can be nullptr.
  return index_id_ >= 0 && table_space_id_ >= 0 &&
         block_size_ > 0 && block_restart_interval_ >= 0 &&
         is_valid_extent_space_type(extent_space_type_) &&
         IS_NOTNULL(internal_key_comparator_) && output_position_.is_valid() &&
         IS_NOTNULL(change_info_);
}

DEFINE_TO_STRING(ExtentWriterArgs, KV_(index_id), KV_(table_space_id),
    KV_(block_size), KV_(block_restart_interval), KV_(extent_space_type),
    KVP_(internal_key_comparator), KV_(output_position), KVP_(block_cache),
    KVP_(row_cache), KVE_(compress_type), KVP_(change_info))

ExtentWriter::ExtentWriter()
    : is_inited_(false),
      index_id_(-1),
      table_space_id_(-1),
      block_size_(0),
      extent_space_type_(FILE_EXTENT_SPACE),
      compress_type_(common::kNoCompression),
      layer_position_(),
      internal_key_comparator_(nullptr),
      block_cache_(nullptr),
      row_cache_(nullptr),
      compress_helper_(ModId::kExtentWriter),
      block_info_(),
      extent_info_(),
      writed_extent_infos_(),
      last_key_(),
      buf_(),
      footer_(),
      index_block_writer_(),
      data_block_writer_(),
      change_info_(nullptr),
      migrate_flag_(false)
{}

ExtentWriter::~ExtentWriter()
{
  destroy();
}

int ExtentWriter::init(const ExtentWriterArgs &args)
{
  int ret = Status::kOk;
  char *extent_buf = nullptr;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "ExtentWriter has been inited", K(ret));
  } else if (UNLIKELY(!args.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(args));
  } else if (FAILED(index_block_writer_.init())) {
    SE_LOG(WARN, "fail to init index block writer", K(ret));
  } else if (FAILED(data_block_writer_.init(args.block_restart_interval_))) {
    SE_LOG(WARN, "fail to init data block writer", K(ret));
  } else if (IS_NULL(extent_buf = reinterpret_cast<char *>(base_memalign(
      DIOHelper::DIO_ALIGN_SIZE, MAX_EXTENT_SIZE, ModId::kExtentWriter)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate extent buf", K(ret));
  } else {
    buf_.assign(extent_buf, MAX_EXTENT_SIZE, 0 /**pos*/);
    index_id_ = args.index_id_;
    table_space_id_ = args.table_space_id_;
    extent_space_type_ = args.extent_space_type_;
    compress_type_ = args.compress_type_;
    layer_position_ = args.output_position_;
    block_size_ = args.block_size_;
    internal_key_comparator_ = args.internal_key_comparator_;
    block_cache_ = args.block_cache_;
    row_cache_ = args.row_cache_;
    change_info_ = args.change_info_;

    is_inited_ = true;
  }

  // resource clean
  if (FAILED(ret)) {
    if (IS_NOTNULL(extent_buf)) {
      base_memalign_free(extent_buf);
      extent_buf = nullptr;
    }
  }

  return ret;
}

void ExtentWriter::destroy()
{
  if (is_inited_) {
    se_assert(nullptr != buf_.data());
    base_memalign_free((buf_.data()));
    is_inited_ = false;
  }
}

int ExtentWriter::append_row(const Slice &key, const Slice &value)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentWriter should be inited first", K(ret));
  } else if (UNLIKELY(key.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(key.empty()));
  } else {
    if (value.size() > LargeObject::LARGE_OBJECT_THRESHOLD_SIZE) {
      if (FAILED(append_large_row(key, value))) {
        SE_LOG(WARN, "fail to append large object", K(ret));
      }
    } else {
      if (FAILED(append_normal_row(key, value))) {
        SE_LOG(WARN, "fail to append normal row", K(ret));
      }
    }
  }

  return ret;
}

int ExtentWriter::append_block(const Slice &block, const Slice &block_info, const Slice &last_key)
{
  int ret = Status::kOk;
  BlockInfo original_block_info;
  int64_t pos = 0;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentWriter should be inited", K(ret));
  } else if (UNLIKELY(block.empty()) || UNLIKELY(block_info.empty()) || UNLIKELY(last_key.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(block.empty()), K(block_info.empty()), K(last_key.empty()));
  } else if (FAILED(original_block_info.deserialize(block_info.data(), block_info.size(), pos))) {
    SE_LOG(WARN, "fail to deserialize block index value", K(ret));
  } else if (FAILED(prepare_append_block(block, original_block_info, last_key))) {
    SE_LOG(WARN, "fail to prepare for appending block", K(ret));
  } else if (FAILED(inner_append_block(block, original_block_info, last_key))) {
    SE_LOG(WARN, "fail to append block", K(ret));
  } else {
    // succeed
  }

  return ret;
}

int ExtentWriter::finish(std::vector<ExtentInfo> *extent_infos)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kOk;
    SE_LOG(WARN, "ExtentWriter should be inited", K(ret));
  } else if (FAILED(write_extent())) {
    SE_LOG(WARN, "fail to write extent", K(ret));
  } else {
    if (IS_NOTNULL(extent_infos)) {
      *extent_infos = writed_extent_infos_;
      writed_extent_infos_.clear();
    }
  }

  return ret;
}

int ExtentWriter::rollback()
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentWriter should be inited", K(ret));
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < writed_extent_infos_.size(); ++i) {
      const ExtentInfo &extent_info = writed_extent_infos_.at(i);
      if (FAILED(ExtentSpaceManager::get_instance().recycle(table_space_id_,
                                                            extent_space_type_,
                                                            extent_info.extent_id_))) {
        SE_LOG(WARN, "fail to recycle extent meta", K(ret), K(extent_info));
      }
    }
  }

  return ret;
}

bool ExtentWriter::is_empty() const
{
  return writed_extent_infos_.empty() && is_current_extent_empty();
}

#ifndef NDEBUG
int ExtentWriter::test_force_flush_data_block()
{
  return write_data_block();
}
#endif

int ExtentWriter::append_normal_row(const Slice &key, const Slice &value)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    SE_LOG(WARN, "ExtentWriter should be inited", K(ret));
  } else if (FAILED(prepare_append_row(key, value))) {
    SE_LOG(WARN, "fail to prepare append row", K(ret));
  } else if (FAILED(inner_append_row(key, value))) {
    SE_LOG(WARN, "fail to inner append row", K(ret));
  }

  return ret;
}

int ExtentWriter::append_large_row(const Slice &key, const Slice &value)
{
  int ret = Status::kOk;
  LargeObject large_object;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentWriter should be inited", K(ret));
  } else if (FAILED(convert_to_large_object_format(key, value, large_object))) {
    SE_LOG(WARN, "fail to convert to large object format", K(ret));
  } else if (FAILED(write_large_object(large_object))) {
    SE_LOG(WARN, "fail to write large object", K(ret));
  } else {
    // succeed
  }

  return ret;
}

int ExtentWriter::prepare_append_row(const Slice &key, const Slice &value)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentWriter should be initialized", K(ret));
  } else if (FAILED(check_key_order(key))) {
    SE_LOG(WARN, "fail to check key order", K(ret), K(key));
  } else if (need_switch_block_for_row(key.size(), value.size()) && FAILED(write_data_block())) {
    SE_LOG(WARN, "fail to switch block", K(ret));
  } else if (need_switch_extent_for_row(key, value) && FAILED(write_extent())) {
    SE_LOG(WARN, "fail to switch extent", K(ret));
  } else {
    // TODO(Zhao Dongsheng) : intro level0 compaction or dump job also need evict row cache?
    if (0 == layer_position_.get_level() && IS_NOTNULL(row_cache_)) {
      if (FAILED(row_cache_->evict(index_id_, key))) {
        SE_LOG(WARN, "fail to evict old version row from row cache", K(ret));
      }
    }
  }

  return ret;
}

int ExtentWriter::inner_append_row(const Slice &key, const Slice &value)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentWriter should be initialized", K(ret));
  } else if (UNLIKELY(key.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(key.empty()));
  } else if (FAILED(data_block_writer_.append(key, value))) {
    SE_LOG(WARN, "fail to append row to block writer", K(ret), K(key), K(value));
  } else if (FAILED(update_block_stats(key))) {
    SE_LOG(WARN, "fail to update block stats", K(ret), K(key));
  } else {
    last_key_.assign(key.data(), key.size());
  }

  return ret;
}

int ExtentWriter::update_block_stats(const Slice &key)
{
  int ret = Status::kOk;
  ParsedInternalKey internal_key;

  if (!ParseInternalKey(key, &internal_key)) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "fail to parse internal key", K(ret), K(key));
  } else if (!IsValueType(internal_key.type)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the key type is unexpected", K(ret), KE(internal_key.type));
  } else {
    // update first key
    if (0 == block_info_.row_count_) {
      block_info_.first_key_.assign(key.data(), key.size());
    }

    // update sequence range
    block_info_.smallest_seq_ = std::min(internal_key.sequence, block_info_.smallest_seq_);
    block_info_.largest_seq_ = std::max(internal_key.sequence, block_info_.largest_seq_);

    // update row count
    ++block_info_.row_count_;
    switch (internal_key.type) {
      case kTypeDeletion:
        ++block_info_.delete_row_count_;
        break;
      case kTypeSingleDeletion:
        ++block_info_.single_delete_row_count_;
        break;
      default:
        // do nothing
        break;
    }
  }

  return ret;  
}

int ExtentWriter::prepare_append_block(const Slice &block,
                                       const BlockInfo &block_info,
                                       const Slice &last_key)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentWriter should be inited", K(ret));
  } else if (UNLIKELY(block.empty()) ||
             UNLIKELY(!block_info.is_valid()) ||
             UNLIKELY(last_key.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(block.empty()), K(block_info), K(last_key));
  } else if (FAILED(check_key_order(block_info.first_key_))) {
    SE_LOG(WARN, "fail to check key order", K(ret));
  // force to close current data block
  } else if (FAILED(write_data_block())) {
    SE_LOG(WARN, "fail to write data block", K(ret));
  } else if (need_switch_extent_for_block(block, block_info, last_key) && FAILED(write_extent())) {
    SE_LOG(WARN, "fail to write extent", K(ret));
  } else {
    // succeed
  }

  return ret;
}

int ExtentWriter::inner_append_block(const Slice &block, const BlockInfo &block_info, const Slice &last_key)
{
  int ret = Status::kOk;
  BlockInfo new_block_info(block_info);
  new_block_info.handle_.offset_ = buf_.size();
  new_block_info.handle_.size_ = block.size();

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentWriter should be inited", K(ret));
  } else if (UNLIKELY(block.empty()) ||
             UNLIKELY(!block_info.is_valid()) ||
             UNLIKELY(last_key.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(block), K(block_info), K(last_key));
  } else if (UNLIKELY(!new_block_info.is_valid())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "new block index value is invalid", K(ret), K(new_block_info));
  } else if (FAILED(buf_.write(block))) {
    SE_LOG(WARN, "fail to write block", K(ret));
  } else if (FAILED(index_block_writer_.append(last_key, new_block_info))) {
    SE_LOG(WARN, "fail to write block index", K(ret));
  } else {
    extent_info_.update(last_key, new_block_info);
    last_key_.assign(last_key.data(), last_key.size());
  }

  return ret;
}

int ExtentWriter::check_key_order(const Slice &key)
{
  int ret = Status::kOk;
  Slice last_key(last_key_);

  if (!is_empty()) {
    if (internal_key_comparator_->Compare(key, last_key) <= 0) {
      ret = Status::kCorruption;
      SE_LOG(WARN, "the key is out of order", K(ret), K(key), K(last_key));
    }
  }

  return ret;
}

bool ExtentWriter::need_switch_block_for_row(const uint32_t key_size, const uint32_t value_size) const
{
  bool res = false;
  const int64_t BLOCK_LOW_WATER_PERCENT = 90;
  // optimize the repeated compute for block_low_water_size if need
  const int64_t block_low_water_size = ((block_size_ * 90) + 99) / 100;

  if (data_block_writer_.current_size() >= block_size_) {
    // block is full
    res = true;
  } else {
    if (data_block_writer_.current_size() >= block_low_water_size
        && data_block_writer_.future_size(key_size, value_size) > block_size_) {
      // block is almost full
      res = true;
    }
  }

  return res;
}

bool ExtentWriter::need_switch_extent_for_row(const Slice &key, const Slice &value) const
{
  int64_t size = SAFE_SPACE_SIZE + Footer::get_max_serialize_size();

  size += buf_.size(); // current extent sze
  size += data_block_writer_.future_size(key.size(), value.size()); // current block future size
  size += index_block_writer_.future_size(key, block_info_);

  return size >= MAX_EXTENT_SIZE;  
}

bool ExtentWriter::need_switch_extent_for_block(const Slice &block, const BlockInfo &block_info, const Slice &last_key)
{
  se_assert(data_block_writer_.is_empty());
  int64_t size = SAFE_SPACE_SIZE + Footer::get_max_serialize_size();

  size += buf_.size(); // current extent size
  size += block.size(); // new block size
  size += index_block_writer_.future_size(last_key, block_info);

  return size >= MAX_EXTENT_SIZE;
}

int ExtentWriter::write_data_block()
{
  int ret = Status::kOk;
  Slice raw_block;
  Slice compressed_block;
  CompressionType compress_type = compress_type_;
  RowBlock *migrating_block = nullptr;
  char *migrating_block_buf = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kOk;
    SE_LOG(WARN, "ExtentWriter should be initialize", K(ret));
  } else if (data_block_writer_.is_empty()) {
    // empty block, do nothing
  } else if (FAILED(data_block_writer_.build(raw_block))) {
    SE_LOG(WARN, "fail to build data block", K(ret));
  } else if (FAILED(compress_helper_.compress(raw_block, compressed_block, compress_type))) {
    SE_LOG(WARN, "fail to compress data block", K(ret));
  } else {
    block_info_.handle_.offset_ = buf_.size();
    block_info_.handle_.size_ = compressed_block.size();
    block_info_.handle_.raw_size_ = raw_block.size();
    block_info_.handle_.compress_type_ = compress_type;
    calculate_block_checksum(compressed_block, block_info_.handle_.checksum_);

    if (FAILED(buf_.write(compressed_block))) {
      SE_LOG(WARN, "fail to write block body", K(ret));
    } else if (FAILED(index_block_writer_.append(last_key_, block_info_))) {
      SE_LOG(WARN, "fail to append block index entry", K(ret));
    } else {
      // Collect handle of block that needs to be migrated.
      if (migrate_flag_) {
        // TODO(Zhao Dongsheng): The blocks currently being migrated to 
        // the block cache are uncompresed. And, ignore the migrate ret?
        if (FAILED(collect_migrating_block(raw_block, block_info_.handle_, kNoCompression))) {
          SE_LOG(WARN, "fail to collect migrating block", K(ret), K_(block_info));
        }
        migrate_flag_ = false;
      }

      // update extent stats
      extent_info_.data_size_ = buf_.size();
      extent_info_.update(Slice(last_key_), block_info_);

      // clear previous block status
      data_block_writer_.reuse();
      block_info_.reset();
    }
  }

  return ret;
}

int ExtentWriter::write_index_block()
{
  int ret = Status::kOk;
  Slice raw_block;
  Slice compressed_block;
  CompressionType compress_type = compress_type_;
  BlockHandle handle;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentWriter should be inited", K(ret));
  } else if (index_block_writer_.is_empty()) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "index block mustn't be empty", K(ret));
  } else if (FAILED(index_block_writer_.build(raw_block))) {
    SE_LOG(WARN, "fail to build index block", K(ret));
  } else if (FAILED(compress_helper_.compress(raw_block, compressed_block, compress_type))) {
    SE_LOG(WARN, "fail to compress index block", K(ret));
  } else {
    handle.offset_ = buf_.size();
    handle.size_ = compressed_block.size();
    handle.raw_size_ = raw_block.size();
    handle.compress_type_ = compress_type;
    calculate_block_checksum(compressed_block, handle.checksum_);

    if (FAILED(buf_.write(compressed_block))) {
      SE_LOG(WARN, "fail to write block body", K(ret));
    } else {
      footer_.index_block_handle_ = handle;
      extent_info_.index_block_handle_ = handle;

      index_block_writer_.reuse();
    }
  }

  return ret;
}

int ExtentWriter::write_footer()
{
  se_assert(buf_.size() < MAX_EXTENT_SIZE);

  int ret = Status::kOk;
  // the space between index block and footer is empty
  int64_t empty_size = MAX_EXTENT_SIZE - (buf_.size() + Footer::get_max_serialize_size()); 
  int64_t pos = 0;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentWriter should be inited", K(ret));
  } else if (UNLIKELY(empty_size < 0)) {
  //} else if (UNLIKELY(empty_size <= 0)) {
    ret = Status::kNoSpace;
    SE_LOG(WARN, "the remain space is not enough to store footer", K(ret),
        "buffer_size", buf_.size(), K(empty_size));
  } else {
    buf_.advance(empty_size);
    if (FAILED(footer_.serialize(buf_.current(), buf_.remain(), pos))) {
      SE_LOG(WARN, "fail to serialize footer", K(ret));
    }
  }

  return ret;
}

int ExtentWriter::write_extent()
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentWriter should be initialized", K(ret));
  } else if (is_current_extent_empty()) {
#ifndef NDEBUG
    SE_LOG(INFO, "current extent is empty");
#endif
  } else if (FAILED(write_data_block())) {
    SE_LOG(WARN, "fail to build and write data block", K(ret));
  } else if (FAILED(write_index_block())) {
    SE_LOG(WARN, "fail to write index block", K(ret));
  } else if (FAILED(write_footer())) {
    SE_LOG(WARN, "fail to write footer", K(ret));
  } else if (FAILED(flush_extent())) {
    SE_LOG(WARN, "fail to flush extent", K(ret));
  } else {
    SE_LOG(INFO, "success to write extent", K_(extent_info));

    extent_info_.reset();
    buf_.reuse();
  }

  return ret;
}

int ExtentWriter::flush_extent()
{
  int ret = Status::kOk;
  storage::WritableExtent writable_extent;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentWriter should be inited", K(ret));
  } else if (FAILED(ExtentSpaceManager::get_instance().allocate(table_space_id_, extent_space_type_, &writable_extent))) {
    SE_LOG(WARN, "fail to allocate extent", K(ret));
  } else if (FAILED(writable_extent.append(Slice(buf_.data(), MAX_EXTENT_SIZE)))) {
    SE_LOG(WARN, "fail to append extent data", K(ret));
  } else {
    extent_info_.table_space_id_ = table_space_id_;
    extent_info_.extent_space_type_ = extent_space_type_;
    extent_info_.extent_id_ = writable_extent.get_extent_id();
    ExtentMeta extent_meta(storage::ExtentMeta::F_NORMAL_EXTENT, extent_info_);
    if (FAILED(write_extent_meta(extent_meta, false /*is_large_object_extent*/))) {
      SE_LOG(WARN, "fail to write extent meta", K(ret));
    } else {
      // Migrate the flagged blocks.This is a best-effort task and should not affect the
      // normal data persistence process.
      migrate_block_cache(writable_extent);
      writed_extent_infos_.push_back(extent_info_);
    }
  }

  return ret;
}

bool ExtentWriter::is_current_extent_empty() const
{
  return (0 == extent_info_.data_block_count_) && data_block_writer_.is_empty();
}

int ExtentWriter::convert_to_large_object_format(const Slice &key, const Slice &value, LargeObject &large_object)
{
  int ret = Status::kOk;

  if (FAILED(convert_to_large_object_key(key, large_object))) {
    SE_LOG(WARN, "fail to convert to large object key", K(ret));
  } else if (FAILED(convert_to_large_object_value(value, large_object))) {
    SE_LOG(WARN, "fail to convert to large object value", K(ret));
  } else {
    if (large_object.get_serialize_size() > LargeObject::MAX_CONVERTED_LARGE_OBJECT_SIZE) {
      ret = Status::kOverLimit;
      SE_LOG(WARN, "the size of coverted large object is still overflowed", K(ret), "key_size", key.size(), "value_size", value.size());
    }
  }

  if (FAILED(ret)) {
    int tmp_ret = Status::kOk;
    if (Status::kOk != (tmp_ret = recycle_large_object_extent(large_object))) {
      SE_LOG(WARN, "fail to recycle large object extent", K(ret), K(tmp_ret));
    }
  }

  return ret;
}

int ExtentWriter::convert_to_large_object_key(const Slice &key, LargeObject &large_object)
{
  int ret= Status::kOk;
  db::ParsedInternalKey internal_key;

  if (!ParseInternalKey(key, &internal_key)) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "the key is corrupted", K(ret));
  } else {
    internal_key.type = db::kTypeValueLarge;
    AppendInternalKey(&(large_object.key_), internal_key);
  }

  return ret;
}

int ExtentWriter::convert_to_large_object_value(const Slice &value, LargeObject &large_object)
{
  int ret = Status::kOk;
  storage::WritableExtent extent;
  storage::ExtentMeta extent_meta;
  CompressionType actual_compress_type = compress_type_;
  Slice compressed_value;
  char *value_buf = nullptr;
  int64_t offset = 0;
  int64_t size = 0;

  if (IS_NULL(value_buf = reinterpret_cast<char *>(memory::base_memalign(
      util::DIOHelper::DIO_ALIGN_SIZE, storage::MAX_EXTENT_SIZE, memory::ModId::kLargeObject)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for large object value", K(ret));
  } else if (FAILED(compress_helper_.compress(value, compressed_value, actual_compress_type))) {
    SE_LOG(WARN, "fail to compress large object value", K(ret));
  } else {
    large_object.value_.compress_type_ = actual_compress_type;
    large_object.value_.raw_size_ = value.size();
    large_object.value_.size_ = compressed_value.size();

    while (SUCCED(ret) && offset < (static_cast<int64_t>(compressed_value.size()))) {
      extent.reset();
      extent_meta.reset();

      size = ((compressed_value.size() - offset) > storage::MAX_EXTENT_SIZE)
             ? storage::MAX_EXTENT_SIZE : (compressed_value.size() - offset);
      memcpy(value_buf, compressed_value.data() + offset, size);

      if (FAILED(storage::ExtentSpaceManager::get_instance().allocate(table_space_id_, extent_space_type_, &extent))) {
        SE_LOG(WARN, "fail to allocate writable extent", K(ret));
      } else if (FAILED(extent.append(Slice(value_buf, DIOHelper::align_size(size))))) {
        SE_LOG(WARN, "fail to append part of large object", K(ret));
      } else if (FAILED(build_large_object_extent_meta(Slice(large_object.key_), extent.get_extent_id(), size, extent_meta))) {
        SE_LOG(WARN, "fail to build large object extent meta", K(ret));
      } else if (FAILED(write_extent_meta(extent_meta, true /*is_large_object_extent*/))) {
        SE_LOG(WARN, "fail to write large object extent meta", K(ret), K(extent_meta));
      } else {
        large_object.value_.extents_.push_back(extent.get_extent_id());
        offset += size;
      }
    }
  }

  //Resource clean
  if (FAILED(ret)) {
    int tmp_ret = Status::kOk;
    if (Status::kOk != (tmp_ret = recycle_large_object_extent(large_object))) {
      SE_LOG(WARN, "fail to recycle large object extents", K(ret));
    }
  }

  if (IS_NOTNULL(value_buf)) {
    base_memalign_free(value_buf);
    value_buf = nullptr;
  }

  return ret;
}

int ExtentWriter::recycle_large_object_extent(LargeObject &large_object)
{
  int ret = Status::kOk;

  for (uint32_t i = 0; SUCCED(ret) && i < large_object.value_.extents_.size(); ++i) {
    if (FAILED(ExtentSpaceManager::get_instance().recycle(table_space_id_, extent_space_type_, large_object.value_.extents_[i]))) {
      SE_LOG(WARN, "fail to recycle large object extent", K(ret), K(i), "extent_id", large_object.value_.extents_[i]);
    }
  }

  // Clear the extent id array, the extent which hasn't been recycled yet will be recycled after restart.
  large_object.value_.extents_.clear();

  return ret;
}

int ExtentWriter::write_large_object(const LargeObject &large_object)
{
  int ret = Status::kOk;
  char *value_buf = nullptr;
  int64_t value_size = large_object.value_.get_serialize_size();
  int64_t pos = 0;

  if (IS_NULL(value_buf = reinterpret_cast<char *>(base_malloc(value_size, ModId::kLargeObject)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for value buf", K(ret));
  } else if (FAILED(large_object.value_.serialize(value_buf, value_size, pos))) {
    SE_LOG(WARN, "fail to serialize large object value", K(ret));
  } else if (FAILED(append_normal_row(Slice(large_object.key_), Slice(value_buf, value_size)))) {
    SE_LOG(WARN, "fail to append converted large object row", K(ret));
  } else {
    // succeed
  }

  if (IS_NOTNULL(value_buf)) {
    base_free(value_buf);
    value_buf = nullptr;
  }

  return ret;
}

int ExtentWriter::build_large_object_extent_meta(const common::Slice &lob_key,
                                                 const storage::ExtentId &extent_id,
                                                 const int64_t data_size,
                                                 storage::ExtentMeta &extent_meta)
{
  int ret = Status::kOk;
  ParsedInternalKey ikey;

  if (!ParseInternalKey(lob_key, &ikey)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "unexpected error, fail to parse internal key", K(ret), K(extent_id));
  } else {
    extent_meta.attr_ = storage::ExtentMeta::F_LARGE_OBJECT_EXTENT;
    extent_meta.smallest_key_.DecodeFrom(lob_key);
    extent_meta.largest_key_.DecodeFrom(lob_key);
    extent_meta.extent_id_ = extent_id;
    extent_meta.smallest_seqno_ = ikey.sequence;
    extent_meta.largest_seqno_ = ikey.sequence;
    extent_meta.refs_ = 0;
    extent_meta.data_size_ = data_size;
    extent_meta.num_data_blocks_ = 1;
    extent_meta.num_entries_ = 1;
    extent_meta.num_deletes_ = 0;
    extent_meta.table_space_id_ = table_space_id_;
    extent_meta.extent_space_type_ = extent_space_type_;
  }

  return ret;
}

int ExtentWriter::write_extent_meta(const storage::ExtentMeta &extent_meta, bool is_large_object_extent)
{
  int ret = Status::kOk;

  if (FAILED(ExtentMetaManager::get_instance().write_meta(extent_meta, true /**write_log*/))) {
    SE_LOG(WARN, "fail to write extent meta", K(ret));
  } else {
    if (is_large_object_extent) {
      if (FAILED(change_info_->add_large_object_extent(extent_meta.extent_id_))) {
        SE_LOG(WARN, "fail to add large object extent to change info", K(ret), K(extent_meta));
      } else {
        SE_LOG(INFO, "success to flush large object extent", "extent_id", extent_meta.extent_id_);
      }
    } else {
      if (FAILED(change_info_->add_extent(layer_position_, extent_meta.extent_id_))) {
        SE_LOG(WARN, "fail to add extent to change info", K(ret));
      } else {
        SE_LOG(INFO, "success to flush normal extent", "extent_id", extent_meta.extent_id_);
      }
    }
  }

  return ret;
}

int ExtentWriter::collect_migrating_block(const Slice &block,
                                          const BlockHandle &block_handle,
                                          const CompressionType &compress_type)
{
  int ret = Status::kOk;
  char *migrating_block_buf = nullptr;
  RowBlock *migrating_block = nullptr;

  if (UNLIKELY(block.empty()) || UNLIKELY(!block_handle.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(block), K(block_handle));
  } else if (IS_NULL(migrating_block_buf = reinterpret_cast<char *>(base_malloc(
      block.size(), ModId::kExtentWriter)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for block", K(ret), "size", block.size());
  } else {
    memcpy(migrating_block_buf, block.data(), block.size());
    if (IS_NULL(migrating_block = MOD_NEW_OBJECT(ModId::kExtentWriter,
                                                 RowBlock,
                                                 migrating_block_buf,
                                                 block.size(),
                                                 compress_type))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for migrating block", K(ret));
    } else if (!(migrating_blocks_.emplace(block_handle.offset_, migrating_block).second)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to emplace migrating block", K(ret), K(block_handle));
    } else {
      // succeed
    }
  }

  // Resource clean.
  if (FAILED(ret)) {
    if (IS_NOTNULL(migrating_block_buf)) {
      base_free(migrating_block_buf);
      migrating_block_buf = nullptr;
    }

    if (IS_NOTNULL(migrating_block)) {
      MOD_DELETE_OBJECT(RowBlock, migrating_block);
    }
  }

  return ret;
}

int ExtentWriter::migrate_block_cache(const WritableExtent &extent)
{
  int ret = Status::kOk;
  CacheEntryKey cache_entry_key;
  char cache_key_buf[CacheEntryKey::MAX_CACHE_KEY_SZIE] = {0};
  Slice cache_key;
  char *cache_value = nullptr;
  RowBlock *block = nullptr;
  
  if (migrating_blocks_.empty() || IS_NULL(block_cache_)) {
    // There are no blocks that need to be migrated.
  } else if (FAILED(cache_entry_key.setup(&extent))) {
    SE_LOG(WARN, "fail to setup cache entry key", K(ret), K(extent));
  } else {
    for (auto iter = migrating_blocks_.begin(); SUCCED(ret) && migrating_blocks_.end() != iter; ++iter) {
      if(IS_NULL(block = iter->second)) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "the migrating block must not nullptr", K(ret), "offset", iter->first);
      } else if (FAILED(cache_entry_key.generate(iter->first, cache_key_buf, cache_key))) {
        SE_LOG(WARN, "fail to generate cache key", K(ret), "offset", iter->first);
      } else if (FAILED(block_cache_->Insert(cache_key,
                                             block,
                                             block->usable_size(),
                                             &CacheEntryHelper::delete_entry<RowBlock>,
                                             nullptr /*handle*/,
                                             Cache::Priority::LOW).code())) {
        SE_LOG(WARN, "fail to insert into block cache", K(ret), "offset", iter->first);
      } else {
        QUERY_COUNT(CountPoint::BLOCK_CACHE_DATA_ADD);
        QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_DATA_BYTES_INSERT, block->usable_size());
        QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD);
        QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_BYTES_WRITE, block->usable_size());
      }
    }
  }

  // Clear the block handles of the current extent to be migrated, regardless of the migration result.
  migrating_blocks_.clear();

  return ret;
}

void ExtentWriter::calculate_block_checksum(const Slice &block, uint32_t &checksum)
{
  checksum = crc32c::Value(block.data(), block.size());
  checksum = crc32c::Mask(checksum);
}

} //namespace table
} //namespace smartengine
