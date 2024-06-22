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

#include "table/block_struct.h"
#include "table/bloom_filter.h"
#include "logger/log_module.h"
#include "storage/io_extent.h"
#include "table/row_block.h"
#include "util/compress/compressor_helper.h"
#include "util/crc32c.h"
#include "util/se_constants.h"

namespace smartengine
{
using namespace common;
using namespace memory;
using namespace storage;
using namespace util;

namespace table
{

BlockHandle::BlockHandle()
    : offset_(0),
      size_(0),
      raw_size_(0),
      checksum_(0),
      compress_type_(kNoCompression)
{}

BlockHandle::BlockHandle(const BlockHandle &block_handle)
    : offset_(block_handle.offset_),
      size_(block_handle.size_),
      raw_size_(block_handle.raw_size_),
      checksum_(block_handle.checksum_),
      compress_type_(block_handle.compress_type_)
{}

BlockHandle::~BlockHandle() {}

BlockHandle &BlockHandle::operator=(const BlockHandle &block_handle)
{
  offset_ = block_handle.offset_;
  size_ = block_handle.size_;
  raw_size_ = block_handle.raw_size_;
  checksum_ = block_handle.checksum_;
  compress_type_ = block_handle.compress_type_;

  return *this;
}

void BlockHandle::reset()
{
  offset_ = 0;
  size_ = 0;
  raw_size_ = 0;
  checksum_ = 0;
  compress_type_ = kNoCompression;
}

bool BlockHandle::is_valid() const
{
  return size_ > 0 && raw_size_ > 0 &&
         ((kNoCompression != compress_type_ && (raw_size_ > size_)) ||
          (kNoCompression == compress_type_ && (raw_size_ == size_)));
}

DEFINE_TO_STRING(BlockHandle, KV_(offset), KV_(size), KV_(raw_size), KV_(checksum), KV_(compress_type))

DEFINE_COMPACTIPLE_SERIALIZATION(BlockHandle, offset_, size_, raw_size_, checksum_, compress_type_)

BlockInfo::BlockInfo()
    : handle_(),
      first_key_(),
      row_count_(0),
      delete_row_count_(0),
      single_delete_row_count_(0),
      smallest_seq_(common::kMaxSequenceNumber),
      largest_seq_(0),
      per_key_bits_(0),
      probe_num_(0),
      bloom_filter_(),
      column_block_(0),
      max_column_count_(0),
      unit_infos_()
{}

BlockInfo::BlockInfo(const BlockInfo &block_info)
    : handle_(block_info.handle_),
      first_key_(block_info.first_key_),
      row_count_(block_info.row_count_),
      delete_row_count_(block_info.delete_row_count_),
      single_delete_row_count_(block_info.single_delete_row_count_),
      smallest_seq_(block_info.smallest_seq_),
      largest_seq_(block_info.largest_seq_),
      per_key_bits_(block_info.per_key_bits_),
      probe_num_(block_info.probe_num_),
      bloom_filter_(block_info.bloom_filter_),
      column_block_(block_info.column_block_),
      max_column_count_(block_info.max_column_count_),
      unit_infos_(block_info.unit_infos_)
{}

BlockInfo::~BlockInfo() {}

BlockInfo &BlockInfo::operator=(const BlockInfo &block_info)
{
  handle_ = block_info.handle_;
  first_key_ = block_info.first_key_;
  row_count_ = block_info.row_count_;
  delete_row_count_ = block_info.delete_row_count_;
  single_delete_row_count_ = block_info.single_delete_row_count_;
  smallest_seq_ = block_info.smallest_seq_;
  largest_seq_ = block_info.largest_seq_;
  per_key_bits_ = block_info.per_key_bits_;
  probe_num_ = block_info.probe_num_;
  bloom_filter_ = block_info.bloom_filter_;
  column_block_ = block_info.column_block_;
  max_column_count_ = block_info.max_column_count_;
  unit_infos_ = block_info.unit_infos_;

  return *this;
}

void BlockInfo::reset()
{
  handle_.reset();
  first_key_.clear();
  row_count_ = 0;
  delete_row_count_ = 0;
  single_delete_row_count_ = 0;
  smallest_seq_ = common::kMaxSequenceNumber;
  largest_seq_ = 0;
  per_key_bits_ = 0;
  probe_num_ = 0;
  bloom_filter_.clear();
  column_block_ = 0;
  max_column_count_ = 0;
  unit_infos_.clear();
}

bool BlockInfo::is_valid() const
{
  return handle_.is_valid() &&
         !first_key_.empty() &&
         row_count_ > 0 &&
         per_key_bits_ > 0 &&
         probe_num_ > 0 &&
         !bloom_filter_.empty() &&
         max_column_count_ >= 0;
}

int64_t BlockInfo::get_max_serialize_size() const
{
  int64_t size = get_serialize_size() + sizeof(BlockInfo);

  // max bloom filter size.
  int32_t bloom_filter_size = 0;
  int32_t dummy_cache_line_num = 0;
  BloomFilterWriter::calculate_space_size(row_count_ + 1, per_key_bits_, bloom_filter_size, dummy_cache_line_num);
  size += bloom_filter_size;

  // max column unit info size.
  size += sizeof(int64_t) + max_column_count_ * ColumnUnitInfo::get_max_serialize_size();

  return size;
}

DEFINE_TO_STRING(BlockInfo, KV_(handle), KV_(first_key), KV_(handle), KV_(row_count),
    KV_(delete_row_count), KV_(single_delete_row_count), KV_(smallest_seq), KV_(per_key_bits),
    KV_(probe_num), KV_(bloom_filter), KV_(largest_seq), K_(column_block), K_(max_column_count),
    K_(unit_infos))

DEFINE_COMPACTIPLE_SERIALIZATION(BlockInfo, handle_, first_key_, row_count_,
    delete_row_count_, single_delete_row_count_, smallest_seq_, largest_seq_,
    per_key_bits_, probe_num_, bloom_filter_, column_block_, max_column_count_,
    unit_infos_)

} // namespace table
} // namespace smartengine