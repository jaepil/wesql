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
      largest_seq_(0)
{}

BlockInfo::BlockInfo(const BlockInfo &block_info)
    : handle_(block_info.handle_),
      first_key_(block_info.first_key_),
      row_count_(block_info.row_count_),
      delete_row_count_(block_info.delete_row_count_),
      single_delete_row_count_(block_info.single_delete_row_count_),
      smallest_seq_(block_info.smallest_seq_),
      largest_seq_(block_info.largest_seq_)
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
}

bool BlockInfo::is_valid() const
{
  return handle_.is_valid() && !first_key_.empty() && row_count_ > 0;
}

int64_t BlockInfo::get_max_serialize_size() const
{
  return get_serialize_size() + sizeof(BlockInfo);
}

DEFINE_TO_STRING(BlockInfo, KV_(handle), KV_(first_key), KV_(handle), KV_(row_count),
    KV_(delete_row_count), KV_(single_delete_row_count), KV_(smallest_seq),
    KV_(largest_seq))

DEFINE_COMPACTIPLE_SERIALIZATION(BlockInfo, handle_, first_key_, row_count_,
    delete_row_count_, single_delete_row_count_, smallest_seq_, largest_seq_)

int BlockIOHelper::read_block(IOExtent *extent,
                              const BlockHandle &handle,
                              AIOHandle *aio_handle,
                              Slice &block)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  uint32_t actual_checksum = 0; // the checksum actually stored in the block info.
  uint32_t expect_checksum = 0; // the checksum expected based on block data.

  if (IS_NULL(extent) || UNLIKELY(!handle.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(extent), K(handle));
  } else if (IS_NULL(buf = reinterpret_cast<char *>(base_malloc(handle.size_, ModId::kBlock)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for block", K(ret), K(handle));
  } else if (FAILED(extent->read(handle.offset_,
                                 handle.size_,
                                 buf,
                                 aio_handle,
                                 block))) {
    SE_LOG(WARN, "fail to do io of block", K(ret), K(handle), K(handle));
  } else if (UNLIKELY(buf != block.data()) ||
             UNLIKELY(handle.size_ != static_cast<int64_t>(block.size()))) {
    ret = Status::kIOError;
    SE_LOG(WARN, "the io result maybe corrupted", K(ret), KP(buf), K(handle), K(block));
  } else {
    // verify checksum
    actual_checksum = crc32c::Unmask(handle.checksum_);
    expect_checksum = crc32c::Value(block.data(), block.size());

    if (actual_checksum != expect_checksum) {
      ret = Status::kCorruption;
      SE_LOG(ERROR, "the block checksum mismatch", K(ret), K(actual_checksum), K(expect_checksum));
    }
  }

  // release source
  if (FAILED(ret)) {
    if (IS_NOTNULL(buf)) {
      base_free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

int BlockIOHelper::read_and_uncompress_block(storage::IOExtent *extent,
                                             const BlockHandle &handle,
                                             const int64_t mod_id,
                                             util::AIOHandle *aio_handle,
                                             RowBlock *&block)
{
  int ret = Status::kOk;
  Slice compressed_block;
  Slice raw_block;

  if (IS_NULL(extent) || UNLIKELY(!handle.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(extent), K(handle));
  } else if (FAILED(read_block(extent, handle, aio_handle, compressed_block))) {
    SE_LOG(WARN, "fail to read block", K(ret));
  } else {
    // 1. Prepare raw block
    if (kNoCompression == handle.compress_type_) {
      // Use the read result as result block directly.
      raw_block.assign(compressed_block.data(), compressed_block.size());
    } else {
      if (FAILED(UncompressHelper::uncompress(compressed_block,
                                              static_cast<CompressionType>(handle.compress_type_),
                                              mod_id, 
                                              handle.raw_size_, 
                                              raw_block))) {
        SE_LOG(WARN, "fail to uncompress block", K(ret));
      }

      assert(nullptr != compressed_block.data());
      // If the block is actually compressed, the result block is uncompressed.
      // So the original compressed block should be released.
      if (IS_NOTNULL(compressed_block.data())) {
        base_free(const_cast<char *>(compressed_block.data()));
      }
    }

    // 2. Prepare Block
    if (SUCCED(ret)) {
      se_assert(!raw_block.empty());
      if (IS_NULL(block = MOD_NEW_OBJECT(mod_id,
                                         RowBlock,
                                         raw_block.data(),
                                         raw_block.size(),
                                         kNoCompression))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for Block", K(ret));
      }
    }
  }

  return ret;
}

} // namespace table
} // namespace smartengine