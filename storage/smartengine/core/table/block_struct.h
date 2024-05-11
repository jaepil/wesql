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

#pragma once

#include "options/advanced_options.h"
#include "util/serialization.h"
#include "util/to_string.h"
#include "util/types.h"

namespace smartengine
{
namespace storage
{
class IOExtent;
} // namespace storage
namespace util
{
struct AIOHandle;
} // namespace util
namespace table
{
struct RowBlock;

enum ChecksumType : int8_t
{
  kNoChecksum = 0x0,  // not yet supported. Will fail
  kCRC32c = 0x1,
  kxxHash = 0x2,
};

struct BlockHandle
{
  static const int64_t BLOCK_HANDLE_VERSION = 1;

  uint32_t offset_;
  uint32_t size_;
  uint32_t raw_size_;
  uint32_t checksum_;
  int8_t compress_type_;

  BlockHandle();
  BlockHandle(const BlockHandle &block_handle);
  ~BlockHandle();
  BlockHandle &operator=(const BlockHandle &block_handle);

  void reset();
  bool is_valid() const;

  DECLARE_TO_STRING()
  DECLARE_COMPACTIPLE_SERIALIZATION(BLOCK_HANDLE_VERSION)
};

struct BlockInfo
{
  static const int64_t BLOCK_INFO_VERSION = 1;

  BlockHandle handle_;
  std::string first_key_;
  int32_t row_count_;
  int32_t delete_row_count_;
  int32_t single_delete_row_count_;
  common::SequenceNumber smallest_seq_;
  common::SequenceNumber largest_seq_;

  BlockInfo();
  BlockInfo(const BlockInfo &block_info);
  ~BlockInfo();
  BlockInfo &operator=(const BlockInfo &block_info);

  void reset();
  bool is_valid() const;
  inline uint32_t get_offset() const { return handle_.offset_; }
  inline uint32_t get_size() const {return handle_.size_; }
  inline int64_t get_delete_percent() const { return ((delete_row_count_ + single_delete_row_count_) * 100) / row_count_; }
  int64_t get_max_serialize_size() const;

  DECLARE_TO_STRING()
  DECLARE_COMPACTIPLE_SERIALIZATION(BLOCK_INFO_VERSION)
};


class BlockIOHelper
{
public:
  // The read result block don't include block trailer
  static int read_block(storage::IOExtent *extent,
                        const BlockHandle &handle,
                        util::AIOHandle *aio_handle,
                        common::Slice &block);
  
  static int read_and_uncompress_block(storage::IOExtent *extent,
                                       const BlockHandle &handle,
                                       const int64_t mod_id,
                                       util::AIOHandle *aio_handle,
                                       RowBlock *&block);

};

} // namespace table
} // namespace smartengine