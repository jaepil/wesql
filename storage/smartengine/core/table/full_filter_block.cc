//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/full_filter_block.h"

#include "table/filter_policy.h"
#include "monitoring/query_perf_context.h"

namespace smartengine {
using namespace common;
using namespace monitor;

namespace table {

FullFilterBlockBuilder::FullFilterBlockBuilder(bool whole_key_filtering,
                                               FilterBitsBuilder* filter_bits_builder)
    : whole_key_filtering_(whole_key_filtering),
      num_added_(0) {
  assert(filter_bits_builder != nullptr);
  filter_bits_builder_.reset(filter_bits_builder);
}

void FullFilterBlockBuilder::Add(const Slice& key) {
  if (whole_key_filtering_) {
    AddKey(key);
  }
}

// Add key to filter if needed
inline void FullFilterBlockBuilder::AddKey(const Slice& key) {
  filter_bits_builder_->AddKey(key);
  num_added_++;
}

Slice FullFilterBlockBuilder::Finish(const BlockHandle& tmp, Status* status) {
  // In this impl we ignore BlockHandle
  *status = Status::OK();
  if (num_added_ != 0) {
    num_added_ = 0;
    return filter_bits_builder_->Finish(&filter_data_);
  }
  return Slice();
}

Slice FullFilterBlockBuilder::Finish(
    std::unique_ptr<char[], memory::ptr_delete<char>>* buf) {
  if (num_added_ != 0) {
    num_added_ = 0;
    return filter_bits_builder_->Finish(buf);
  }
  return Slice();
}

FullFilterBlockReader::FullFilterBlockReader(bool _whole_key_filtering,
                                             char *data,
                                             size_t data_size,
                                             FilterBitsReader* filter_bits_reader,
                                             Statistics* stats)
    : FilterBlockReader(data_size, stats, _whole_key_filtering),
      contents_(data, data_size) {
  assert(filter_bits_reader != nullptr);
  filter_bits_reader_.reset(filter_bits_reader);
  filter_data_.reset(const_cast<char*>(data));
}

FullFilterBlockReader::FullFilterBlockReader(bool _whole_key_filtering,
                                             const Slice& contents,
                                             FilterBitsReader* filter_bits_reader,
                                             Statistics* stats)
    : FilterBlockReader(contents.size(), stats, _whole_key_filtering),
      contents_(contents)
{
  assert(filter_bits_reader != nullptr);
  filter_bits_reader_.reset(filter_bits_reader);
}

//TODO: Zhao Dongsheng, this construct function is confused with upper one
FullFilterBlockReader::FullFilterBlockReader(bool _whole_key_filtering,
                                             BlockContents&& contents,
                                             FilterBitsReader* filter_bits_reader,
                                             Statistics* stats)
    : FullFilterBlockReader(_whole_key_filtering,
                            contents.data,
                            filter_bits_reader,
                            stats)
{
  block_contents_ = std::move(contents);
}

bool FullFilterBlockReader::KeyMayMatch(const Slice& key,
                                        uint64_t block_offset,
                                        const bool no_io,
                                        const Slice* const const_ikey_ptr) {
  assert(block_offset == kNotValid);
  if (!whole_key_filtering_) {
    return true;
  }
  return MayMatch(key);
}

bool FullFilterBlockReader::MayMatch(const Slice& entry) {
  if (contents_.size() != 0) {
    if (filter_bits_reader_->MayMatch(entry)) {
      QUERY_COUNT(CountPoint::BLOOM_SST_HIT);
      return true;
    } else {
      QUERY_COUNT(CountPoint::BLOOM_SST_MISS);
      return false;
    }
  }
  return true;  // remain the same with block_based filter
}

size_t FullFilterBlockReader::ApproximateMemoryUsage() const {
  return contents_.size();
}

void FullFilterBlockReader::BindData() { filter_data_.reset(const_cast<char*>(contents_.data())); }

}  // namespace table
}  // namespace smartengine
