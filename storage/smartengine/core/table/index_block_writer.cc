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

#include "table/index_block_writer.h"

namespace smartengine
{
using namespace common;
using namespace memory;

namespace table
{


IndexBlockWriter::IndexBlockWriter()
    : is_inited_(false),
      buf_(ModId::kIndexBlockWriter),
      block_writer_()
{}

IndexBlockWriter::~IndexBlockWriter()
{}

int IndexBlockWriter::init()
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "IndexBlockWriter has been inited", K(ret));
  } else if (FAILED(block_writer_.init(INDEX_BLOCK_RESTART_INTERVAL))) {
    SE_LOG(WARN, "fail to init block writer", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void IndexBlockWriter::reuse()
{
  buf_.reuse();
  block_writer_.reuse();
}

int IndexBlockWriter::append(const common::Slice &key, const BlockInfo &block_info)
{
  int ret = Status::kOk;
  Slice serialized_value;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "IndexBlockWriter should be inited", K(ret));
  } else if (UNLIKELY(key.empty()) || UNLIKELY(!block_info.is_valid())) {
    SE_LOG(WARN, "invalid argument", K(ret), K(key), K(block_info));
  } else if (FAILED(serialize_block_stats(block_info, serialized_value)))  {
    SE_LOG(WARN, "fail to serialize block index value", K(ret));
  } else if (FAILED(block_writer_.append(key, serialized_value))) {
    SE_LOG(WARN, "fail to append entry to block writer", K(ret));
  } else {
    // succeed
  }

  return ret;
}

int IndexBlockWriter::build(Slice &block)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "IndexBlockWriter should be inited", K(ret));
  } else if (FAILED(block_writer_.build(block))) {
    SE_LOG(WARN, "fail to build block", K(ret));
  } else {
    // succeed
  }

  return ret;
}

bool IndexBlockWriter::is_empty() const { return block_writer_.is_empty(); }

int64_t IndexBlockWriter::future_size(const common::Slice &key, const BlockInfo &block_info) const
{
  return block_writer_.future_size(key.size(), block_info.get_max_serialize_size());
}

int IndexBlockWriter::serialize_block_stats(const BlockInfo &block_info, Slice &serialized_value)
{
  int ret = Status::kOk;
  int64_t pos = 0;
  const int64_t serialize_size = block_info.get_serialize_size();
  buf_.reuse();

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "IndexBlockWriter should be inited", K(ret));
  } else if (UNLIKELY(!block_info.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(block_info));
  } else if (FAILED(buf_.reserve(serialize_size))) {
    SE_LOG(WARN, "fail to reserve serialize buffer", K(ret), K(block_info));
  } else if (FAILED(block_info.serialize(buf_.data(), buf_.capacity(), pos))) {
    SE_LOG(WARN, "fail to serialize block index value", K(ret), K(block_info));
  } else {
    se_assert(serialize_size == pos);
    serialized_value.assign(buf_.data(), pos);
  }

  return ret;
}

} // namespace table
} // namespace smartengine
