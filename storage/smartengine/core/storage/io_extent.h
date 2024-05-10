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

#include "storage/storage_common.h"
#include "util/aio_wrapper.h"

namespace smartengine
{
namespace storage
{
class IOExtent
{
public:
  IOExtent() : is_inited_(false), io_info_() {}
  virtual ~IOExtent() {}

  virtual int init(const ExtentIOInfo &io_info);
  virtual void reset();
  virtual int prefetch(const int64_t offset, const int64_t size, util::AIOHandle *aio_handle) { return common::Status::kNotSupported; }
  virtual int read(const int64_t offset, const int64_t size, char *buf, util::AIOHandle *aio_handle, common::Slice &result) { return common::Status::kNotSupported; }
  virtual int append(const common::Slice &data) { return common::Status::kNotSupported; }
  virtual ExtentId get_extent_id() const { return io_info_.get_extent_id(); }
  virtual int64_t get_unique_id(char *id, const int64_t max_size) const;

  DECLARE_AND_DEFINE_TO_STRING(KV_(io_info))
protected:
  bool is_aligned(const int64_t offset, const int64_t size, const char *buf) const;

protected:
  bool is_inited_;
  ExtentIOInfo io_info_;
};

class WritableExtent : public IOExtent
{
public:
  WritableExtent() : curr_offset_(0) {}
  virtual ~WritableExtent() override {}

  virtual void reset() override;
  virtual int append(const common::Slice &data) override;

private:
  int direct_write(const int fd, const int64_t offset, const char *buf, const int64_t size);

private:
  int64_t curr_offset_;
};

class ReadableExtent : public IOExtent
{
public:
  ReadableExtent() {}
  virtual ~ReadableExtent() override {}

  virtual int prefetch(const int64_t offset, const int64_t size, util::AIOHandle *aio_handle) override;
  virtual int read(const int64_t offset, const int64_t size, char *buf, util::AIOHandle *aio_handle, common::Slice &result) override;

protected:
  int sync_read(const int64_t offset, const int64_t size, char *buf, common::Slice &result);
  int async_read(const int64_t offset, const int64_t size, char *buf, util::AIOHandle *aio_handle, common::Slice &result);
  // convert the offset int the extent to the offset in the data file.
  int fill_aio_info(const int64_t offset, const int64_t size, util::AIOInfo &aio_info) const;

private:
  int align_to_direct_read(const int fd, const int64_t offset, const int64_t size, char *buf);
  int direct_read(const int fd, const int64_t offset, const int64_t size, char *buf);
};

class FullPrefetchExtent : public ReadableExtent
{
public:
  FullPrefetchExtent() : aio_(nullptr), aio_req_() {}
  virtual ~FullPrefetchExtent() override {}

  virtual int init(const ExtentIOInfo &io_info) override;
  virtual void reset() override;
  virtual int full_prefetch();
  virtual int read(const int64_t offset, const int64_t size, char *buf, util::AIOHandle *aio_handle, common::Slice &result) override;

private:
  int reap();

private:
  util::AIO *aio_;
  util::AIOReq aio_req_;
};

}  // storage
}  // smartengine
