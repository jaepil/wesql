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
  IOExtent();
  virtual ~IOExtent();

  virtual int write(const common::Slice &data) = 0;
  virtual int read(util::AIOHandle *aio_handle,
                   int64_t offset,
                   int64_t size,
                   char *buf,
                   common::Slice &result) = 0;
  virtual int prefetch(util::AIOHandle *aio_handle, int64_t offset, int64_t size) = 0;
  virtual ExtentId get_extent_id() const { return extent_id_; }
  virtual int64_t get_unique_id(char *id, const int64_t max_size) const;

  DEFINE_PURE_VIRTUAL_CONSTRUCTOR_SIZE()
protected:
  bool is_inited_;
  ExtentId extent_id_;
  int64_t unique_id_; // for unique identify in cache
};

class FileIOExtent : public IOExtent
{
public:
  FileIOExtent();
  ~FileIOExtent() override;

  int init(const ExtentId &extent_id, int64_t unique_id, int fd);
  int write(const common::Slice &data) override;
  int read(util::AIOHandle *aio_handle, int64_t offset, int64_t size, char *buf, common::Slice &result) override;
  int prefetch(util::AIOHandle *aio_handle, int64_t offset, int64_t size) override; 

  DEFINE_OVERRIDE_CONSTRUCTOR_SIZE()
private:
  inline int64_t get_base_offset() const { return (extent_id_.offset * storage::MAX_EXTENT_SIZE); }
  // convert the offset int the extent to the offset in the data file.
  int fill_aio_info(int64_t offset, int64_t size, util::AIOInfo &aio_info) const;
  bool is_aligned(int64_t offset, int64_t size, const char *buf) const;
  int align_to_direct_write(int fd, int64_t offset, const char *buf, int64_t size);
  int direct_write(int fd, int64_t offset, const char *buf, int64_t size);
  int sync_read(int64_t offset, int64_t size, char *buf, common::Slice &result);
  int async_read(util::AIOHandle *aio_handle, int64_t offset, int64_t size, char *buf, common::Slice &result);
  int align_to_direct_read(int fd, int64_t offset, int64_t size, char *buf);
  int direct_read(int fd, int64_t offset, int64_t size, char *buf);

protected:
  int fd_;
};

class ObjectIOExtent : public IOExtent
{
public:
  ObjectIOExtent();
  virtual ~ObjectIOExtent() override;

  int init(const ExtentId &extent_id,
           int64_t unique_id,
           ::objstore::ObjectStore *object_store,
           const std::string &bucket);
  int write(const common::Slice &data) override;
  int read(util::AIOHandle *aio_handle, int64_t offset, int64_t size, char *buf, common::Slice &result) override;
  int prefetch(util::AIOHandle *aio_handle, int64_t offset, int64_t size) override; 

  DEFINE_OVERRIDE_CONSTRUCTOR_SIZE()
private:
  int fill_aio_info(util::AIOHandle *aio_handle, int64_t offset, int64_t size, util::AIOInfo &aio_info);
  int write_object(const char *data, int64_t data_size);
  int read_object(int64_t offset, int64_t size, char *buf, common::Slice &result);
  int read_persistent_cache(util::AIOHandle *aio_handle, int64_t offset, int64_t size, char *buf, common::Slice &result);
  int load_extent(cache::Cache::Handle **handle);

protected:
  ::objstore::ObjectStore *object_store_;
  std::string bucket_;
};

}  // namespace storage
}  // namespace smartengine
