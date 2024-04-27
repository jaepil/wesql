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

#include "storage/io_extent.h"
#include <unistd.h>
#include "util/misc_utility.h"

namespace smartengine
{
using namespace util;
using namespace common;
using namespace table;

namespace table
{
extern const uint64_t kExtentBasedTableMagicNumber;
}

namespace storage
{

int IOExtent::init(const ExtentIOInfo &io_info)
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "the extent has been inited", K(ret), K(io_info));
  } else if (UNLIKELY(!io_info.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(io_info));
  } else {
    io_info_ = io_info;
    is_inited_ = true;
  }

  return ret;
}

void IOExtent::reset()
{
  is_inited_ = false;
  io_info_.reset();
}

int64_t IOExtent::get_unique_id(char *id, const int64_t max_size) const
{
  int64_t len = std::min(static_cast<int64_t>(sizeof(io_info_.unique_id_)), max_size);
  memcpy(id, (char *)(&(io_info_.unique_id_)), len);
  return len;
}
bool IOExtent::is_aligned(const int64_t offset, const int64_t size, const char *buf) const
{
  return DIOHelper::is_aligned(offset) &&
         DIOHelper::is_aligned(size) &&
         DIOHelper::is_aligned(reinterpret_cast<std::uintptr_t>(buf));
}

void WritableExtent::reset()
{
  IOExtent::reset();
  curr_offset_ = 0;
}

int WritableExtent::append(const Slice &data)
{
  int ret = Status::kOk;
  int64_t offset = io_info_.get_offset() + curr_offset_;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "WritableExtent should be inited", K(ret));
  } else if (UNLIKELY(IS_NULL(data.data())) || UNLIKELY(0 == data.size())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(data));
  } else if (UNLIKELY((curr_offset_ + static_cast<int64_t>(data.size())) > io_info_.extent_size_)) {
    ret = Status::kOverLimit;
    SE_LOG(WARN, "extent size overlimit", K(ret), K(offset), K_(curr_offset),
      K(data), "data_size", data.size(), K_(io_info));
  } else if (UNLIKELY(!is_aligned(offset, data.size(), data.data()))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the write buf should be aligned", K(ret), K(offset), K_(curr_offset), K(data));
  } else if (FAILED(direct_write(io_info_.fd_, offset, data.data(), data.size()))) {
    SE_LOG(WARN, "fail to direct write", K(ret), K_(io_info), K(offset), K_(curr_offset), K(data));
  } else {
    curr_offset_ += data.size();
  }

  return ret;
}

int WritableExtent::direct_write(const int fd, const int64_t offset, const char *buf, const int64_t size)
{
  assert(fd > 0 && offset >= 0 && size > 0 && (nullptr != buf));
  assert(is_aligned(offset, size, buf));

  int ret = Status::kOk;
  int64_t write_size = 0;
  int64_t total_write_size = 0;
  const char *curr_buf = buf;
  int64_t curr_offset = offset;
  int64_t left_size = size;

  while (SUCCED(ret) && left_size > 0) {
    write_size = pwrite(fd, curr_buf, left_size, curr_offset);

    if (write_size <= 0) {
      if (EINTR == errno) {
        continue;
      } else {
        ret = Status::kIOError;
#ifndef NDEBUG
        SE_LOG(ERROR, "extent io error!", K(ret), K(errno));
#endif
      }
    } else {
      total_write_size += write_size;
      curr_buf = buf + total_write_size;
      curr_offset = offset + total_write_size;
      left_size = size - total_write_size;
    }
  }
  se_assert(size == total_write_size);

  return ret;
}

int ReadableExtent::prefetch(const int64_t offset, const int64_t size, AIOHandle *aio_handle)
{
  int ret = Status::kOk;
  AIOInfo aio_info;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ReadableExtent should be inited", K(ret));
  } else if (UNLIKELY(offset < 0) || UNLIKELY(size <= 0) || IS_NULL(aio_handle)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(offset), K(size), KP(aio_handle));
  } else if (FAILED(fill_aio_info(offset, size, aio_info))) {
    SE_LOG(WARN, "fail to fill aio info", K(ret), K(offset), K(size), K_(io_info));
  } else if (FAILED(aio_handle->prefetch(aio_info))) {
      SE_LOG(WARN, "fail to pretch", K(ret), K(aio_info));
  }

  // Prefetch failed, will try sync IO, overwrite ret here.
  if (FAILED(ret)) {
    ret = Status::kOk;
    aio_handle->aio_req_->status_ = Status::kErrorUnexpected;
    SE_LOG(INFO, "aio prefetch failed, will try sync IO!");
  }

  return ret;
}

int ReadableExtent::read(const int64_t offset, const int64_t size, char *buf, AIOHandle *aio_handle, Slice &result)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ReadableExtent should be inited", K(ret));
  } else if (UNLIKELY(offset < 0) || UNLIKELY(size <= 0) || IS_NULL(buf)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(offset), K(size), KP(buf));
  } else if (UNLIKELY((offset + size) > io_info_.extent_size_)) {
    ret = Status::kOverLimit;
    SE_LOG(WARN, "extent size overflow", K(ret), K(offset), K(size),
        K(buf), K_(io_info));
  } else {
    if (IS_NULL(aio_handle)) {
      // sync read
      if (FAILED(sync_read(offset, size, buf, result))) {
        SE_LOG(WARN, "fail to sync read", K(ret), K(offset), K(size), K_(io_info));
      }
    } else {
      // async read
      if (FAILED(async_read(offset, size, buf, aio_handle, result))) {
        SE_LOG(WARN, "fail to async read", K(ret), K(offset), K(size), K_(io_info));
      }
    }
  }

  return ret;
}

int ReadableExtent::sync_read(const int64_t offset, const int64_t size, char *buf, Slice &result)
{
  int ret = Status::kOk;
  int64_t file_offset = io_info_.get_offset() + offset;

  if (is_aligned(offset, size, buf)) {
    if (FAILED(direct_read(io_info_.fd_, file_offset, size, buf))) {
      SE_LOG(WARN, "fail to direct read", K(ret), K(offset), K(size), K_(io_info));
    }
  } else {
    if (FAILED(align_to_direct_read(io_info_.fd_, file_offset, size, buf))) {
      SE_LOG(WARN, "fail to convert to direct read", K(ret), K(offset), K(size), K_(io_info));
    }
  }

  if (SUCCED(ret)) {
    result.assign(buf, size);
  }

  return ret;
}

int ReadableExtent::async_read(const int64_t offset, const int64_t size, char *buf, AIOHandle *aio_handle, Slice &result)
{
  assert(nullptr != aio_handle);
  int ret = Status::kOk;
  AIOInfo aio_info;

  if (FAILED(fill_aio_info(offset, size, aio_info))) {
    SE_LOG(WARN, "fail to fill aio info", K(ret), K(offset), K(size), K_(io_info));
  } else if (FAILED(aio_handle->read(aio_info.offset_, aio_info.size_, &result, buf))) {
    SE_LOG(WARN, "fail to aio handle read", K(ret), K(offset), K(size), K(aio_info));
    BACKTRACE(ERROR, "aio handle read failed!");
  }

  // AIO read failed, try sync read, overwrite ret here.
  if (FAILED(ret)) {
    if (FAILED(sync_read(offset, size, buf, result))) {
      SE_LOG(WARN, "fail to sync read after async read failed", K(ret), K(offset), K(size), K(aio_info));
    }
  }

  return ret;
}

int ReadableExtent::fill_aio_info(const int64_t offset, const int64_t size, AIOInfo &aio_info) const
{
  int ret = Status::kOk;
  aio_info.reset();

  if (UNLIKELY((offset + size) > io_info_.extent_size_)) {
    ret = Status::kOverLimit;
    SE_LOG(WARN, "extent size overflow!", K(ret), K(offset), K(size), K_(io_info));
  } else {
    aio_info.fd_ = io_info_.fd_;
    aio_info.offset_ = io_info_.get_offset() + offset;
    aio_info.size_ = size;
  }

  return ret;
}

int ReadableExtent::align_to_direct_read(const int fd, const int64_t offset, const int64_t size, char *buf)
{
  assert(fd > 0 && offset >= 0 && size > 0 && (nullptr != buf));
  assert(!is_aligned(offset, size, buf));

  int ret = Status::kOk;
  int64_t aligned_offset = DIOHelper::align_offset(offset);
  int64_t aligned_size = DIOHelper::align_size(offset + size - aligned_offset);
  char *aligned_buf = nullptr;

  if (IS_NULL(aligned_buf = reinterpret_cast<char *>(memory::base_memalign(
      DIOHelper::DIO_ALIGN_SIZE, aligned_size, memory::ModId::kReadableExtent)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for aligned buffer", K(ret), K(aligned_size));
  } else if (FAILED(direct_read(fd, aligned_offset, aligned_size, aligned_buf))) {
    SE_LOG(WARN, "fail to direct read", K(ret), K(aligned_offset), K(aligned_size));
  } else {
    memcpy(buf, aligned_buf + (offset - aligned_offset), size);
  }

  if (IS_NOTNULL(aligned_buf)) {
    memory::base_memalign_free(aligned_buf);
    aligned_buf = nullptr;
  }

  return ret;
}

int ReadableExtent::direct_read(int fd, const int64_t offset, const int64_t size, char *buf)
{
  assert(fd > 0 && offset >= 0 && size > 0 && (nullptr != buf));
  assert(is_aligned(offset, size, buf));

  int ret = Status::kOk;
  int64_t total_read_size = 0;
  int64_t read_size = 0;
  char *curr_buf = buf;
  int64_t curr_offset = offset;
  int64_t left_size = size;


  while (SUCCED(ret) && left_size > 0) {
    read_size = pread(fd, curr_buf, left_size, curr_offset);
    if (read_size <= 0) {
      if (EINTR == errno) {
        continue;;
      } else {
        ret = Status::kIOError;
#ifndef NDEBUG
        SE_LOG(ERROR, "extent io error!", K(ret), K(errno));
#endif
      }
    } else {
      total_read_size += read_size;
      curr_buf = buf + total_read_size;
      curr_offset = offset + total_read_size;
      left_size = size - total_read_size;
    }
  }
  se_assert(size == total_read_size);

  return ret;
}


int FullPrefetchExtent::init(const ExtentIOInfo &io_info)
{
  int ret = Status::kOk;

  if (FAILED(IOExtent::init(io_info))) {
    SE_LOG(WARN, "fail to init basic io", K(ret));
  } else {
    aio_req_.status_ = Status::kInvalidArgument; // not prepare
    if (IS_NULL(aio_ = AIO::get_instance())) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "aio instance must not be nullptr", K(ret));
    }
  }

  return ret;
}

void FullPrefetchExtent::reset()
{
  IOExtent::reset();
  aio_ = nullptr;
  aio_req_.reset();
}

int FullPrefetchExtent::full_prefetch()
{
  int ret = Status::kOk;
  AIOInfo aio_info(io_info_.fd_, io_info_.get_offset(), io_info_.extent_size_);

  if (UNLIKELY(!is_inited_)) {
    SE_LOG(WARN, "FullPrefetchExtent should be inited", K(ret));
  } else if (FAILED(aio_req_.prepare_read(aio_info))) {
    SE_LOG(WARN, "fail to prepare iocb", K(ret), K(aio_info));
  } else if (FAILED(aio_->submit(&aio_req_, 1))) {
    SE_LOG(WARN, "fail to submit aio request", K(ret), K(aio_info));
  }

  return ret;
}

int FullPrefetchExtent::read(const int64_t offset, const int64_t size, char *buf, AIOHandle *aio_handle, Slice &result)
{
  UNUSED(aio_handle);
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "FullPrefetchExtent should be inited", K(ret));
  } else if (FAILED(reap())) {
    SE_LOG(WARN, "fail to reap aio result", K(ret), K(offset), K(size));
  } else if (UNLIKELY((offset + size) > aio_req_.aio_buf_.size())) {
    ret = Status::kOverLimit;
    SE_LOG(WARN, "extent size overflow", K(ret), K(offset), K(size), K_(aio_req));
  } else {
    result.assign(aio_req_.aio_buf_.data() + offset, size);
  }

  // async read failed, try sync read, overwrite ret here.
  if (FAILED(ret)) {
    if (FAILED(ReadableExtent::read(offset, size, buf, nullptr, result))) {
      SE_LOG(WARN, "fail to sync read after async read failed", K(ret), K(offset), K(size), K_(io_info));
    }
  }

  return ret;
}

int FullPrefetchExtent::reap()
{
  int ret = Status::kOk;

  if (Status::kBusy == aio_req_.status_) {
    // have submit, but not reap
    if (FAILED(aio_->reap(&aio_req_))) {
      aio_req_.status_ = Status::kIOError;
    } else if (FAILED(aio_req_.status_)) {
      aio_req_.status_ = Status::kIOError;
    }
  }

  return aio_req_.status_; 
}

}  // storage
}  // smartengine
