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

#include "cache/persistent_cache.h"
#include "cache/cache_entry.h"
#include "env/io_posix.h"
#include "logger/log_module.h"
#include "storage/io_extent.h"
#include "util/aio_wrapper.h"
#include "util/se_constants.h"
#include "util/status.h"

namespace smartengine
{
using namespace cache;
using namespace common;
using namespace memory;
using namespace storage;
using namespace util;

namespace cache
{

PersistentCacheFile::PersistentCacheFile()
    : is_opened_(false),
      env_(nullptr),
      file_path_(),
      file_(nullptr),
      size_(0),
      extent_count_(0),
      space_mutex_(),
      used_status_(),
      free_space_()
{}

PersistentCacheFile::~PersistentCacheFile()
{
  close();
}

int PersistentCacheFile::open(Env *env, const std::string &file_path, int64_t size)
{
  int ret = Status::kOk;
  std::string file_name = file_path + "/" + DEFAULT_PERSISTENT_FILE_NAME;

  if (UNLIKELY(is_opened_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "the persistent cache file has been opened", K(ret), K(file_path));
  } else if (UNLIKELY(IS_NULL(env)) || UNLIKELY(file_path.empty()) ||
             UNLIKELY(0 != (size % storage::MAX_EXTENT_SIZE))) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(env), K(file_path), K(size));
  } else if (FAILED(create_cache_file(env, file_name))) {
    SE_LOG(WARN, "fail to create cache file", K(ret), K(file_name));
  } else if (FAILED(file_->fallocate(0 /*mode*/, 0, size))) {
    SE_LOG(WARN, "fail to fallocate cache file", K(ret), K(file_name), K(size));
  } else {
    env_ = env;
    size_ = size;
    extent_count_ = size_ / storage::MAX_EXTENT_SIZE;
    for (int64_t i = 0; i < extent_count_; ++i) {
      free_space_.push(i);
    }

    is_opened_ = true;
  }

  return ret;
}

void PersistentCacheFile::close()
{
  if (is_opened_) {
    std::lock_guard<std::mutex> guard(space_mutex_);
    PosixRandomRWFile *file_ptr = reinterpret_cast<PosixRandomRWFile *>(file_);
    MOD_DELETE_OBJECT(PosixRandomRWFile, file_ptr);
    file_ = nullptr;
    used_status_.clear();
    free_space_.clear();
    is_opened_ = false;
  }
}

int PersistentCacheFile::write(const Slice &extent_data, PersistentCacheInfo &cache_info)
{
  int ret = Status::kOk;
  ExtentId extent_id;
  FileIOExtent extent;
  cache_info.reset();

  if (UNLIKELY(!is_opened_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the cache file has not been opened", K(ret));
  } else if (UNLIKELY(extent_data.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(extent_data));
  } else if (FAILED(alloc_cache_space(extent_id))) {
    if (Status::kNoSpace != ret) {
      SE_LOG(WARN, "fail to alloca cache space", K(ret));
    }
  } else if (FAILED(extent.init(extent_id, PERSISTENT_UNIQUE_ID, file_->get_fd()))) {
    SE_LOG(WARN, "fail to init extent", K(ret), K(extent_id));
  } else if (FAILED(extent.write(extent_data))) {
    SE_LOG(WARN, "fail to write extent", K(ret));
  } else {
    cache_info.cache_file_ = this;
    cache_info.extent_id_ = extent_id;
    assert(!exceed_capacity(cache_info.extent_id_));

    SE_LOG(DEBUG, "success to write extent", K(extent_id));
  }

  return ret;
}

int PersistentCacheFile::read(PersistentCacheInfo &cache_info,
                              AIOHandle *aio_handle,
                              int64_t offset,
                              int64_t size,
                              char *buf,
                              Slice &result)
{
  int ret = Status::kOk;
  FileIOExtent extent;

  if (UNLIKELY(!is_opened_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the PersistentCacheFile has not been opened", K(ret));
  } else if (UNLIKELY(!cache_info.is_valid()) || UNLIKELY(offset < 0) ||
             UNLIKELY(size <= 0) || UNLIKELY(IS_NULL(buf))) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret),
        K(cache_info), K(offset), K(size), KP(buf));
  } else if (UNLIKELY(exceed_capacity(cache_info.extent_id_))) {
    ret = Status::kIOError;
    SE_LOG(ERROR, "the read range exceed cache file capacity", K(ret), K(cache_info));
  } else if (UNLIKELY(!is_used_extent(cache_info.extent_id_))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(ERROR, "the extent isn't used, may read dirty data", K(ret), K(cache_info));
  } else if (FAILED(extent.init(cache_info.extent_id_, PERSISTENT_UNIQUE_ID, file_->get_fd()))) {
    SE_LOG(WARN, "fail to init extent", K(ret), K(cache_info));
  } else if (FAILED(extent.read(aio_handle, offset, size, buf, result))) {
    SE_LOG(WARN, "fail to read from cache file extent", K(ret));
  } else {
    // succeed
  }

  return ret;
}

int PersistentCacheFile::recycle(PersistentCacheInfo &cache_info)
{
  int ret = Status::kOk;

  std::lock_guard<std::mutex> guard(space_mutex_);
  if (UNLIKELY(!is_opened_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the PersistentCacheFile has not been opened", K(ret));
  } else if (UNLIKELY(!cache_info.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(cache_info));
  } else if (UNLIKELY(exceed_capacity(cache_info.extent_id_))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(ERROR, "the read range exceed cache file capacity", K(ret), K(cache_info));
  } else if (FAILED(set_free_extent(cache_info.extent_id_))) {
    SE_LOG(ERROR, "fail to set extent free", K(ret), K(cache_info));
  } else {
    free_space_.push(cache_info.extent_id_.offset);
  }

  return ret;
}

// TODO (Zhao Dongsheng) : If the cache is exist, simply delete it now,
// and may reuse it in the future.
int PersistentCacheFile::create_cache_file(Env *env, const std::string &file_path)
{
  int ret = Status::kOk;
  util::EnvOptions env_options;
  env_options.use_direct_reads = true;
  env_options.use_direct_writes = true;

  if (UNLIKELY(IS_NULL(env)) || UNLIKELY(file_path.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(env), K(file_path));
  } else if (FAILED(env->FileExists(file_path).code())) {
    if (Status::kNotFound != ret) {
      SE_LOG(WARN, "fail to check file exist", K(ret));
    } else if (FAILED(env->NewRandomRWFile(file_path, file_, env_options).code())) {
      SE_LOG(WARN, "fail to create cache file", K(ret), K(file_path));
    } else {
      SE_LOG(INFO, "success to create cache file", K(file_path));
    }
  } else if (FAILED(env->DeleteFile(file_path).code())) {
    SE_LOG(WARN, "fail to delete previous cache file", K(ret), K(file_path));
  } else if (FAILED(env->NewRandomRWFile(file_path, file_, env_options).code())) {
    SE_LOG(WARN, "fail to create cache file", K(ret), K(file_path));
  } else {
    SE_LOG(INFO, "success to create cache file", K(file_path));
  }

  return ret;
}

int PersistentCacheFile::alloc_cache_space(ExtentId &extent_id)
{
  int ret = Status::kOk;

  std::lock_guard<std::mutex> guard(space_mutex_);
  if (UNLIKELY(free_space_.empty())) {
    assert(extent_count_ == static_cast<int64_t>(used_status_.size()));
    ret = Status::kNoSpace;
    SE_LOG(DEBUG, "the persistent cache file is full");
  } else {
    extent_id.file_number = PERSISTENT_FILE_NUMBER;
    extent_id.offset = free_space_.top();
    free_space_.pop();

    if (FAILED(set_used_extent(extent_id))) {
      SE_LOG(ERROR, "fail to set extent used", K(ret), K(extent_id));
    }
  }

  return ret;
}

int PersistentCacheFile::set_used_extent(const storage::ExtentId &extent_id)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_free_extent(extent_id))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(ERROR, "unexpected error, the space status is corrupted", K(ret), K(extent_id));
  } else if (UNLIKELY(!(used_status_.emplace(extent_id.offset).second))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(ERROR, "unexpected error, fail to set used status", K(ret), K(extent_id));
  } else {
    assert(!exceed_capacity(extent_id));
  }

  return ret;
}

int PersistentCacheFile::set_free_extent(const storage::ExtentId &extent_id)
{
  int ret = Status::kOk;
  int count = 0;

  if (UNLIKELY(!is_used_extent(extent_id))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(ERROR, "unexpected error, the recycling extent isn't used", K(ret), K(extent_id));
  } else if (UNLIKELY(1 != (count = used_status_.erase(extent_id.offset)))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(ERROR, "unexpected error, fail to set extent free", K(ret), K(extent_id), K(count));
  } else {
    // succeed
  }

  return ret;
}

const char *PersistentCacheFile::DEFAULT_PERSISTENT_FILE_NAME = "SMARTENGINE_PERSISTENT_CACHE";

PersistentCacheInfo::PersistentCacheInfo() : cache_file_(nullptr), extent_id_() {}

PersistentCacheInfo::~PersistentCacheInfo() {}

void PersistentCacheInfo::reset()
{
  cache_file_ = nullptr;
  extent_id_.reset();
}

bool PersistentCacheInfo::is_valid() const
{
  return IS_NOTNULL(cache_file_) && (extent_id_.offset >= 0);
}

void PersistentCacheInfo::delete_entry(const Slice &key, void *value)
{
  assert(IS_NOTNULL(value));
  PersistentCacheInfo *cache_info = reinterpret_cast<PersistentCacheInfo *>(value);
  cache_info->cache_file_->recycle(*cache_info);
}

DEFINE_TO_STRING(PersistentCacheInfo, KVP_(cache_file), KV_(extent_id))

PersistentCache::PersistentCache()
    : is_inited_(false),
      mode_(kReadWriteThrough),
      cache_(nullptr),
      cache_file_()
{}

PersistentCache::~PersistentCache()
{
  destroy();
}

PersistentCache &PersistentCache::get_instance()
{
  static PersistentCache persistent_cache;
  return persistent_cache;
}

int PersistentCache::init(Env *env,
                          const std::string &cache_file_path,
                          int64_t cache_size,
                          PersistentCacheMode mode)
{
  int ret = Status::kOk;
  int64_t actual_cache_size = ((cache_size + storage::MAX_EXTENT_SIZE -1) / storage::MAX_EXTENT_SIZE) * storage::MAX_EXTENT_SIZE;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "the PersistentCache has been inited", K(ret));
  } else if (UNLIKELY(IS_NULL(env)) ||
             UNLIKELY(cache_file_path.empty()) ||
             UNLIKELY(cache_size < 0) ||
             UNLIKELY(mode >= kMaxPersistentCacheMode)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(env), K(cache_file_path), K(cache_size), KE(mode));
  } else {
    if (0 == actual_cache_size) {
      SE_LOG(INFO, "the PersistentCache is disabled");
    } else if (IS_NULL(cache_ = NewLRUCache(actual_cache_size,
                                            7 /*num_shard_bits*/,
                                            false /*strict_capacity_limit*/,
                                            0.0 /*high_pri_pool_ratio*/,
                                            0.0 /*old_pool_ratio*/,
                                            memory::ModId::kPersistentCache))) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "fail to init lru cache instance for persistent cache", K(ret), K(actual_cache_size));
    } else if (FAILED(cache_file_.open(env, cache_file_path, actual_cache_size))) { 
      SE_LOG(WARN, "fail to open persistent cache file", K(ret), K(cache_file_path), K(actual_cache_size));
    } else {
      mode_ = mode;
      is_inited_ = true;
      SE_LOG(INFO, "success to enable PersistentCache", K(cache_file_path), K(cache_size), KE(mode));
    }
  }

  // Resource clean
  if (FAILED(ret)) {
    cache_.reset();
    cache_file_.close();
  }

  return ret;
}

void PersistentCache::destroy()
{
  if (is_inited_) {
    cache_->DisownData();
    cache_->destroy();
    cache_.reset();
    cache_file_.close();
    is_inited_ = false;
  }
}

int PersistentCache::insert(const ExtentId &extent_id,
                            const Slice &extent_data,
                            Cache::Handle **handle)
{
  int ret = Status::kOk;
  PersistentCacheInfo *cache_info = nullptr;
  char cache_key_buf[MAX_PERSISTENT_CACHEL_KEY_SIZE] = {0};
  Slice cache_key;
  generate_cache_key(extent_id, cache_key_buf, cache_key);

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the PersistentCache has not been inited", K(ret));
  } else if (UNLIKELY(extent_data.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(extent_data));
  } else if (IS_NULL(cache_info = MOD_NEW_OBJECT(ModId::kPersistentCache, PersistentCacheInfo))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for PersistentCacheInfo", K(ret));
  } else if (FAILED(cache_file_.write(extent_data, *cache_info))) {
    if (Status::kNoSpace != ret) {
      SE_LOG(WARN, "fail to write data to cache file", K(ret));
    }
  } else if (FAILED(cache_->Insert(cache_key,
                                   cache_info,
                                   cache_info->get_usable_size(),
                                   &PersistentCacheInfo::delete_entry,
                                   handle,
                                   Cache::Priority::HIGH).code())) {
    SE_LOG(WARN, "fail to insert into persistent cache", K(ret), K(extent_id));
  } else {
    SE_LOG(DEBUG, "success to insert into persistent cache", K(ret), K(extent_id));
  }

  // Resource Clean
  if (FAILED(ret)) {
    int tmp_ret = Status::kOk;
    if (IS_NOTNULL(cache_info)) {
      if (cache_info->is_valid()) {
        // The space for this extent in persistent cache has been successfully allocated
        // and needs to be recycled.
        if (Status::kOk != (tmp_ret = cache_file_.recycle(*cache_info))) {
          SE_LOG(WARN, "fail to recycle cache space", K(tmp_ret), K(*cache_info));
        }
      }
      MOD_DELETE_OBJECT(PersistentCacheInfo, cache_info);
    }
  }

  return ret;
}

int PersistentCache::lookup(const storage::ExtentId &extent_id, Cache::Handle *&handle)
{
  int ret = Status::kOk;
  char cache_key_buf[MAX_PERSISTENT_CACHEL_KEY_SIZE] = {0};
  Slice cache_key;
  generate_cache_key(extent_id, cache_key_buf, cache_key);

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "PersistentCache has not been inited", K(ret));
  } else if (IS_NULL(handle = cache_->Lookup(cache_key))) {
    ret = Status::kNotFound;
  }

  return ret;
}

int PersistentCache::evict(const storage::ExtentId &extent_id)
{
  int ret = Status::kOk;
  char cache_key_buf[MAX_PERSISTENT_CACHEL_KEY_SIZE] = {0};
  Slice cache_key;
  generate_cache_key(extent_id, cache_key_buf, cache_key);

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "PersistentCache has not been inited", K(ret));
  } else {
    cache_->Erase(cache_key);
  }

  return ret;
}
int PersistentCache::read_from_handle(Cache::Handle *handle,
                                      AIOHandle *aio_handle,
                                      int64_t offset,
                                      int64_t size,
                                      char *buf,
                                      common::Slice &result)
{
  int ret = Status::kOk;
  PersistentCacheInfo *cache_info = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the PersistentCache has not been inited", K(ret));
  } else if (UNLIKELY(IS_NULL(handle)) || UNLIKELY(offset < 0) ||
             UNLIKELY(size <= 0) || UNLIKELY(IS_NULL(buf))) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret),
        KP(handle), K(offset), K(size), KP(buf));
  } else if (IS_NULL(cache_info = get_cache_info_from_handle(handle))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the cache info must not be nullptr", K(ret), KP(handle));
  } else if (UNLIKELY(!cache_info->is_valid())) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the cache info is invalid", K(ret), K(*cache_info));
  } else if (FAILED(cache_info->cache_file_->read(*cache_info, aio_handle, offset, size, buf, result))) {
    SE_LOG(WARN, "fail to read from cache file", K(ret));
  } else {
    // succeed
  }

  return ret;
}

void PersistentCache::release_handle(Cache::Handle *handle)
{
  assert(is_inited_);
  cache_->Release(handle);
}

PersistentCacheInfo *PersistentCache::get_cache_info_from_handle(Cache::Handle *handle)
{
  return reinterpret_cast<PersistentCacheInfo *>(cache_->Value(handle));
}

void PersistentCache::generate_cache_key(const ExtentId &extent_id, char *buf, Slice &cache_key)
{
  char *end = util::EncodeVarint64(buf, extent_id.id());
  cache_key.assign(buf, end - buf);
}

} // namespace cache
} // namespace smartengine