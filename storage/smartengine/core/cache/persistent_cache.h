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

#include "cache/cache_entry.h"
#include "env/env.h"
#include "storage/storage_common.h"
#include "util/heap.h"
#include <mutex>
#include <string>
namespace smartengine
{
namespace common
{
  class Slice;
} // namespace common
namespace storage
{
  struct ExtentId;
} // namespace storage
namespace util
{
  struct AIOHandle;
} // namespace util

namespace cache
{
struct PersistentCacheInfo;

/**The format of the persistent cache files is similar to that of the
data files, both using fixed-length 2MB(storage::MAX_EXTENT_SIZE) as
the basic management unit.Each extent is uniquely identified by offset
0, 1, 2 ...*/
class PersistentCacheFile
{
public:
  PersistentCacheFile();
  ~PersistentCacheFile();

  int open(util::Env *env, const std::string &file_path, int64_t max_size);
  void close();
  int write(const common::Slice &extent_data, PersistentCacheInfo &cache_info);
  int read(PersistentCacheInfo &cache_info,
           util::AIOHandle *aio_handle,
           int64_t offset,
           int64_t size,
           char *buf,
           common::Slice &result);
  int recycle(PersistentCacheInfo &cache_info);  
  int get_fd() { return file_->get_fd(); }

private:
  int create_cache_file(util::Env *env, const std::string &file_path);
  int alloc_cache_space(storage::ExtentId &extent_id);
  int set_used_extent(const storage::ExtentId &extent);
  int set_free_extent(const storage::ExtentId &extent);
  inline bool exceed_capacity(const storage::ExtentId &extent_id) const { return extent_id.offset >= extent_count_; }
  inline bool is_used_extent(const storage::ExtentId &extent_id) const { return (1 == used_status_.count(extent_id.offset)); }
  inline bool is_free_extent(const storage::ExtentId &extent_id) const { return (0 == used_status_.count(extent_id.offset));}

private:
  static const int32_t PERSISTENT_FILE_NUMBER = (INT32_MAX - 1);
  static const int64_t PERSISTENT_UNIQUE_ID = (INT64_MAX - 1);
  static const char *DEFAULT_PERSISTENT_FILE_NAME;
  typedef util::BinaryHeap<int32_t, std::greater<int32_t>> FreeSpaceHeap;

private:
  bool is_opened_;
  util::Env *env_;
  std::string file_path_;
  util::RandomRWFile *file_;
  int64_t size_;
  int64_t extent_count_;
  std::mutex space_mutex_;
  std::set<int32_t> used_status_;
  FreeSpaceHeap free_space_;
};

struct PersistentCacheInfo
{
  PersistentCacheFile *cache_file_;
  storage::ExtentId extent_id_;

  PersistentCacheInfo();
  ~PersistentCacheInfo();

  void reset();
  bool is_valid() const;
  int get_fd() { return cache_file_->get_fd(); }
  int64_t get_offset() { return extent_id_.offset * storage::MAX_EXTENT_SIZE; }
  int64_t get_usable_size() { return storage::MAX_EXTENT_SIZE; }
  static void delete_entry(const common::Slice &key, void *value);

  DECLARE_TO_STRING()
};

class PersistentCache
{
public:
  static PersistentCache &get_instance();

  int init(util::Env *env, const std::string &cache_file_path, int64_t cache_size);
  void destroy();
  int insert(const storage::ExtentId &extent_id,
             const common::Slice &extent_data,
             cache::Cache::Handle **handle);
  int lookup(const storage::ExtentId &extent_id, cache::Cache::Handle *&handle);
  int evict(const storage::ExtentId &extent_id);
  int read_from_handle(cache::Cache::Handle *handle,
                       util::AIOHandle *aio_handle,
                       int64_t offset,
                       int64_t size,
                       char *buf,
                       common::Slice &result);
  void release_handle(cache::Cache::Handle *handle);
  PersistentCacheInfo *get_cache_info_from_handle(cache::Cache::Handle *handle);
  inline bool is_enabled() const { return is_inited_; } 

private:
  PersistentCache();
  ~PersistentCache();
  void generate_cache_key(const storage::ExtentId &extent_id, char *buf, common::Slice &cache_key);

private:
  static const int64_t MAX_PERSISTENT_CACHEL_KEY_SIZE = 10;

private:
  bool is_inited_;
  // TODO(Zhao Dongsheng) : don't use shared_ptr!
  std::shared_ptr<cache::Cache> cache_;
  PersistentCacheFile cache_file_;
};

} // namespace cache
} // namespace smartengine