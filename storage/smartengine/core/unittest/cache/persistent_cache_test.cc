/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cache/persistent_cache.h"
#include "cache/lru_cache.h"
#include "unittest/db/db_test_util.h"
#include "util/testharness.h"
#include "port/stack_trace.h"

static const std::string test_dir = "/hotbackup_test";
namespace smartengine
{
using namespace common;
using namespace db;
using namespace memory;
using namespace util;

namespace cache
{
class PersistentCacheTest : public testing::Test
{
public:
  PersistentCacheTest()
  {
    file_path_ = test::TmpDir(Env::Default()) + test_dir;
  }

public:
  std::string file_path_;
};
 
TEST_F(PersistentCacheTest, persistent_cache_file_open)
{
  int ret = Status::kOk;
  PersistentCacheFile cache_file;

  // invalid argument
  ret = cache_file.open(nullptr, file_path_, 0);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = cache_file.open(Env::Default(), std::string(), 0);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = cache_file.open(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE + 1);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // success open
  ret = cache_file.open(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE);
  ASSERT_EQ(Status::kOk, ret);

  // repeat open
  ret = cache_file.open(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE);
  ASSERT_EQ(Status::kInitTwice, ret);

  // close and reopen
  cache_file.close();
  ret = cache_file.open(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE);
}

TEST_F(PersistentCacheTest, persistent_cache_file_write)
{
  int ret = Status::kOk;
  PersistentCacheFile cache_file;
  PersistentCacheInfo cache_info;
  std::string data;

  // not init
  ret = cache_file.write(Slice(), cache_info);
  ASSERT_EQ(Status::kNotInit, ret);

  // open cache file
  ret = cache_file.open(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE * 2);
  ASSERT_EQ(Status::kOk, ret);

  // invalid argument
  ret = cache_file.write(Slice(), cache_info);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // success to write data
  data = test::RandomStringGenerator::generate(storage::MAX_EXTENT_SIZE);
  for (int64_t i = 0; i < 2; ++i) {
    ret = cache_file.write(Slice(data.data(), storage::MAX_EXTENT_SIZE), cache_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_TRUE(cache_info.is_valid());
    ASSERT_EQ(i, cache_info.extent_id_.offset);
  }

  // no space
  ret = cache_file.write(Slice(data.data(), storage::MAX_EXTENT_SIZE), cache_info);
  ASSERT_EQ(Status::kNoSpace, ret);
}

TEST_F(PersistentCacheTest, persistent_cache_file_read)
{
  int ret = Status::kOk;
  PersistentCacheFile cache_file;
  PersistentCacheInfo cache_info;
  std::string data;
  Slice result;
  char *buf = reinterpret_cast<char *>(base_malloc(storage::MAX_EXTENT_SIZE)); 
  ASSERT_TRUE(nullptr != buf);

  // not open
  ret = cache_file.read(cache_info, nullptr, 0, 0, nullptr, result);
  ASSERT_EQ(Status::kNotInit, ret);

  // open cache file
  ret = cache_file.open(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE * 2); 
  ASSERT_EQ(Status::kOk, ret);

  // invalid argument
  ret = cache_file.read(cache_info, nullptr, 0, 10, buf, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  // mock valid cache info
  cache_info.cache_file_ = reinterpret_cast<PersistentCacheFile *>(1);
  cache_info.extent_id_.offset = 0;
  ret = cache_file.read(cache_info, nullptr, -1, 10, buf, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = cache_file.read(cache_info, nullptr, 0, 0, buf, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = cache_file.read(cache_info, nullptr, 0, 10, nullptr, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = cache_file.read(cache_info, nullptr, 0, 10, buf, result);

  // exceed capacity
  cache_info.extent_id_.offset = 10;
  ret = cache_file.read(cache_info, nullptr, 0, 10, buf, result);
  ASSERT_EQ(Status::kIOError, ret);

  // read unused extent
  cache_info.extent_id_.offset = 0;
  ret = cache_file.read(cache_info, nullptr, 0, 10, buf, result);
  ASSERT_EQ(Status::kErrorUnexpected, ret);

  // write data
  data = test::RandomStringGenerator::generate(storage::MAX_EXTENT_SIZE);
  ret = cache_file.write(Slice(data.data(), data.size()), cache_info);
  ASSERT_EQ(Status::kOk, ret);

  // read range[10, 110]
  ret = cache_file.read(cache_info, nullptr, 10, 100, buf, result);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_TRUE(0 == memcmp(data.data() + 10, result.data(), 100));
}

TEST_F(PersistentCacheTest, persistent_cache_file_recycle)
{
  int ret = Status::kOk;
  PersistentCacheFile cache_file;
  PersistentCacheInfo cache_info;
  std::string data;

  // not init
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kNotInit, ret);

  // open cache file
  ret = cache_file.open(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE * 3);
  ASSERT_EQ(Status::kOk, ret);

  // invalid argument
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // write data
  data = test::RandomStringGenerator::generate(storage::MAX_EXTENT_SIZE);
  for (int64_t i = 0; i < 3; ++i) {
    ret = cache_file.write(Slice(data.data(), storage::MAX_EXTENT_SIZE), cache_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_TRUE(cache_info.is_valid());
    ASSERT_EQ(i, cache_info.extent_id_.offset);
  }

  // no space
  ret = cache_file.write(Slice(data.data(), storage::MAX_EXTENT_SIZE), cache_info);
  ASSERT_EQ(Status::kNoSpace, ret);

  // recycle extents
  cache_info.cache_file_ = &cache_file;
  cache_info.extent_id_.file_number = INT32_MAX - 1;
  cache_info.extent_id_.offset = 0;
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kOk, ret);
  cache_info.extent_id_.offset = 1;
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kOk, ret);
  // repeat recycle same extent
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kErrorUnexpected, ret);
  // recycle not exist extent
  cache_info.extent_id_.offset = 10;
  ret = cache_file.recycle(cache_info);
  ASSERT_EQ(Status::kErrorUnexpected, ret);

  // continue write
  for (int64_t i = 0; i < 2; ++i) {
    ret = cache_file.write(Slice(data.data(), storage::MAX_EXTENT_SIZE), cache_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_TRUE(cache_info.is_valid());
    ASSERT_EQ(i, cache_info.extent_id_.offset);
  } 

  // exhausted again
  ret = cache_file.write(Slice(data.data(), storage::MAX_EXTENT_SIZE), cache_info);
  ASSERT_EQ(Status::kNoSpace, ret); 
}

TEST_F(PersistentCacheTest, persistent_cache_init)
{
  int ret = Status::kOk;
  
  // invalid argument
  ret = PersistentCache::get_instance().init(nullptr, file_path_, 0);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = PersistentCache::get_instance().init(Env::Default(), std::string(), 0);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = PersistentCache::get_instance().init(Env::Default(), file_path_, -1);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // disable persistent cache
  ret = PersistentCache::get_instance().init(Env::Default(), file_path_, 0);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_FALSE(PersistentCache::get_instance().is_enabled());

  // enable persistent cache
  ret = PersistentCache::get_instance().init(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE * 3);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_TRUE(PersistentCache::get_instance().is_enabled());

  // rollback status
  PersistentCache::get_instance().destroy();
}

TEST_F(PersistentCacheTest, persistent_cache_insert)
{
  int ret = Status::kOk;
  cache::Cache::Handle *handle = nullptr;
  std::vector<cache::Cache::Handle *> handles;
  std::string data;
  std::vector<std::string> datas;
  storage::ExtentId extent_id(0, 1);

  // not init
  ret = PersistentCache::get_instance().insert(extent_id, Slice(data.data(), data.size()), nullptr);
  ASSERT_EQ(Status::kNotInit, ret);

  // init persistent cache
  ret = PersistentCache::get_instance().init(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE * 3);
  ASSERT_EQ(Status::kOk, ret);

  // full fill the persistent cache and acquire the handle
  for (int64_t i = 0; i < 3; ++i) {
    handle = nullptr;
    data = test::RandomStringGenerator::generate(storage::MAX_EXTENT_SIZE);
    extent_id.offset = i;
    ret = PersistentCache::get_instance().insert(extent_id, Slice(data.data(), data.size()), &handle);
    ASSERT_EQ(Status::kOk, ret);
    handles.push_back(handle);
    datas.push_back(data); 
  }

  // no space
  extent_id.offset = 3;
  ret = PersistentCache::get_instance().insert(extent_id, Slice(data.data(), data.size()), &handle);
  ASSERT_EQ(Status::kNoSpace,ret);

  // release one handle and re-insert
  PersistentCache::get_instance().release_handle(handles[0]);
  ret = PersistentCache::get_instance().insert(extent_id, Slice(data.data(), data.size()), nullptr);
  ASSERT_EQ(Status::kOk, ret);

  // release left handles
  PersistentCache::get_instance().release_handle(handles[1]);
  PersistentCache::get_instance().release_handle(handles[2]);

  // rollback status
  PersistentCache::get_instance().destroy();
}

TEST_F(PersistentCacheTest, persistent_cache_read_from_handle)
{
  int ret = Status::kOk;
  cache::Cache::Handle *handle = nullptr;
  std::vector<cache::Cache::Handle *> handles;
  std::string data;
  std::vector<std::string> datas;
  storage::ExtentId extent_id(0, 1);
  Slice result;
  char *buf = reinterpret_cast<char *>(base_malloc(storage::MAX_EXTENT_SIZE));

  // not init
  ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, 0, 0, nullptr, result);
  ASSERT_EQ(Status::kNotInit, ret);

  // init persistent cache
  ret = PersistentCache::get_instance().init(Env::Default(), file_path_, storage::MAX_EXTENT_SIZE * 3);
  ASSERT_EQ(Status::kOk, ret);

  // invalid argument
  ret = PersistentCache::get_instance().read_from_handle(nullptr, nullptr, 0, 10, buf, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  // mock valid handle
  handle = reinterpret_cast<cache::Cache::Handle *>(1);
  ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, -1, 0, buf, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, 0, 0, buf, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, 0, -1, buf, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, 0, 10, nullptr, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // null cache info
  cache::LRUHandle lru_handle;
  handle = reinterpret_cast<cache::Cache::Handle *>(&lru_handle);
  ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, 0, 10, buf, result);
  ASSERT_EQ(Status::kErrorUnexpected, ret);

  // invalid cache info
  PersistentCacheInfo cache_info;
  lru_handle.value = &cache_info;
  handle = reinterpret_cast<cache::Cache::Handle *>(&lru_handle);
  ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, 0, 10, buf, result);
  ASSERT_EQ(Status::kErrorUnexpected, ret);

  // prepare data
  for (int64_t i = 0; i < 3; ++i) {
    handle = nullptr;
    data = test::RandomStringGenerator::generate(storage::MAX_EXTENT_SIZE);
    extent_id.offset = i;
    ret = PersistentCache::get_instance().insert(extent_id, Slice(data.data(), data.size()), &handle);
    ASSERT_EQ(Status::kOk, ret);
    handles.push_back(handle);
    datas.push_back(data); 
  } 

  // check data
  int64_t offset = 0;
  int64_t size = 0;
  for (int64_t i = 0; i < 3; ++i) {
    handle = handles[i];
    data = datas[i];
    offset = i * 10;
    size = i + 100;
    ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, offset, size, buf, result);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(0, memcmp(data.data() + offset, result.data(), size));
    PersistentCache::get_instance().release_handle(handle);
  }

  // rollback status
  PersistentCache::get_instance().destroy();
}

TEST_F(PersistentCacheTest, persistent_cache_lookup_and_evict)
{
  int ret = Status::kOk;
  cache::Cache::Handle *handle = nullptr;
  std::vector<cache::Cache::Handle *> handles;
  std::string data;
  std::vector<std::string> datas;
  storage::ExtentId extent_id(0, 1);
  Slice result;
  char *buf = reinterpret_cast<char *>(base_malloc(storage::MAX_EXTENT_SIZE)); 

  // not init
  ret = PersistentCache::get_instance().lookup(extent_id, handle);
  ASSERT_EQ(Status::kNotInit, ret);
  ret = PersistentCache::get_instance().evict(extent_id);
  ASSERT_EQ(Status::kNotInit, ret);

  // init persistent cache
  // make cache size enough to contain three extent
  int64_t cache_size = (1 << 8) * storage::MAX_EXTENT_SIZE;
  ret = PersistentCache::get_instance().init(Env::Default(), file_path_, cache_size);
  ASSERT_EQ(Status::kOk, ret);

  // prepare data
  for (int64_t i = 0; i < 3; ++i) {
    data = test::RandomStringGenerator::generate(storage::MAX_EXTENT_SIZE);
    extent_id.offset = i;
    ret = PersistentCache::get_instance().insert(extent_id, Slice(data.data(), data.size()), nullptr);
    ASSERT_EQ(Status::kOk, ret);
    datas.push_back(data); 
  } 

  // lookup and check data
  for (int64_t i = 0; i < 3; ++i) {
    handle = nullptr;
    data = datas[i];
    extent_id.offset = i;
    ret = PersistentCache::get_instance().lookup(extent_id, handle);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_TRUE(nullptr != handle);
    ret = PersistentCache::get_instance().read_from_handle(handle, nullptr, 10, 200, buf, result);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(0, memcmp(data.data() + 10, result.data(), 200));
    PersistentCache::get_instance().release_handle(handle);
  }

  // cache miss
  extent_id.offset = 4;
  ret = PersistentCache::get_instance().lookup(extent_id, handle);
  ASSERT_EQ(Status::kNotFound, ret);

  // evict extent [0, 1]
  extent_id.offset = 1;
  ret = PersistentCache::get_instance().evict(extent_id);
  ASSERT_EQ(Status::kOk, ret);
  ret = PersistentCache::get_instance().lookup(extent_id, handle);
  ASSERT_EQ(Status::kNotFound, ret);
}

} // namespace cache
} // namespace smartengine

int main(int argc, char **argv) 
{
  std::string info_log_path = smartengine::util::test::TmpDir() + "/persistent_cache_test.log";
  std::string abs_test_dir = smartengine::util::test::TmpDir() + test_dir;
  smartengine::util::test::remove_dir(abs_test_dir.c_str());
  smartengine::util::Env::Default()->CreateDir(abs_test_dir);
	smartengine::util::test::init_logger(info_log_path.c_str(), smartengine::logger::INFO_LEVEL);
  smartengine::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}