//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"
#include "db/dbformat.h"

#include "monitoring/query_perf_context.h"
#include "storage/extent_space_manager.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/table_builder.h"
#include "util/file_reader_writer.h"

namespace smartengine {
using namespace cache;
using namespace common;
using namespace memory;
using namespace monitor;
using namespace storage;
using namespace table;
using namespace util;

namespace db {
namespace {

template <class T>
static void DeleteEntry(const Slice& key, void* value) {
  T* typed_value = reinterpret_cast<T*>(value);
//  delete typed_value;
  MOD_DELETE_OBJECT(T, typed_value);
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

static Slice GetSliceForFileNumber(const uint64_t* file_number) {
  return Slice(reinterpret_cast<const char*>(file_number),
               sizeof(*file_number));
}

}  // namespace

TableCache::TableCache(const ImmutableCFOptions& ioptions, Cache* const cache)
    : ioptions_(ioptions),
      cache_(cache)
{
  se_assert(nullptr != cache_);
}

TableCache::~TableCache() {}

InternalIterator* TableCache::create_iterator(const ReadOptions& options,
                                              const InternalKeyComparator& internal_key_comparator,
                                              int32_t level,
                                              const ExtentId &extent_id,
                                              TableReader *&table_reader)
{
  int ret = Status::kOk;
  Cache::Handle* handle = nullptr;
  InternalIterator* iterator = nullptr;

  if (FAILED(find_table_reader(internal_key_comparator,
                               extent_id,
                               options.read_tier == kBlockCacheTier /*no_io=*/,
                               false /*skip_filters*/,
                               level,
                               true /*prefetch_index_and_filter_in_cache*/,
                               &handle))) {
    SE_LOG(WARN, "fail to find reader handle", K(ret), K(extent_id));
  } else if (IS_NULL(table_reader = get_table_reader_from_handle(handle))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the reader must not be nullptr", K(ret));
  } else if (IS_NULL(iterator = table_reader->NewIterator(options, nullptr /*arena*/, false /*skip_filters*/, 0 /*scan_add_blocks_limit*/))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "fail to create interator", K(ret));
  } else {
    if (IS_NOTNULL(handle)) {
      iterator->RegisterCleanup(&UnrefEntry, cache_, handle);
      handle = nullptr;
    }
  }


  // resource clean
  if (FAILED(ret)) {
    if (IS_NOTNULL(handle)) {
      release_handle(handle);
      handle = nullptr;
    }

    if (IS_NOTNULL(iterator)) {
      MOD_DELETE_OBJECT(InternalIterator, iterator);
      iterator = nullptr;
    }
  } else {
    se_assert(IS_NULL(handle) && IS_NOTNULL(iterator));
  }

  return iterator;
}

int TableCache::get(const ReadOptions& options,
                    const InternalKeyComparator& internal_key_comparator,
                    const int32_t level,
                    const ExtentId &extent_id,
                    const Slice& key,
                    GetContext* get_context)
{
  int ret = Status::kOk;
  TableReader* reader = nullptr;
  Cache::Handle* handle = nullptr;

  if (FAILED(find_table_reader(internal_key_comparator,
                               extent_id,
                               options.read_tier == common::kBlockCacheTier /*no_io=*/,
                               false /*skip_filters*/,
                               level,
                               true /*prefetch_index_and_filter_in_cache*/,
                               &handle))) {
    SE_LOG(WARN, "fail to find reader", K(ret), K(extent_id));
  } else if (IS_NULL(reader = get_table_reader_from_handle(handle))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the reader must not be nullptr", K(ret), K(extent_id));
  } else if (FAILED(reader->Get(options, key, get_context, false /*skip_filters*/).code())) {
    SE_LOG(WARN, "fail to get key", K(ret), K(extent_id));
  } else {
    STRESS_CHECK_APPEND(GET_FROM_EXTENT, Status::kOk);
  }

  if (IS_NOTNULL(handle)) {
    release_handle(handle);
    handle = nullptr;
  }

  return ret;
}

int TableCache::find_table_reader(const InternalKeyComparator &internal_key_comparator,
                                  const ExtentId &extent_id,
                                  const bool no_io,
                                  const bool skip_filters,
                                  const int32_t level,
                                  const bool prefetch_index_and_filter_in_cache,
                                  Cache::Handle **handle)
{
  int ret = Status::kOk;
  TableReader *table_reader = nullptr;
  uint64_t id = extent_id.id();
  Slice key = GetSliceForFileNumber(&id); 

  if (IS_NULL(*handle = cache_->Lookup(key))) {
    // cache miss, create it
    if (no_io) {
      ret = Status::kIncomplete;
      SE_LOG(WARN, "can't find table reader from cache", K(extent_id));
    } else if (FAILED(create_table_reader(internal_key_comparator,
                                          extent_id,
                                          skip_filters,
                                          level,
                                          prefetch_index_and_filter_in_cache,
                                          table_reader))) {
      SE_LOG(WARN, "fail to get table reader", K(ret));
    } else if (FAILED(cache_->Insert(key,
                                     table_reader,
                                     table_reader->get_usable_size(),
                                     &DeleteEntry<TableReader>,
                                     handle).code())) {
    
    } else{
      table_reader->set_mod_id(ModId::kTableCache);
    }
  }

  return ret;
}

TableReader* TableCache::get_table_reader_from_handle(Cache::Handle* handle)
{
  return reinterpret_cast<TableReader*>(cache_->Value(handle));
}

void TableCache::release_handle(Cache::Handle* handle)
{
  cache_->Release(handle);
}

void TableCache::evict(Cache* cache, uint64_t file_number)
{
  cache->Erase(GetSliceForFileNumber(&file_number));
}

int TableCache::create_table_reader(const InternalKeyComparator &internal_key_comparator,
                                    const ExtentId &extent_id,
                                    bool skip_filters,
                                    int level,
                                    bool prefetch_index_and_filter_in_cache,
                                    TableReader *&table_reader)
{
  int ret = Status::kOk;
  RandomAccessExtent *extent = nullptr;
  RandomAccessFileReader *file_reader = nullptr;
  TableReaderOptions reader_options(ioptions_, internal_key_comparator,
      extent_id, skip_filters, level);

  if (IS_NULL(extent = MOD_NEW_OBJECT(ModId::kTableCache, RandomAccessExtent))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for RandomAccessExtent", K(ret));
  } else if (FAILED(ExtentSpaceManager::get_instance().get_random_access_extent(extent_id, *extent))) {
    SE_LOG(WARN, "fail to get RandomAccessExtent", K(ret), K(extent_id));
  } else if (IS_NULL(file_reader = MOD_NEW_OBJECT(ModId::kTableCache,
                                                  RandomAccessFileReader,
                                                  extent,
                                                  false /*use_allocator*/))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for RandomAccessFileReader", K(ret));
  } else if (FAILED(ioptions_.table_factory->NewTableReader(reader_options,
                                                            file_reader,
                                                            MAX_EXTENT_SIZE,
                                                            table_reader,
                                                            prefetch_index_and_filter_in_cache).code())) {
    SE_LOG(WARN, "failk to create table reader", K(ret));
  } else {
    QUERY_COUNT(CountPoint::NUMBER_FILE_OPENS);
  }

  // resource clean
  if (FAILED(ret)) {
    if (IS_NOTNULL(extent) && IS_NULL(file_reader)) {
      MOD_DELETE_OBJECT(RandomAccessExtent, extent);
    }
    if (IS_NOTNULL(file_reader)) {
      MOD_DELETE_OBJECT(RandomAccessFileReader, file_reader);
    }
  }

  return ret;
}

}  // namespace db
}  // namespace smartengine
