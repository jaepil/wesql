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
#include "table/extent_table_reader.h"

#include <algorithm>
#include <limits>
#include <string>
#include <utility>

#include "cache/sharded_cache.h"
#include "db/dbformat.h"
#include "monitoring/query_perf_context.h"
#include "options/options.h"
#include "table/block.h"
#include "table/extent_table_factory.h"
#include "table/filter_block.h"
#include "table/filter_policy.h"
#include "table/filter_manager.h"
#include "table/format.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/two_level_iterator.h"
#include "table/sstable_scan_struct.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/string_util.h"
#include "util/sync_point.h"


namespace smartengine {
using namespace common;
using namespace util;
using namespace cache;
using namespace monitor;
using namespace db;
using namespace memory;

namespace table {

using std::unique_ptr;

extern const uint64_t kExtentBasedTableMagicNumber;

typedef ExtentBasedTable::IndexReader IndexReader;

ExtentBasedTable::~ExtentBasedTable() {
  Close();
//  delete rep_;
  if (nullptr != rep_) {
    if (rep_->internal_alloc_) {
      MOD_DELETE_OBJECT(Rep, rep_);
    } else {
      auto alloc = rep_->alloc_.get();
      assert(alloc);
      FREE_OBJECT(Rep, *alloc, rep_);
    }
  }
}

namespace {
// Read the block identified by "handle" from "file".
// The only relevant option is options.verify_checksums for now.
// On failure return non-OK.
// On success fill *result and return OK - caller owns *result
// @param compression_dict Data for presetting the compression library's
//    dictionary.
Status ReadBlockFromFile(RandomAccessFileReader* file,
                         const Footer& footer,
                         const ReadOptions& options,
                         const BlockHandle& handle,
                         Block *&result,
                         const ImmutableCFOptions& ioptions,
                         bool do_uncompress,
                         const Slice& compression_dict,
                         SequenceNumber global_seqno,
                         size_t read_amp_bytes_per_bit,
                         AIOHandle *aio_handle,
                         SimpleAllocator *alloc) {
  BlockContents contents;
  Status s = ReadBlockContents(file,
                               footer,
                               options,
                               handle,
                               &contents,
                               ioptions,
                               do_uncompress,
                               compression_dict,
                               aio_handle);
  if (s.ok()) {
    result = COMMON_NEW(ModId::kDefaultMod, Block, alloc, std::move(contents),
        global_seqno, read_amp_bytes_per_bit);
  }
  QUERY_COUNT(CountPoint::ENGINE_DISK_READ);
  QUERY_TRACE_END();
  return s;
}


// Delete the resource that is held by the iterator.
template <class ResourceType>
void DeleteHeldResource(void* arg, void* ignored) {
//  delete reinterpret_cast<ResourceType*>(arg);
  auto del_arg = reinterpret_cast<ResourceType*>(arg);
  MOD_DELETE_OBJECT(ResourceType, del_arg);
}

// Delete the entry resided in the cache.
template <class Entry>
void DeleteCachedEntry(const Slice& key, void* value) {
  auto entry = reinterpret_cast<Entry*>(value);
//  delete entry;
  MOD_DELETE_OBJECT(Entry, entry);
}

void DeleteCachedIndexEntry(const Slice& key, void* value);

// Release the cached entry and decrement its ref count.
void ReleaseCachedEntry(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

Slice GetCacheKeyFromOffset(const char* cache_key_prefix,
                            size_t cache_key_prefix_size, uint64_t offset,
                            char* cache_key) {
  assert(cache_key != nullptr);
  assert(cache_key_prefix_size != 0);
  assert(cache_key_prefix_size <= ExtentBasedTable::kMaxCacheKeyPrefixSize);
  memcpy(cache_key, cache_key_prefix, cache_key_prefix_size);
  char* end = EncodeVarint64(cache_key + cache_key_prefix_size, offset);
  return Slice(cache_key, static_cast<size_t>(end - cache_key));
}

Cache::Handle* GetEntryFromCache(Cache* block_cache, const Slice& key,
                                 CountPoint block_cache_miss_ticker,
                                 CountPoint block_cache_hit_ticker) {
  auto cache_handle = block_cache->Lookup(key);
  uint32_t shard_id = static_cast<ShardedCache *>(block_cache)->get_shard_id(key);

  if (cache_handle != nullptr) {
    // overall cache hit
    QUERY_COUNT(CountPoint::BLOCK_CACHE_HIT);
    // total bytes read from cache
    QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_BYTES_READ,
                    block_cache->GetUsage(cache_handle));
    // block-type specific cache hit
    QUERY_COUNT(block_cache_hit_ticker);

    QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_HIT, shard_id);
    QUERY_COUNT_SHARD(block_cache_hit_ticker, shard_id);
  } else {
    // overall cache miss
    QUERY_COUNT(CountPoint::BLOCK_CACHE_MISS);
    // block-type specific cache miss
    QUERY_COUNT(block_cache_miss_ticker);

    QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_MISS, shard_id);
    QUERY_COUNT_SHARD(block_cache_miss_ticker, shard_id);
  }

  return cache_handle;
}

// Index that allows binary search lookup for the first key of each block.
// This class can be viewed as a thin wrapper for `Block` class which already
// supports binary search.
class BinarySearchIndexReader : public ExtentBasedTable::IndexReader {
 public:
  // Read index from the file and create an intance for
  // `BinarySearchIndexReader`.
  // On success, index_reader will be populated; otherwise it will remain
  // unmodified.
  static Status Create(RandomAccessFileReader* file,
                       const Footer& footer,
                       const BlockHandle& index_handle,
                       const ImmutableCFOptions& ioptions,
                       const Comparator* comparator,
                       IndexReader** index_reader,
                       AIOHandle *aio_handle,
                       SimpleAllocator *alloc)
  {
    std::unique_ptr<Block, ptr_destruct_delete<Block>> index_block_ptr;
    Block *index_block = nullptr;
    auto s = ReadBlockFromFile(file,
                               footer,
                               ReadOptions(),
                               index_handle,
                               index_block,
                               ioptions,
                               true /* decompress */,
                               Slice() /*compression dict*/,
                               kDisableGlobalSequenceNumber,
                               0 /* read_amp_bytes_per_bit */,
                               aio_handle,
                               nullptr /* allocator */);

    if (s.ok()) {
      index_block_ptr.reset(index_block);
      *index_reader = COMMON_NEW(ModId::kDefaultMod, BinarySearchIndexReader, alloc,
          comparator, std::move(index_block_ptr), ioptions.statistics);
    }

    return s;
  }

  virtual InternalIterator* NewIterator(BlockIter* iter = nullptr,
                                        bool dont_care = true) override {
    return index_block_->NewIterator(comparator_, iter, true, nullptr, true);
  }

  virtual size_t size() const override { return index_block_->size(); }
  virtual size_t usable_size() const override {
    return index_block_->usable_size();
  }

  virtual size_t ApproximateMemoryUsage() const override {
    assert(index_block_);
    return index_block_->ApproximateMemoryUsage();
  }

  virtual void set_mod_id(const size_t mod_id) const override {
    assert(index_block_);
    update_mod_id(index_block_->data(), mod_id);
  }

 private:
  BinarySearchIndexReader(const Comparator* comparator,
                          std::unique_ptr<Block, ptr_destruct_delete<Block>>&& index_block,
                          Statistics* stats)
      : IndexReader(comparator, stats), index_block_(std::move(index_block)) {
    assert(index_block_ != nullptr);
  }
  std::unique_ptr<Block, ptr_destruct_delete<Block>> index_block_;
};


}  // namespace

// Helper function to setup the cache key's prefix for the Table.
void ExtentBasedTable::SetupCacheKeyPrefix(Rep* rep, uint64_t file_size) {
  assert(kMaxCacheKeyPrefixSize >= 10);
  rep->cache_key_prefix_size = 0;
  rep->compressed_cache_key_prefix_size = 0;
  if (rep->table_options.block_cache != nullptr) {
    GenerateCachePrefix(rep->table_options.block_cache.get(), rep->file->file(),
                        &rep->cache_key_prefix[0], &rep->cache_key_prefix_size);
    // Create dummy offset of index reader which is beyond the file size.
    rep->dummy_index_reader_offset =
        file_size + rep->table_options.block_cache->NewId();
  }
  if (rep->table_options.block_cache_compressed != nullptr) {
    GenerateCachePrefix(rep->table_options.block_cache_compressed.get(),
                        rep->file->file(), &rep->compressed_cache_key_prefix[0],
                        &rep->compressed_cache_key_prefix_size);
  }
}

void ExtentBasedTable::GenerateCachePrefix(Cache* cc, RandomAccessFile* file,
                                           char* buffer, size_t* size) {
  // generate an id from the file
  *size = file->GetUniqueId(buffer, kMaxCacheKeyPrefixSize);

  // If the prefix wasn't generated or was too long,
  // create one from the cache.
  if (cc && *size == 0) {
    char* end = EncodeVarint64(buffer, cc->NewId());
    *size = static_cast<size_t>(end - buffer);
  }
}

void ExtentBasedTable::GenerateCachePrefix(Cache* cc, WritableFile* file,
                                           char* buffer, size_t* size) {
  // generate an id from the file
  *size = file->GetUniqueId(buffer, kMaxCacheKeyPrefixSize);

  // If the prefix wasn't generated or was too long,
  // create one from the cache.
  if (*size == 0) {
    char* end = EncodeVarint64(buffer, cc->NewId());
    *size = static_cast<size_t>(end - buffer);
  }
}


Slice ExtentBasedTable::GetCacheKey(const char* cache_key_prefix,
                                    size_t cache_key_prefix_size,
                                    const BlockHandle& handle,
                                    char* cache_key) {
  assert(cache_key != nullptr);
  assert(cache_key_prefix_size != 0);
  assert(cache_key_prefix_size <= kMaxCacheKeyPrefixSize);
  memcpy(cache_key, cache_key_prefix, cache_key_prefix_size);
  char* end =
      EncodeVarint64(cache_key + cache_key_prefix_size, handle.offset());
  return Slice(cache_key, static_cast<size_t>(end - cache_key));
}

ExtentBasedTable::Rep::Rep(const common::ImmutableCFOptions& _ioptions,
    const util::EnvOptions& _env_options,
    const BlockBasedTableOptions& _table_opt,
    const db::InternalKeyComparator& _internal_comparator,
    bool skip_filters,
    SimpleAllocator *alloc)
    : ioptions(_ioptions),
      env_options(_env_options),
      table_options(_table_opt),
      filter_policy(skip_filters ? nullptr : _table_opt.filter_policy.get()),
      internal_comparator(_internal_comparator),
      filter_type(FilterType::kNoFilter),
      whole_key_filtering(_table_opt.whole_key_filtering),
      global_seqno(db::kDisableGlobalSequenceNumber),
      internal_alloc_(false) {
  if (nullptr == alloc) {
    internal_alloc_ = true;
    alloc_.reset(MOD_NEW_OBJECT(ModId::kRep, ArenaAllocator, 512, ModId::kRep));
  } else {
    alloc_.reset(alloc);
  }
}

ExtentBasedTable::Rep::~Rep() {
  if (internal_alloc_) {
    RandomAccessFileReader *dfile = file.release();
    MOD_DELETE_OBJECT(RandomAccessFileReader, dfile);
  } else {
    alloc_.release();
    // rep use external allocator, file is also allocated by it
    // no need free memory
  }
}

// ExtentBasedTable::Rep::reset_data_block() {
//  if (internal_alloc_) {
//    data_block.reset();
//  } else {
//    Block *block = data_block.release();
//    MOD_DELETE_OBJECT(Block, block);
//  }
//}
//
uint64_t ExtentBasedTable::Rep::get_usable_size() {
  return sizeof(*this) + sizeof(*alloc_) + alloc_->size();
}

Status ExtentBasedTable::Open(const ImmutableCFOptions& ioptions,
                              const EnvOptions& env_options,
                              const BlockBasedTableOptions& table_options,
                              const InternalKeyComparator& internal_comparator,
                              RandomAccessFileReader *file,
                              uint64_t file_size,
                              TableReader *&table_reader,
                              const FileDescriptor* fd,
                              HistogramImpl* file_read_hist,
                              const bool prefetch_index_and_filter_in_cache,
                              const bool skip_filters,
                              const int level,
                              SimpleAllocator *alloc) {
//  table_reader->reset();

  Footer footer;

  // Before read footer, readahead backwards to prefetch data
  Status s = file->Prefetch((file_size < 512 * 1024 ? 0 : file_size - 512 * 1024), 512 * 1024 /* 512 KB prefetching */);
  s = ReadFooterFromFile(file, file_size, &footer, kExtentBasedTableMagicNumber);
  if (!s.ok()) {
    return s;
  }
  if (!BlockBasedTableSupportedVersion(footer.version())) {
    return Status::Corruption(
        "Unknown Footer version. Maybe this file was created with newer "
        "version of smartengine?");
  }

  // We've successfully read the footer. We are ready to serve requests.
  // Better not mutate rep_ after the creation. eg. internal_prefix_transform
  // raw pointer will be used to create HashIndexReader, whose reset may
  // access a dangling pointer.
  //Rep* rep = new ExtentBasedTable::Rep(ioptions, env_options, table_options,
  //                                     internal_comparator, skip_filters);
  Rep* rep = COMMON_NEW(ModId::kRep, Rep, alloc, ioptions, env_options,
    table_options, internal_comparator, skip_filters, alloc);
//  rep->file = std::move(file);
  rep->file.reset(file);
  rep->level = level;
  // Copy once for this maybe used after open.
  rep->fd_valid = fd != nullptr;
  rep->fd = (fd == nullptr ? FileDescriptor() : *fd);
  rep->file_read_hist = file_read_hist;
  rep->footer = footer;
  SetupCacheKeyPrefix(rep, file_size);
  ExtentBasedTable *new_table = COMMON_NEW(ModId::kRep, ExtentBasedTable, alloc, rep);

  // Read meta index
  std::unique_ptr<Block, ptr_destruct<Block>> meta_ptr;
  std::unique_ptr<InternalIterator, ptr_destruct<InternalIterator>> meta_iter_ptr;
  Block *meta = nullptr;
  InternalIterator *meta_iter = nullptr;
  s = ReadMetaBlock(rep, meta, meta_iter, rep->alloc_.get());
  meta_ptr.reset(meta);
  meta_iter_ptr.reset(meta_iter);
  if (!s.ok()) {
    return s;
  }

  // Find filter handle and filter type
  if (rep->filter_policy) {
    for (auto filter_type :
         {Rep::FilterType::kFullFilter, Rep::FilterType::kPartitionedFilter,
          Rep::FilterType::kBlockFilter}) {
      std::string prefix;
      switch (filter_type) {
        case Rep::FilterType::kFullFilter:
          prefix = kFullFilterBlockPrefix;
          break;
        case Rep::FilterType::kPartitionedFilter:
          prefix = kPartitionedFilterBlockPrefix;
          break;
        case Rep::FilterType::kBlockFilter:
          prefix = kFilterBlockPrefix;
          break;
        default:
          assert(0);
      }
      std::string filter_block_key = prefix;
      filter_block_key.append(rep->filter_policy->Name());
      if (FindMetaBlock(meta_iter, filter_block_key, &rep->filter_handle).ok()) {
        rep->filter_type = filter_type;
        break;
      }
    }
  }

  // Read the properties
  bool found_properties_block = true;
  s = SeekToPropertiesBlock(meta_iter, &found_properties_block);

  if (!s.ok()) {
    __SE_LOG(WARN, "Error when seeking to properties block from file: %s", s.ToString().c_str());
  } else if (found_properties_block) {
    s = meta_iter->status();
    TableProperties* table_properties = nullptr;
    if (s.ok()) {
      s = ReadProperties(meta_iter->value(), rep->file.get(), rep->footer,
                         rep->ioptions, &table_properties);
    }

    if (!s.ok()) {
      __SE_LOG(WARN, "Encountered error while reading data from properties block %s", s.ToString().c_str());
    } else {
      rep->table_properties.reset(table_properties);
    }
  } else {
    __SE_LOG(ERROR, "Cannot find Properties block from file.");
  }

  // Read the compression dictionary meta block
  bool found_compression_dict;
  s = SeekToCompressionDictBlock(meta_iter, &found_compression_dict);
  if (!s.ok()) {
    __SE_LOG(WARN, "Error when seeking to compression dictionary block from file: %s", s.ToString().c_str());
  } else if (found_compression_dict) {
    // TODO(andrewkr): Add to block cache if cache_index_and_filter_blocks is
    // true.
    unique_ptr<BlockContents, ptr_destruct<BlockContents>>
    compression_dict_block(ALLOC_OBJECT(BlockContents, *rep->alloc_.get()));
    // TODO(andrewkr): ReadMetaBlock repeats SeekToCompressionDictBlock().
    // maybe decode a handle from meta_iter
    // and do ReadBlockContents(handle) instead
    s = table::ReadMetaBlock(
        rep->file.get(), file_size, kExtentBasedTableMagicNumber, rep->ioptions,
        kCompressionDictBlock, compression_dict_block.get());
    if (!s.ok()) {
      __SE_LOG(WARN, "Encountered error while reading data from compression dictionary block %s", s.ToString().c_str());
    } else {
      rep->compression_dict_block = std::move(compression_dict_block);
    }
  }

  // pre-fetching of blocks is turned on
  // Will use block cache for index/filter blocks access
  // Always prefetch index and filter for level 0
  if (table_options.cache_index_and_filter_blocks) {
    if (prefetch_index_and_filter_in_cache || level == 0) { // todo why level0 need add block cache ?
      assert(table_options.block_cache != nullptr);
      // Hack: Call NewIndexIterator() to implicitly add index to the
      // block_cache

      // if pin_l0_filter_and_index_blocks_in_cache is true and this is
      // a level0 file, then we will pass in this pointer to rep->index
      // to NewIndexIterator(), which will save the index block in there
      // else it's a nullptr and nothing special happens
      CachableEntry<IndexReader>* index_entry = nullptr;
      if (rep->table_options.pin_l0_filter_and_index_blocks_in_cache &&
          level == 0) {
        index_entry = &rep->index_entry;
      }
      unique_ptr<InternalIterator, ptr_destruct_delete<InternalIterator>> iter(
          new_table->NewIndexIterator(ReadOptions(), nullptr, index_entry));
      s = iter->status();

      if (s.ok()) {
        // Hack: Call GetFilter() to implicitly add filter to the block_cache
        auto filter_entry = new_table->GetFilter();
        // if pin_l0_filter_and_index_blocks_in_cache is true, and this is
        // a level0 file, then save it in rep_->filter_entry; it will be
        // released in the destructor only, hence it will be pinned in the
        // cache while this reader is alive
        if (rep->table_options.pin_l0_filter_and_index_blocks_in_cache &&
            level == 0) {
          rep->filter_entry = filter_entry;
          if (rep->filter_entry.value != nullptr) {
            rep->filter_entry.value->SetLevel(level);
          }
        } else {
          filter_entry.Release(table_options.block_cache.get());
        }
      }
    }
  } else {
    // If we don't use block cache for index/filter blocks access, we'll
    // pre-load these blocks, which will kept in member variables in Rep
    // and with a same life-time as this table object.
    IndexReader* index_reader = nullptr;
    s = new_table->CreateIndexReader(&index_reader, 
                                     nullptr /* aio_handle */,
                                     meta_iter,
                                     level,
                                     rep->alloc_.get());

    if (s.ok()) {
      rep->index_reader.reset(index_reader);
    } else {
//      delete index_reader;
      FREE_OBJECT(IndexReader, *rep->alloc_.get(), index_reader);
    }
  }

  if (s.ok()) {
//    *table_reader = std::move(new_table);
    table_reader = new_table;
  }

  return s;
}

void ExtentBasedTable::SetupForCompaction() {
  compaction_optimized_ = true;
}

std::shared_ptr<const TableProperties> ExtentBasedTable::GetTableProperties()
    const {
  return rep_->table_properties;
}

size_t ExtentBasedTable::ApproximateMemoryUsage() const {
  size_t usage = 0;
  if (rep_->filter) {
    usage += rep_->filter->ApproximateMemoryUsage();
  }
  if (rep_->index_reader) {
    usage += rep_->index_reader->ApproximateMemoryUsage();
  }
  return usage;
}

// Load the meta-block from the file. On success, return the loaded meta block
// and its iterator.
Status ExtentBasedTable::ReadMetaBlock(
    Rep* rep, Block *&meta_block,
    InternalIterator *&iter,
    SimpleAllocator *alloc) {
  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  Status s = ReadBlockFromFile(rep->file.get(),
                               rep->footer,
                               ReadOptions(),
                               rep->footer.metaindex_handle(),
                               meta_block,
                               rep->ioptions,
                               true /* decompress */,
                               Slice() /*compression dict*/,
                               kDisableGlobalSequenceNumber,
                               0 /* read_amp_bytes_per_bit */,
                               nullptr,
                               alloc);

  if (!s.ok()) {
    __SE_LOG(ERROR, "Encountered error while reading data from properties block %s", s.ToString().c_str());
    return s;
  }

  iter = meta_block->NewIterator(BytewiseComparator(), nullptr, true, nullptr, false, alloc);
  return Status::OK();
}

Status ExtentBasedTable::GetDataBlockFromCache(
    const Slice& block_cache_key, const Slice& compressed_block_cache_key,
    Cache* block_cache, Cache* block_cache_compressed,
    const ImmutableCFOptions& ioptions, const ReadOptions& read_options,
    ExtentBasedTable::CachableEntry<Block>* block, uint32_t format_version,
    const Slice& compression_dict, size_t read_amp_bytes_per_bit,
    bool is_index) {
  Status s;
  Block* compressed_block = nullptr;
  Cache::Handle* block_cache_compressed_handle = nullptr;

  // Lookup uncompressed cache first
  if (block_cache != nullptr) {
    block->cache_handle = GetEntryFromCache(
        block_cache, block_cache_key,
        is_index ? CountPoint::BLOCK_CACHE_INDEX_MISS :
                   CountPoint::BLOCK_CACHE_DATA_MISS,
        is_index ? CountPoint::BLOCK_CACHE_INDEX_HIT :
                   CountPoint::BLOCK_CACHE_DATA_HIT);
    if (block->cache_handle != nullptr) {
      block->value =
          reinterpret_cast<Block*>(block_cache->Value(block->cache_handle));
      return s;
    }
  }

  // If not found, search from the compressed block cache.
  assert(block->cache_handle == nullptr && block->value == nullptr);

  if (block_cache_compressed == nullptr) {
    return s;
  }

  assert(!compressed_block_cache_key.empty());
  block_cache_compressed_handle =
      block_cache_compressed->Lookup(compressed_block_cache_key);
  // if we found in the compressed cache, then uncompress and insert into
  // uncompressed cache
  if (block_cache_compressed_handle == nullptr) {
    QUERY_COUNT(CountPoint::BLOCK_CACHE_COMPRESSED_MISS);
    return s;
  }

  // found compressed block
  QUERY_COUNT(CountPoint::BLOCK_CACHE_COMPRESSED_HIT);
  compressed_block = reinterpret_cast<Block*>(
      block_cache_compressed->Value(block_cache_compressed_handle));
  assert(compressed_block->compression_type() != kNoCompression);

  // Retrieve the uncompressed contents into a new buffer
  BlockContents contents;
  s = UncompressBlockContents(compressed_block->data(),
                              compressed_block->size(), &contents,
                              format_version, compression_dict, ioptions);

  // Insert uncompressed block into block cache
  if (s.ok()) {
    block->value = MOD_NEW_OBJECT(ModId::kCache, Block, std::move(contents),
        compressed_block->global_seqno(), read_amp_bytes_per_bit);  // uncompressed block
//        new Block(std::move(contents), compressed_block->global_seqno(),
//                  read_amp_bytes_per_bit);  // uncompressed block
    assert(block->value->compression_type() == kNoCompression);
    if (block_cache != nullptr && block->value->cachable() &&
        read_options.fill_cache) {
      update_mod_id(block->value->data(), ModId::kDataBlockCache);
      s = block_cache->Insert(block_cache_key, block->value,
                              block->value->usable_size(),
                              &DeleteCachedEntry<Block>, &(block->cache_handle),
                              Cache::Priority::LOW);
      uint32_t shard_id =
          static_cast<ShardedCache *>(block_cache)->get_shard_id(block_cache_key);
      if (s.ok()) {
        QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD);
        QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_ADD, shard_id);
        if (is_index) {
          QUERY_COUNT(CountPoint::BLOCK_CACHE_INDEX_ADD);
          QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_INDEX_ADD,
                          block->value->usable_size());

          QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_INDEX_ADD, shard_id);
        } else {
          QUERY_COUNT(CountPoint::BLOCK_CACHE_DATA_ADD);
          QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_DATA_BYTES_INSERT,
                          block->value->usable_size());

          QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_DATA_ADD, shard_id);
        }
        QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_BYTES_WRITE,
                        block->value->usable_size());
      } else {
        QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD_FAILURES);
        QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_ADD_FAILURES, shard_id);
//        delete block->value;
        MOD_DELETE_OBJECT(Block, block->value);
        block->value = nullptr;
      }
    }
  }

  // Release hold on compressed cache entry
  block_cache_compressed->Release(block_cache_compressed_handle);
  return s;
}

Status ExtentBasedTable::PutDataBlockToCache(
    const Slice& block_cache_key, const Slice& compressed_block_cache_key,
    Cache* block_cache, Cache* block_cache_compressed,
    const ReadOptions& read_options, const ImmutableCFOptions& ioptions,
    CachableEntry<Block>* block, Block* raw_block, uint32_t format_version,
    const Slice& compression_dict, size_t read_amp_bytes_per_bit, bool is_index,
    Cache::Priority priority) {
  assert(raw_block->compression_type() == kNoCompression ||
         block_cache_compressed != nullptr);

  Status s;
  // Retrieve the uncompressed contents into a new buffer
  BlockContents contents;
  Statistics* statistics = ioptions.statistics;
  if (raw_block->compression_type() != kNoCompression) {
    s = UncompressBlockContents(raw_block->data(), raw_block->size(), &contents,
                                format_version, compression_dict, ioptions);
  }
  if (!s.ok()) {
//    delete raw_block;
    MOD_DELETE_OBJECT(Block, raw_block);
    return s;
  }

  if (raw_block->compression_type() != kNoCompression) {
//    block->value = new Block(std::move(contents), raw_block->global_seqno(),
//                             read_amp_bytes_per_bit);  // uncompressed block
    block->value = MOD_NEW_OBJECT(ModId::kDefaultMod, Block,
        std::move(contents), raw_block->global_seqno(), read_amp_bytes_per_bit);
  } else {
    block->value = raw_block;
    raw_block = nullptr;
  }

  // Insert compressed block into compressed block cache.
  // Release the hold on the compressed cache entry immediately.
  if (block_cache_compressed != nullptr && raw_block != nullptr &&
      raw_block->cachable()) {
    s = block_cache_compressed->Insert(compressed_block_cache_key, raw_block,
                                       raw_block->usable_size(),
                                       &DeleteCachedEntry<Block>, nullptr,
                                       Cache::Priority::LOW);
    if (s.ok()) {
      // Avoid the following code to delete this cached block.
      raw_block = nullptr;
      QUERY_COUNT(CountPoint::BLOCK_CACHE_COMPRESSED_ADD);
    } else {
      QUERY_COUNT(CountPoint::BLOCK_CACHE_COMPRESSED_ADD_FAILURES);
    }
  }
  delete raw_block;

  // insert into uncompressed block cache
  assert((block->value->compression_type() == kNoCompression));
  if (block_cache != nullptr && block->value->cachable()) {
    update_mod_id(block->value->data(), ModId::kDataBlockCache);
    s = block_cache->Insert(block_cache_key, block->value,
                            block->value->usable_size(),
                            &DeleteCachedEntry<Block>, &(block->cache_handle),
                            priority);
    uint32_t shard_id =
        static_cast<ShardedCache *>(block_cache)->get_shard_id(block_cache_key);
    if (s.ok()) {
      assert(block->cache_handle != nullptr);
      QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD);
      QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_ADD, shard_id);
      if (is_index) {
        QUERY_COUNT(CountPoint::BLOCK_CACHE_INDEX_ADD);
        QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_INDEX_ADD, shard_id);
        QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_INDEX_BYTES_INSERT,
                        block->value->usable_size());
      } else {
        QUERY_COUNT(CountPoint::BLOCK_CACHE_DATA_ADD);
        QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_DATA_ADD, shard_id);
        QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_DATA_BYTES_INSERT,
                        block->value->usable_size());
      }
      QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_BYTES_WRITE,
                      block->value->usable_size());
      assert(reinterpret_cast<Block*>(
                 block_cache->Value(block->cache_handle)) == block->value);
    } else {
      QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD_FAILURES);
      QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_ADD_FAILURES, shard_id);
//      delete block->value;
      MOD_DELETE_OBJECT(Block, block->value);
      block->value = nullptr;
    }
  }

  return s;
}

ExtentBasedTable::CachableEntry<FilterBlockReader> ExtentBasedTable::GetFilter(
    bool no_io) const {
  const BlockHandle& filter_blk_handle = rep_->filter_handle;
  const bool is_a_filter_partition = true;
  return GetFilter(filter_blk_handle, !is_a_filter_partition, no_io);
}

ExtentBasedTable::CachableEntry<FilterBlockReader> ExtentBasedTable::GetFilter(
    const BlockHandle& filter_blk_handle, const bool is_a_filter_partition,
    bool no_io) const {
  // disable filter
  return {nullptr /* filter */, nullptr /* cache handle */};

  Cache* block_cache = rep_->table_options.block_cache.get();
  if (block_cache == nullptr /* no block cache at all */ || !rep_->fd_valid ||
      !rep_->ioptions.filter_manager->is_working()) {
    return {nullptr /* filter */, nullptr /* cache handle */};
  }

  if (!is_a_filter_partition && rep_->filter_entry.IsSet()) {
    return rep_->filter_entry;
  }

  // Fetching from the cache
  char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  auto key = GetCacheKey(rep_->cache_key_prefix, rep_->cache_key_prefix_size,
                         filter_blk_handle, cache_key);
  FilterBlockReader* filter = nullptr;
  Cache::Handle* cache_handle = nullptr;
  Statistics* statistics = rep_->ioptions.statistics;
  Status s = rep_->ioptions.filter_manager->get_filter(
      key, block_cache, statistics, no_io, rep_->level, rep_->fd,
      rep_->file_read_hist, filter, cache_handle);
  if (!s.ok()) {
    return {nullptr /* filter */, nullptr /* cache handle */};
  }
  return {filter, cache_handle};
}

InternalIterator* ExtentBasedTable::create_index_iterator(
    const ReadOptions& read_options, BlockIter* input_iter,
    IndexReader*& index_reader, 
    ArenaAllocator &alloc) {
  // index reader has already been pre-populated.
  if (rep_->index_reader) {
    return rep_->index_reader->NewIterator(input_iter,
                                           read_options.total_order_seek);
  }
  // we have a pinned index block
  if (rep_->index_entry.IsSet()) {
    return rep_->index_entry.value->NewIterator(input_iter,
                                                read_options.total_order_seek);
  }

  // Create index reader and put it in the cache.
  Status s = CreateIndexReader(&index_reader, nullptr, nullptr, -1, &alloc);

  if (!s.ok()) {
    __SE_LOG(WARN, "create index iterator failed(%s).", s.ToString().c_str());
    if (index_reader != nullptr) {
//      delete index_reader;
      MOD_DELETE_OBJECT(IndexReader, index_reader);
    }
    // make sure if something goes wrong, index_reader shall remain intact.
    if (input_iter != nullptr) {
      input_iter->SetStatus(s);
      return input_iter;
    } else {
      return NewErrorInternalIterator(s);
    }
  }

  auto* iter =
      index_reader->NewIterator(input_iter, read_options.total_order_seek);
  // compaction index block not use block cache
  rep_->index_reader.reset(index_reader);
  return iter;
}

InternalIterator* ExtentBasedTable::create_data_block_iterator(
    const ReadOptions& ro, const BlockHandle& handle, BlockIter* input_iter) {
  // Didn't get any data from block caches.
  Slice compression_dict;
  if (rep_->compression_dict_block) {
    compression_dict = rep_->compression_dict_block->data;
  }

  rep_->data_block.reset();
  Block *tmp_block = nullptr;
  Status s = ReadBlockFromFile(rep_->file.get(),
                               rep_->footer,
                               ro,
                               handle,
                               tmp_block,
                               rep_->ioptions,
                               true /* compress */,
                               compression_dict,
                               rep_->global_seqno,
                               rep_->table_options.read_amp_bytes_per_bit,
                               nullptr /* aio_handle */,
                               nullptr /* allocaor */);
  rep_->data_block.reset(tmp_block);

  InternalIterator* iter = nullptr;
  if (s.ok()) {
    assert(rep_->data_block.get() != nullptr);
    iter = rep_->data_block->NewIterator(&rep_->internal_comparator, input_iter,
                                         true, rep_->ioptions.statistics);
  } else {
    __SE_LOG(WARN, "create data block(%ld,%ld) iterator failed(%s).", handle.offset(), handle.size(), s.ToString().c_str());
    if (input_iter != nullptr) {
      input_iter->SetStatus(s);
      iter = input_iter;
    } else {
      iter = NewErrorInternalIterator(s);
    }
  }
  return iter;
}

InternalIterator* ExtentBasedTable::NewIndexIterator(
    const ReadOptions& read_options, BlockIter* input_iter,
    CachableEntry<IndexReader>* index_entry) {
  // index reader has already been pre-populated.
  if (rep_->index_reader) {
    return rep_->index_reader->NewIterator(input_iter,
                                           read_options.total_order_seek);
  }
  // we have a pinned index block
  if (rep_->index_entry.IsSet()) {
    return rep_->index_entry.value->NewIterator(input_iter,
                                                read_options.total_order_seek);
  }

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  Cache* block_cache = rep_->table_options.block_cache.get();
  char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  auto key =
      GetCacheKeyFromOffset(rep_->cache_key_prefix, rep_->cache_key_prefix_size,
                            rep_->dummy_index_reader_offset, cache_key);
  auto cache_handle =
      GetEntryFromCache(block_cache, key,
                        CountPoint::BLOCK_CACHE_INDEX_MISS,
                        CountPoint::BLOCK_CACHE_INDEX_HIT);


  if (cache_handle == nullptr && no_io) {
    if (input_iter != nullptr) {
      input_iter->SetStatus(Status::Incomplete("no blocking io"));
      return input_iter;
    } else {
      return NewErrorInternalIterator(Status::Incomplete("no blocking io"));
    }
  }

  IndexReader* index_reader = nullptr;
  if (cache_handle != nullptr) {
    index_reader =
        reinterpret_cast<IndexReader*>(block_cache->Value(cache_handle));
  } else {
    QUERY_TRACE_SCOPE(TracePoint::LOAD_INDEX_BLOCK);
    // Create index reader and put it in the cache.
    Status s;
    TEST_SYNC_POINT("ExtentBasedTable::NewIndexIterator::thread2:2");
    s = CreateIndexReader(&index_reader);
    TEST_SYNC_POINT("ExtentBasedTable::NewIndexIterator::thread1:1");
    TEST_SYNC_POINT("ExtentBasedTable::NewIndexIterator::thread2:3");
    TEST_SYNC_POINT("ExtentBasedTable::NewIndexIterator::thread1:4");
    if (s.ok()) {
      assert(index_reader != nullptr);

      index_reader->set_mod_id(ModId::kIndexBlockCache);
      s = block_cache->Insert(
          key, index_reader, index_reader->usable_size(),
          &DeleteCachedIndexEntry, &cache_handle,
          rep_->table_options.cache_index_and_filter_blocks_with_high_priority
              ? Cache::Priority::HIGH
              : Cache::Priority::LOW);
    }
    uint32_t shard_id = static_cast<ShardedCache *>(block_cache)->get_shard_id(key);
    if (s.ok()) {
      size_t usable_size = index_reader->usable_size();
      QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD);
      QUERY_COUNT(CountPoint::BLOCK_CACHE_INDEX_ADD);
      QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_INDEX_BYTES_INSERT, usable_size);
      QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_BYTES_WRITE, usable_size);

      QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_ADD, shard_id)
      QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_INDEX_ADD, shard_id);
    } else {
      if (index_reader != nullptr) {
        delete index_reader;
      }
      QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD_FAILURES);
      QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_ADD_FAILURES, shard_id);
      // make sure if something goes wrong, index_reader shall remain intact.
      if (input_iter != nullptr) {
        input_iter->SetStatus(s);
        return input_iter;
      } else {
        return NewErrorInternalIterator(s);
      }
    }
  }

  assert(cache_handle);
  auto* iter =
      index_reader->NewIterator(input_iter, read_options.total_order_seek);

  // the caller would like to take ownership of the index block
  // don't call RegisterCleanup() in this case, the caller will take care of it
  if (index_entry != nullptr) {
    *index_entry = {index_reader, cache_handle};
  } else {
    iter->RegisterCleanup(&ReleaseCachedEntry, block_cache, cache_handle);
  }

  return iter;
}

InternalIterator* ExtentBasedTable::NewDataBlockIterator(
    Rep* rep, const ReadOptions& ro, const Slice& index_value,
    BlockIter* input_iter, bool is_index, uint64_t* add_blocks,
    const uint64_t scan_add_blocks_limit) {
  BlockHandle handle;
  Slice input = index_value;
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.
  Status s = handle.DecodeFrom(&input);
  return NewDataBlockIterator(rep, ro, handle,
      input_iter, is_index, s, add_blocks, scan_add_blocks_limit);
}

int ExtentBasedTable::get_data_block(const BlockHandle& handle,
                                     Slice& data_block, bool verify_checksums) {
  if (data_block.size() < handle.size() + kBlockTrailerSize) {
    return Status::kNoSpace;
  }

  ReadOptions ro;
  ro.verify_checksums = verify_checksums;
  Status s = ReadBlock(rep_->file.get(), rep_->footer, ro, handle, &data_block,
                       const_cast<char*>(data_block.data()));
  return s.code();
}

void ExtentBasedTable::set_mod_id(const size_t mod_id) const {
  if (nullptr != rep_ && nullptr != rep_->index_reader) {
    rep_->index_reader->set_mod_id(mod_id);
  }
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// If input_iter is null, new a iterator
// If input_iter is not null, update this iter and return it
InternalIterator* ExtentBasedTable::NewDataBlockIterator(
    Rep* rep, const ReadOptions& ro, const BlockHandle& handle,
    BlockIter* input_iter, bool is_index, Status s,
    uint64_t* add_blocks, const uint64_t scan_add_blocks_limit) {
  const bool no_io = (ro.read_tier == kBlockCacheTier);
  Cache* block_cache = rep->table_options.block_cache.get();
  CachableEntry<Block> block;
  Slice compression_dict;
  if (s.ok()) {
    if (rep->compression_dict_block) {
      compression_dict = rep->compression_dict_block->data;
    }
    s = MaybeLoadDataBlockToCache(rep, ro, handle, compression_dict, &block,
                                  is_index, add_blocks, scan_add_blocks_limit);
  }

  // Didn't get any data from block caches.
  if (s.ok() && block.value == nullptr) {
    if (no_io) {
      // Could not read from block_cache and can't do IO
      if (input_iter != nullptr) {
        input_iter->SetStatus(Status::Incomplete("no blocking io"));
        return input_iter;
      } else {
        return NewErrorInternalIterator(Status::Incomplete("no blocking io"));
      }
    }

    Block *block_value = nullptr;
    s = ReadBlockFromFile(rep->file.get(),
                          rep->footer,
                          ro,
                          handle,
                          block_value,
                          rep->ioptions,
                          true /* compress */,
                          compression_dict,
                          rep->global_seqno,
                          rep->table_options.read_amp_bytes_per_bit,
                          nullptr /* aio_handle */,
                          nullptr /* allocator */);
    if (s.ok()) {
      block.value = block_value;
    }
  }

  InternalIterator* iter;
  if (s.ok()) {
    assert(block.value != nullptr);
    iter = block.value->NewIterator(&rep->internal_comparator, input_iter, true,
                                    rep->ioptions.statistics);
    if (block.cache_handle != nullptr) {
      // todo release block.value
      iter->RegisterCleanup(&ReleaseCachedEntry, block_cache,
                            block.cache_handle);
    } else {
      iter->RegisterCleanup(&DeleteHeldResource<Block>, block.value, nullptr);
    }
  } else {
    assert(block.value == nullptr);
    if (input_iter != nullptr) {
      input_iter->SetStatus(s);
      iter = input_iter;
    } else {
      iter = NewErrorInternalIterator(s);
    }
  }
  return iter;
}

Status ExtentBasedTable::MaybeLoadDataBlockToCache(
    Rep* rep, const ReadOptions& ro, const BlockHandle& handle,
    Slice compression_dict, CachableEntry<Block>* block_entry,
    bool is_index, uint64_t* add_blocks,
    const uint64_t scan_add_blocks_limit) {
  const bool no_io = (ro.read_tier == kBlockCacheTier);
  Cache* block_cache = rep->table_options.block_cache.get();
  Cache* block_cache_compressed =
      rep->table_options.block_cache_compressed.get();

  // If either block cache is enabled, we'll try to read from it.
  Status s;
  if (block_cache != nullptr || block_cache_compressed != nullptr) {
    Statistics* statistics = rep->ioptions.statistics;
    char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    char compressed_cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    Slice key, /* key to the block cache */
        ckey /* key to the compressed block cache */;

    // create key for block cache
    if (block_cache != nullptr) {
      key = GetCacheKey(rep->cache_key_prefix, rep->cache_key_prefix_size,
                        handle, cache_key);
    }

    if (block_cache_compressed != nullptr) {
      ckey = GetCacheKey(rep->compressed_cache_key_prefix,
                         rep->compressed_cache_key_prefix_size, handle,
                         compressed_cache_key);
    }

    s = GetDataBlockFromCache(
        key, ckey, block_cache, block_cache_compressed, rep->ioptions, ro,
        block_entry, rep->table_options.format_version, compression_dict,
        rep->table_options.read_amp_bytes_per_bit, is_index);

    if (block_entry->value == nullptr && !no_io && ro.fill_cache) {
      Block *raw_block = nullptr;
      s = ReadBlockFromFile(rep->file.get(),
                            rep->footer,
                            ro,
                            handle,
                            raw_block,
                            rep->ioptions,
                            block_cache_compressed == nullptr,
                            compression_dict,
                            rep->global_seqno,
                            rep->table_options.read_amp_bytes_per_bit,
                            nullptr /* aio_handle */,
                            nullptr /* allocator */);

      if (s.ok()
          && (nullptr == add_blocks || *add_blocks < scan_add_blocks_limit)) {
        s = PutDataBlockToCache(
            key, ckey, block_cache, block_cache_compressed, ro, rep->ioptions,
            block_entry, raw_block, rep->table_options.format_version,
            compression_dict, rep->table_options.read_amp_bytes_per_bit,
            is_index,
            is_index &&
                    rep->table_options
                        .cache_index_and_filter_blocks_with_high_priority
                ? Cache::Priority::HIGH
                : Cache::Priority::LOW);
        if (s.ok() && nullptr != add_blocks) {
          ++(*add_blocks);
        }
      } else {
        // set return_val, avoid to read from file again
        block_entry->value = raw_block;
      }
    }
  }
  return s;
}

ExtentBasedTable::BlockEntryIteratorState::BlockEntryIteratorState(
    ExtentBasedTable* table, const ReadOptions& read_options, bool skip_filters,
    bool is_index, Cleanable* block_cache_cleaner, const uint64_t scan_add_blocks_limit)
    : TwoLevelIteratorState(),
      table_(table),
      read_options_(read_options),
      skip_filters_(skip_filters),
      is_index_(is_index),
      block_cache_cleaner_(block_cache_cleaner),
      scan_add_blocks_limit_(scan_add_blocks_limit) {}

InternalIterator*
ExtentBasedTable::BlockEntryIteratorState::NewSecondaryIterator(
    const Slice& index_value, uint64_t *add_blocks) {
  // Return a block iterator on the index partition
  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  auto iter = NewDataBlockIterator(table_->rep_, read_options_, handle, nullptr,
                                   is_index_, s, add_blocks, scan_add_blocks_limit_);
  if (block_cache_cleaner_) {
    uint64_t offset = handle.offset();
    {
      ReadLock rl(&cleaner_mu);
      if (cleaner_set.find(offset) != cleaner_set.end()) {
        // already have a refernce to the block cache objects
        return iter;
      }
    }
    WriteLock wl(&cleaner_mu);
    cleaner_set.insert(offset);
    // Keep the data into cache until the cleaner cleansup
    iter->DelegateCleanupsTo(block_cache_cleaner_);
  }
  return iter;
}

InternalIterator* ExtentBasedTable::NewIterator(const ReadOptions& read_options,
                                                SimpleAllocator* arena,
                                                bool skip_filters,
                                                const uint64_t scan_add_blocks_limit) {
  return NewTwoLevelIterator(
      MOD_NEW_OBJECT(ModId::kDbIter, BlockEntryIteratorState, this, read_options,
          skip_filters, false, nullptr, scan_add_blocks_limit),
      NewIndexIterator(read_options),
      TracePoint::INDEX_ITERATOR_NEXT,
      arena);
}

bool ExtentBasedTable::FullFilterKeyMayMatch(const ReadOptions& read_options,
                                             FilterBlockReader* filter,
                                             const Slice& internal_key,
                                             const bool no_io) const {
  if (filter == nullptr || filter->IsBlockBased()) {
    return true;
  }
  Slice user_key = ExtractUserKey(internal_key);
  const Slice* const const_ikey_ptr = &internal_key;
  if (filter->whole_key_filtering()) {
    return filter->KeyMayMatch(user_key, kNotValid, no_io, const_ikey_ptr);
  }
  se_assert(false);
}

int ExtentBasedTable::check_block_if_in_cache(const BlockHandle &block_handle, bool &in_cache)
{
  int ret = 0;
  in_cache = false;
  Cache* block_cache = rep_->table_options.block_cache.get();
  char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  Slice key = GetCacheKey(rep_->cache_key_prefix, rep_->cache_key_prefix_size,
      block_handle, cache_key);
  if (nullptr != block_cache) {
    in_cache = block_cache->check_in_cache(key);
  }
  return ret;
}

int ExtentBasedTable::erase_block_from_cache(const BlockHandle &block_handle)
{
  int ret = 0;
  Cache* block_cache = rep_->table_options.block_cache.get();
  char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  Slice key = GetCacheKey(rep_->cache_key_prefix, rep_->cache_key_prefix_size,
      block_handle, cache_key);
  if (nullptr != block_cache) {
    block_cache->Erase(key);
  }
  return ret;
}

Status ExtentBasedTable::Get(const ReadOptions& read_options, const Slice& key,
                             GetContext* get_context, bool skip_filters) {
  QUERY_TRACE_SCOPE(TracePoint::EXTENT_GET);
  Status s;
  const bool no_io = read_options.read_tier == kBlockCacheTier;
  CachableEntry<FilterBlockReader> filter_entry;
  if (!skip_filters) {
    filter_entry = GetFilter(read_options.read_tier == kBlockCacheTier);
  }
  FilterBlockReader* filter = filter_entry.value;

  // First check the full filter
  // If full filter not useful, Then go into each block
  if (!FullFilterKeyMayMatch(read_options, filter, key, no_io)) {
    QUERY_COUNT(CountPoint::BLOOM_FILTER_USEFUL);
  } else {
    BlockIter iiter_on_stack;
    auto iiter = NewIndexIterator(read_options, &iiter_on_stack);
    std::unique_ptr<InternalIterator, ptr_destruct_delete<InternalIterator>> iiter_unique_ptr;
    if (iiter != &iiter_on_stack) {
      iiter_unique_ptr.reset(iiter);
    }

    bool done = false;
    for (iiter->Seek(key); iiter->Valid() && !done; iiter->Next()) {
      Slice handle_value = iiter->value();

      BlockHandle handle;
      bool not_exist_in_filter =
          filter != nullptr && filter->IsBlockBased() == true &&
          handle.DecodeFrom(&handle_value).ok() &&
          !filter->KeyMayMatch(ExtractUserKey(key), handle.offset(), no_io);

      if (not_exist_in_filter) {
        // Not found
        // TODO: think about interaction with Merge. If a user key cannot
        // cross one data block, we should be fine.
        QUERY_COUNT(CountPoint::BLOOM_FILTER_USEFUL);
        break;
      } else {
        BlockIter biter;
        NewDataBlockIterator(rep_, read_options, iiter->value(), &biter);

        if (read_options.read_tier == kBlockCacheTier &&
            biter.status().IsIncomplete()) {
          // couldn't get block from block_cache
          // Update Saver.state to Found because we are only looking for whether
          // we can guarantee the key is not there when "no_io" is set
          get_context->MarkKeyMayExist();
          break;
        }
        if (!biter.status().ok()) {
          s = biter.status();
          break;
        }

        // Call the *saver function on each entry/block until it returns false
        for (biter.Seek(key); biter.Valid(); biter.Next()) {
          ParsedInternalKey parsed_key;
          if (!ParseInternalKey(biter.key(), &parsed_key)) {
            s = Status::Corruption(Slice());
          }

          if (!get_context->SaveValue(parsed_key, biter.value(), &biter)) {
            done = true;

            if ((get_context->State() == GetContext::kFound) && (parsed_key.type == kTypeValueLarge)) {
              storage::RandomAccessExtent *file = dynamic_cast<storage::RandomAccessExtent *>(rep_->file->file());
              assert(file);
              Slice result;
              LargeValue large_value;
              std::unique_ptr<char[], ptr_delete<char>> unzip_buf;
              size_t unzip_buf_size = 0;
              std::unique_ptr<char[], void(&)(void *)> oob_uptr(nullptr, base_memalign_free);
              int64_t oob_size = 0;
              int ret = Status::kOk;
              if (FAILED(get_oob_large_value(biter.value(), large_value, oob_uptr, oob_size))) {
                __SE_LOG(ERROR, "fail to get content of large value\n");
                s = Status::kCorruption;
              } else if (kNoCompression == large_value.compression_type_) {
                get_context->SaveLargeValue(Slice(oob_uptr.get(), large_value.size_));
              } else if (FAILED(unzip_data(oob_uptr.get(),
                                           large_value.size_,
                                           LargeValue::COMPRESSION_FORMAT_VERSION,
                                           static_cast<CompressionType>(large_value.compression_type_),
                                           unzip_buf,
                                           unzip_buf_size))) {
                __SE_LOG(ERROR, "fail to unzip large value\n");
                s = Status::kCorruption;
              } else {
                get_context->SaveLargeValue(Slice(unzip_buf.get(), unzip_buf_size));
              }
            }
            break;
          }
        }
        s = biter.status();
      }
      if (done) {
        // Avoid the extra Next which is expensive in two-level indexes
        break;
      }
    }
    if (s.ok()) {
      s = iiter->status();
    }
  }

  // if rep_->filter_entry is not set, we should call Release(); otherwise
  // don't call, in this case we have a local copy in rep_->filter_entry,
  // it's pinned to the cache and will be released in the destructor
  if (!rep_->filter_entry.IsSet()) {
    filter_entry.Release(rep_->table_options.block_cache.get());
  }
  return s;
}

Status ExtentBasedTable::Prefetch(const Slice* const begin,
                                  const Slice* const end) {
  auto& comparator = rep_->internal_comparator;
  // pre-condition
  if (begin && end && comparator.Compare(*begin, *end) > 0) {
    return Status::InvalidArgument(*begin, *end);
  }

  BlockIter iiter_on_stack;
  auto iiter = NewIndexIterator(ReadOptions(), &iiter_on_stack);
  std::unique_ptr<InternalIterator, ptr_destruct_delete<InternalIterator>> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
//    iiter_unique_ptr = std::unique_ptr<InternalIterator>(iiter);
    iiter_unique_ptr.reset(iiter);
  }

  if (!iiter->status().ok()) {
    // error opening index iterator
    return iiter->status();
  }

  // indicates if we are on the last page that need to be pre-fetched
  bool prefetching_boundary_page = false;

  for (begin ? iiter->Seek(*begin) : iiter->SeekToFirst(); iiter->Valid();
       iiter->Next()) {
    Slice block_handle = iiter->value();

    if (end && comparator.Compare(iiter->key(), *end) >= 0) {
      if (prefetching_boundary_page) {
        break;
      }

      // The index entry represents the last key in the data block.
      // We should load this page into memory as well, but no more
      prefetching_boundary_page = true;
    }

    // Load the block specified by the block_handle into the block cache
    BlockIter biter;
    NewDataBlockIterator(rep_, ReadOptions(), block_handle, &biter);

    if (!biter.status().ok()) {
      // there was an unexpected error while pre-fetching
      return biter.status();
    }
  }

  return Status::OK();
}

bool ExtentBasedTable::TEST_KeyInCache(const ReadOptions& options,
                                       const Slice& key) {
  std::unique_ptr<InternalIterator, ptr_destruct_delete<InternalIterator>> iiter(NewIndexIterator(options));
  iiter->Seek(key);
  assert(iiter->Valid());
  CachableEntry<Block> block;

  BlockHandle handle;
  Slice input = iiter->value();
  Status s = handle.DecodeFrom(&input);
  assert(s.ok());
  Cache* block_cache = rep_->table_options.block_cache.get();
  assert(block_cache != nullptr);

  char cache_key_storage[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
  Slice cache_key =
      GetCacheKey(rep_->cache_key_prefix, rep_->cache_key_prefix_size, handle,
                  cache_key_storage);
  Slice ckey;

  s = GetDataBlockFromCache(
      cache_key, ckey, block_cache, nullptr, rep_->ioptions, options, &block,
      rep_->table_options.format_version,
      rep_->compression_dict_block ? rep_->compression_dict_block->data
                                   : Slice(),
      0 /* read_amp_bytes_per_bit */);
  assert(s.ok());
  bool in_cache = block.value != nullptr;
  if (in_cache) {
    ReleaseCachedEntry(block_cache, block.cache_handle);
  }
  return in_cache;
}

// REQUIRES: The following fields of rep_ should have already been populated:
//  1. file
//  2. index_handle,
//  3. options
//  4. internal_comparator
Status ExtentBasedTable::CreateIndexReader(IndexReader** index_reader, 
                                           AIOHandle *aio_handle,
                                           InternalIterator* preloaded_meta_index_iter,
                                           int level,
                                           SimpleAllocator *alloc)
{
  auto file = rep_->file.get();
  auto comparator = &rep_->internal_comparator;
  const Footer& footer = rep_->footer;

  return BinarySearchIndexReader::Create(file,
                                         footer,
                                         footer.index_handle(),
                                         rep_->ioptions,
                                         comparator,
                                         index_reader,
                                         aio_handle,
                                         alloc);
}

uint64_t ExtentBasedTable::ApproximateOffsetOf(const Slice& key) {
  unique_ptr<InternalIterator, ptr_destruct_delete<InternalIterator>> index_iter(NewIndexIterator(ReadOptions()));

  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->footer.metaindex_handle().offset();
    }
  } else {
    // key is past the last key in the file. If table_properties is not
    // available, approximate the offset by returning the offset of the
    // metaindex block (which is right near the end of the file).
    result = 0;
    if (rep_->table_properties) {
      result = rep_->table_properties->data_size;
    }
    // table_properties is not present in the table.
    if (result == 0) {
      result = rep_->footer.metaindex_handle().offset();
    }
  }
  return result;
}

bool ExtentBasedTable::TEST_filter_block_preloaded() const {
  return rep_->filter != nullptr;
}

bool ExtentBasedTable::TEST_index_reader_preloaded() const {
  return rep_->index_reader != nullptr;
}

Status ExtentBasedTable::GetKVPairsFromDataBlocks(
    std::vector<KVPairBlock>* kv_pair_blocks) {
  std::unique_ptr<InternalIterator, ptr_destruct_delete<InternalIterator>> blockhandles_iter(
      NewIndexIterator(ReadOptions()));

  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    // Cannot read Index Block
    return s;
  }

  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       blockhandles_iter->Next()) {
    s = blockhandles_iter->status();

    if (!s.ok()) {
      break;
    }

    std::unique_ptr<InternalIterator> datablock_iter;
    datablock_iter.reset(
        NewDataBlockIterator(rep_, ReadOptions(), blockhandles_iter->value()));
    s = datablock_iter->status();

    if (!s.ok()) {
      // Error reading the block - Skipped
      continue;
    }

    KVPairBlock kv_pair_block;
    for (datablock_iter->SeekToFirst(); datablock_iter->Valid();
         datablock_iter->Next()) {
      s = datablock_iter->status();
      if (!s.ok()) {
        // Error reading the block - Skipped
        break;
      }
      const Slice& key = datablock_iter->key();
      const Slice& value = datablock_iter->value();
      std::string key_copy = std::string(key.data(), key.size());
      std::string value_copy = std::string(value.data(), value.size());

      kv_pair_block.push_back(
          std::make_pair(std::move(key_copy), std::move(value_copy)));
    }
    kv_pair_blocks->push_back(std::move(kv_pair_block));
  }
  return Status::OK();
}

Status ExtentBasedTable::DumpTable(WritableFile* out_file) {
  // Output Footer
  out_file->Append(
      "Footer Details:\n"
      "--------------------------------------\n"
      "  ");
  out_file->Append(rep_->footer.ToString().c_str());
  out_file->Append("\n");

  // Output MetaIndex
  out_file->Append(
      "Metaindex Details:\n"
      "--------------------------------------\n");
  std::unique_ptr<Block, ptr_destruct_delete<Block>> meta_ptr;
  std::unique_ptr<InternalIterator, ptr_destruct_delete<InternalIterator>> meta_iter_ptr;
  Block *meta = nullptr;
  InternalIterator *meta_iter = nullptr;
  Status s = ReadMetaBlock(rep_, meta, meta_iter);
  meta_ptr.reset(meta);
  meta_iter_ptr.reset(meta_iter);
  if (s.ok()) {
    for (meta_iter->SeekToFirst(); meta_iter->Valid(); meta_iter->Next()) {
      s = meta_iter->status();
      if (!s.ok()) {
        return s;
      }
      if (meta_iter->key() == kPropertiesBlock) {
        out_file->Append("  Properties block handle: ");
        out_file->Append(meta_iter->value().ToString(true).c_str());
        out_file->Append("\n");
      } else if (meta_iter->key() == kCompressionDictBlock) {
        out_file->Append("  Compression dictionary block handle: ");
        out_file->Append(meta_iter->value().ToString(true).c_str());
        out_file->Append("\n");
      } else if (strstr(meta_iter->key().ToString().c_str(),
                        "filter.rocksdb.") != nullptr) {
        out_file->Append("  Filter block handle: ");
        out_file->Append(meta_iter->value().ToString(true).c_str());
        out_file->Append("\n");
      }
    }
    out_file->Append("\n");
  } else {
    return s;
  }

  // Output TableProperties
  const TableProperties* table_properties;
  table_properties = rep_->table_properties.get();

  if (table_properties != nullptr) {
    out_file->Append(
        "Table Properties:\n"
        "--------------------------------------\n"
        "  ");
    out_file->Append(table_properties->ToString("\n  ", ": ").c_str());
    out_file->Append("\n");
  }

  // Output Filter blocks
  if (!rep_->filter && !table_properties->filter_policy_name.empty()) {
    // Support only BloomFilter as off now
    BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(NewBloomFilterPolicy(1));
    if (table_properties->filter_policy_name.compare(
            table_options.filter_policy->Name()) == 0) {
      std::string filter_block_key = kFilterBlockPrefix;
      filter_block_key.append(table_properties->filter_policy_name);
      BlockHandle handle;
      if (FindMetaBlock(meta_iter, filter_block_key, &handle).ok()) {
        BlockContents block;
        if (ReadBlockContents(rep_->file.get(),
                              rep_->footer,
                              ReadOptions(),
                              handle,
                              &block,
                              rep_->ioptions,
                              false /*decompress*/,
                              Slice() /*compression dict*/,
                              nullptr /*aio_handle*/)
                .ok()) {
          abort();
        }
      }
    }
  }
  if (rep_->filter) {
    out_file->Append(
        "Filter Details:\n"
        "--------------------------------------\n"
        "  ");
    out_file->Append(rep_->filter->ToString().c_str());
    out_file->Append("\n");
  }

  // Output Index block
  s = DumpIndexBlock(out_file);
  if (!s.ok()) {
    return s;
  }

  // Output compression dictionary
  if (rep_->compression_dict_block != nullptr) {
    auto compression_dict = rep_->compression_dict_block->data;
    out_file->Append(
        "Compression Dictionary:\n"
        "--------------------------------------\n");
    out_file->Append("  size (bytes): ");
    out_file->Append(util::ToString(compression_dict.size()));
    out_file->Append("\n\n");
    out_file->Append("  HEX    ");
    out_file->Append(compression_dict.ToString(true).c_str());
    out_file->Append("\n\n");
  }

  // Output Data blocks
  s = DumpDataBlocks(out_file);

  return s;
}

Status ExtentBasedTable::check_range(const Slice &start, const Slice &end, bool &result) {
  std::unique_ptr<Block, ptr_destruct_delete<Block>> meta_ptr;
  std::unique_ptr<InternalIterator, ptr_destruct_delete<InternalIterator>> meta_iter_ptr;
  Block *meta = nullptr;
  InternalIterator *meta_iter = nullptr;
  Status s = ReadMetaBlock(rep_, meta, meta_iter);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<InternalIterator, ptr_destruct_delete<InternalIterator>> blockhandles_iter(
      NewIndexIterator(ReadOptions()));
  s = blockhandles_iter->status();
  if (!s.ok()) {
    fprintf(stderr, "Can not read Index Block \n\n");
    return s;
  }

  const int64_t max_buf_size = 1024;
  char buf[max_buf_size];

  blockhandles_iter->SeekToFirst();
  if (!blockhandles_iter->Valid()) {
    s = blockhandles_iter->status();
    if (!s.ok()) {
      fprintf(stderr, "seek to index block first key failed %d", s.code());
    } else {
      fprintf(stderr, "check a empty extent\n");
      result = false;
    }
    return s;
  }

  Slice key = blockhandles_iter->key();
  InternalKey ikey;
  ikey.DecodeFrom(key);
  Slice block_index_content = blockhandles_iter->value();
  BlockHandle handle;
  handle.DecodeFrom(const_cast<Slice*>(&block_index_content));
  BlockStats block_stats;
  block_stats.decode(block_index_content);
  if (block_stats.first_key_.size() != start.size() || 
      memcmp(block_stats.first_key_.c_str(), start.data(), start.size())) {
    SE_LOG(ERROR, "meta start key is not match extent", K(start), K(block_stats.first_key_)); 
    result = false;
  }

  return s;
}

void ExtentBasedTable::Close() {
  rep_->filter_entry.Release(rep_->table_options.block_cache.get());
  rep_->index_entry.Release(rep_->table_options.block_cache.get());
  // cleanup index and filter blocks to avoid accessing dangling pointer
  if (!rep_->table_options.no_block_cache) {
    char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    // Get the filter block key
    auto key = GetCacheKey(rep_->cache_key_prefix, rep_->cache_key_prefix_size,
                           rep_->filter_handle, cache_key);
    rep_->table_options.block_cache.get()->Erase(key);
    // Get the index block key
    key = GetCacheKeyFromOffset(rep_->cache_key_prefix,
                                rep_->cache_key_prefix_size,
                                rep_->dummy_index_reader_offset, cache_key);
    rep_->table_options.block_cache.get()->Erase(key);
  }
}

uint64_t ExtentBasedTable::get_usable_size() {
  return sizeof(*this) + rep_->get_usable_size();
}

Status ExtentBasedTable::DumpIndexBlock(WritableFile* out_file) {
  out_file->Append(
      "Index Details:\n"
      "--------------------------------------\n");

  std::unique_ptr<InternalIterator, ptr_destruct_delete<InternalIterator>> blockhandles_iter(
      NewIndexIterator(ReadOptions()));
  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    out_file->Append("Can not read Index Block \n\n");
    return s;
  }

  const int64_t max_buf_size = 1024;
  char buf[max_buf_size];
  char block_stats_buffer[1024];

  out_file->Append("  Block key hex dump: Data block handle\n");
  out_file->Append("  Block key ascii\n\n");
  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       blockhandles_iter->Next()) {
    s = blockhandles_iter->status();
    if (!s.ok()) {
      break;
    }
    Slice key = blockhandles_iter->key();
    InternalKey ikey;
    ikey.DecodeFrom(key);

    out_file->Append("  HEX    ");
    out_file->Append(ikey.user_key().ToString(true).c_str());

    Slice block_index_content = blockhandles_iter->value();
    BlockHandle handle;
    handle.DecodeFrom(const_cast<Slice*>(&block_index_content));
    snprintf(buf, max_buf_size, ",SEQ:%ld,Type:%d @ (%ld,%ld) : ",
             GetInternalKeySeqno(key), ExtractValueType(key), handle.offset(),
             handle.size());
    out_file->Append(Slice(buf, strlen(buf)));

    BlockStats block_stats;
    block_stats.decode(block_index_content);
    block_stats.to_string(block_stats_buffer, 1024);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"
    snprintf(buf, max_buf_size, "%s, first_key:(",block_stats_buffer);
#pragma GCC diagnostic pop    
    out_file->Append(Slice(buf, strlen(buf)));
    out_file->Append(
        Slice(block_stats.first_key_.c_str(), block_stats.first_key_.size())
            .ToString(true)
            .c_str());
    out_file->Append(")");
    out_file->Append(blockhandles_iter->value().ToString(true).c_str());
    out_file->Append("\n");

    std::string str_key = ikey.user_key().ToString();
    std::string res_key("");
    char cspace = ' ';
    for (size_t i = 0; i < str_key.size(); i++) {
      res_key.append(&str_key[i], 1);
      res_key.append(1, cspace);
    }
    out_file->Append("  ASCII  ");
    out_file->Append(res_key.c_str());
    out_file->Append("\n  ------\n");
  }
  out_file->Append("\n");
  return Status::OK();
}

Status ExtentBasedTable::DumpDataBlocks(WritableFile* out_file) {
  std::unique_ptr<InternalIterator, ptr_destruct_delete<InternalIterator>> blockhandles_iter(
      NewIndexIterator(ReadOptions()));
  Status s = blockhandles_iter->status();
  if (!s.ok()) {
    out_file->Append("Can not read Index Block \n\n");
    return s;
  }

  uint64_t datablock_size_min = std::numeric_limits<uint64_t>::max();
  uint64_t datablock_size_max = 0;
  uint64_t datablock_size_sum = 0;

  size_t block_id = 1;
  for (blockhandles_iter->SeekToFirst(); blockhandles_iter->Valid();
       block_id++, blockhandles_iter->Next()) {
    s = blockhandles_iter->status();
    if (!s.ok()) {
      break;
    }

    Slice bh_val = blockhandles_iter->value();
    BlockHandle bh;
    bh.DecodeFrom(&bh_val);
    uint64_t datablock_size = bh.size();
    datablock_size_min = std::min(datablock_size_min, datablock_size);
    datablock_size_max = std::max(datablock_size_max, datablock_size);
    datablock_size_sum += datablock_size;

    out_file->Append("Data Block # ");
    out_file->Append(util::ToString(block_id));
    out_file->Append(" @ ");
    out_file->Append(blockhandles_iter->value().ToString(true).c_str());
    out_file->Append("\n");
    out_file->Append("--------------------------------------\n");

    std::unique_ptr<InternalIterator, ptr_destruct_delete<InternalIterator>> datablock_iter;
    datablock_iter.reset(
        NewDataBlockIterator(rep_, ReadOptions(), blockhandles_iter->value()));
    s = datablock_iter->status();

    if (!s.ok()) {
      out_file->Append("Error reading the block - Skipped \n\n");
      continue;
    }

    for (datablock_iter->SeekToFirst(); datablock_iter->Valid();
         datablock_iter->Next()) {
      s = datablock_iter->status();
      if (!s.ok()) {
        out_file->Append("Error reading the block - Skipped \n");
        break;
      }
      DumpKeyValue(datablock_iter->key(), datablock_iter->value(), out_file);
    }
    out_file->Append("\n");
  }

  uint64_t num_datablocks = block_id - 1;
  if (num_datablocks) {
    double datablock_size_avg =
        static_cast<double>(datablock_size_sum) / num_datablocks;
    out_file->Append("Data Block Summary:\n");
    out_file->Append("--------------------------------------");
    out_file->Append("\n  # data blocks: ");
    out_file->Append(util::ToString(num_datablocks));
    out_file->Append("\n  min data block size: ");
    out_file->Append(util::ToString(datablock_size_min));
    out_file->Append("\n  max data block size: ");
    out_file->Append(util::ToString(datablock_size_max));
    out_file->Append("\n  avg data block size: ");
    out_file->Append(util::ToString(datablock_size_avg));
    out_file->Append("\n");
  }

  return Status::OK();
}

void ExtentBasedTable::DumpKeyValue(const Slice& key, const Slice& value,
                                    WritableFile* out_file) {
  InternalKey ikey;
  ikey.DecodeFrom(key);

  const int64_t max_buf_size = 256;
  char buf[max_buf_size];

  out_file->Append("  HEX    ");
  out_file->Append(ikey.user_key().ToString(true).c_str());
  snprintf(buf, max_buf_size, ",SEQ:%ld,Type:%d", GetInternalKeySeqno(key),
           ExtractValueType(key));
  out_file->Append(Slice(buf, strlen(buf)));
  out_file->Append(": ");
  out_file->Append(value.ToString(true).c_str());
  out_file->Append("\n");

  std::string str_key = ikey.user_key().ToString();
  std::string str_value = value.ToString();
  std::string res_key(""), res_value("");
  char cspace = ' ';
  for (size_t i = 0; i < str_key.size(); i++) {
    res_key.append(&str_key[i], 1);
    res_key.append(1, cspace);
  }
  for (size_t i = 0; i < str_value.size(); i++) {
    res_value.append(&str_value[i], 1);
    res_value.append(1, cspace);
  }

  out_file->Append("  ASCII  ");
  out_file->Append(res_key.c_str());
  out_file->Append(": ");
  out_file->Append(res_value.c_str());
  out_file->Append("\n  ------\n");
}

const BlockBasedTableOptions& ExtentBasedTable::get_table_options() {
  return rep_->table_options;
}

int ExtentBasedTable::prefetch_index_block(BlockDataHandle<IndexReader> &handle)
{
  int ret = Status::kOk;
  if (rep_->index_reader) {
    // index reader has already been pre-populated.
    handle.block_entry_.value = rep_->index_reader.get();
    handle.need_do_cleanup_ = false;
  } else if (rep_->index_entry.IsSet()) {
    // we have a pinned index block
    handle.block_entry_.value = rep_->index_entry.value;
    handle.need_do_cleanup_ = false;
  } else {
    // get from block cache
    Cache* block_cache = rep_->table_options.block_cache.get();
    char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    auto key = GetCacheKeyFromOffset(rep_->cache_key_prefix,
        rep_->cache_key_prefix_size,
        rep_->dummy_index_reader_offset,
        cache_key);
    auto cache_handle = GetEntryFromCache(block_cache,
        key,
        CountPoint::BLOCK_CACHE_INDEX_MISS,
        CountPoint::BLOCK_CACHE_INDEX_HIT);
    if (nullptr != cache_handle) {
      handle.block_entry_.cache_handle = cache_handle;
      handle.block_entry_.value = reinterpret_cast<IndexReader*>(block_cache->Value(cache_handle));
      handle.cache_ = rep_->table_options.block_cache.get();
      handle.need_do_cleanup_ = true;
    } else {
      handle.block_handle_ =  rep_->footer.index_handle();
      handle.aio_handle_.aio_req_.reset(new AIOReq()); // sharded_ptr
      if (FAILED(do_io_prefetch(handle.block_handle_.offset(),
                                handle.block_handle_.size() + kBlockTrailerSize,
                                &handle.aio_handle_))) {
        SE_LOG(WARN, "failed to prefetch", K(ret));
      }
    }
  }
  return ret;
}

int ExtentBasedTable::prefetch_data_block(const ReadOptions &read_options,
                                          BlockDataHandle<Block> &handle)
{
  int ret = Status::kOk;
  // get from block cache
  Cache* block_cache = rep_->table_options.block_cache.get();
  Cache* block_cache_compressed = rep_->table_options.block_cache_compressed.get();
  // If either block cache is enabled, we'll try to read from it.
  if (block_cache != nullptr || block_cache_compressed != nullptr) {
    Statistics* statistics = rep_->ioptions.statistics;
    char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    char compressed_cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
    Slice key, /* key to the block cache */
          ckey /* key to the compressed block cache */;

    // create key for block cache
    if (block_cache != nullptr) {
      key = GetCacheKey(rep_->cache_key_prefix,
                        rep_->cache_key_prefix_size,
                        handle.block_handle_,
                        cache_key);
    }

    if (block_cache_compressed != nullptr) {
      ckey = GetCacheKey(rep_->compressed_cache_key_prefix,
                         rep_->compressed_cache_key_prefix_size,
                         handle.block_handle_,
                         compressed_cache_key);
    }
    Slice compression_dict;
    if (FAILED(GetDataBlockFromCache(key,
                                     ckey,
                                     block_cache,
                                     block_cache_compressed,
                                     rep_->ioptions,
                                     read_options,
                                     &handle.block_entry_,
                                     rep_->table_options.format_version,
                                     compression_dict,
                                     rep_->table_options.read_amp_bytes_per_bit,
                                     false /* is_index */).code())) {
      SE_LOG(WARN, "failed to get data block from cache", K(ret));
    } else if (nullptr != handle.block_entry_.cache_handle) {
      handle.cache_ = rep_->table_options.block_cache.get();
      handle.need_do_cleanup_ = true;
    }
  }
  return ret;
}

int ExtentBasedTable::do_io_prefetch(const int64_t offset,
                                     const int64_t size,
                                     AIOHandle *aio_handle)
{
  int ret = Status::kOk;
  if (IS_NULL(aio_handle)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "aio handle is nullptr", K(ret));
  } else if (FAILED(rep_->file->prefetch(offset, size, aio_handle))) {
    SE_LOG(WARN, "failed to prefetch", K(ret));
  }
  return ret;
}

int ExtentBasedTable::new_index_iterator(const ReadOptions &read_options,
                                         BlockDataHandle<IndexReader> &handle,
                                         BlockIter &index_block_iter)
{
  int ret = Status::kOk;
  if (nullptr == handle.block_entry_.value && nullptr == handle.block_entry_.cache_handle) {
    const bool no_io = (read_options.read_tier == kBlockCacheTier);
    if (no_io) {
      index_block_iter.SetStatus(Status::Incomplete("no blocking io"));
    } else {
      IndexReader* index_reader = nullptr;
      if (FAILED(CreateIndexReader(&index_reader, &handle.aio_handle_).code())) {
        SE_LOG(WARN, "failed to create index reader", K(ret));
      } else if (IS_NULL(index_reader)) {
        ret = Status::kErrorUnexpected;
        SE_LOG(WARN, "index reader is nullptr", K(ret));
      } else {
        index_reader->set_mod_id(ModId::kIndexBlockCache);
        Cache* block_cache = rep_->table_options.block_cache.get();
        char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
        auto key = GetCacheKeyFromOffset(rep_->cache_key_prefix,
            rep_->cache_key_prefix_size,
            rep_->dummy_index_reader_offset,
            cache_key);
        if (FAILED(block_cache->Insert(key,
                index_reader,
                index_reader->usable_size(),
                &DeleteCachedIndexEntry,
                &handle.block_entry_.cache_handle,
                rep_->table_options.cache_index_and_filter_blocks_with_high_priority
                ? Cache::Priority::HIGH
                : Cache::Priority::LOW).code())) {
          SE_LOG(WARN, "failed to insert to block cache", K(ret));
          delete index_reader;
          index_reader = nullptr;
          QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD_FAILURES);
        } else {
          handle.block_entry_.value = index_reader;
          size_t usable_size = index_reader->usable_size();
          QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD);
          QUERY_COUNT(CountPoint::BLOCK_CACHE_INDEX_ADD);
          QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_INDEX_BYTES_INSERT, usable_size);
          QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_BYTES_WRITE, usable_size);
          handle.cache_ = rep_->table_options.block_cache.get();
          handle.need_do_cleanup_ = true;
        }
      }
    }
  }
  if (SUCCED(ret)) {
    if (IS_NULL(handle.block_entry_.value)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "block cache value is nullptr", K(ret));
    } else {
      handle.block_entry_.value->NewIterator(&index_block_iter, read_options.total_order_seek);
      index_block_iter.set_source(rep_->fd.extent_id.id());
    }
  }
  return ret;
}

int ExtentBasedTable::new_data_block_iterator(const ReadOptions &read_options,
                                              BlockDataHandle<Block> &handle,
                                              BlockIter &data_block_iter,
                                              uint64_t *add_blocks,
                                              const uint64_t scan_add_blocks_limit)
{
  int ret = Status::kOk;
  if (nullptr == handle.block_entry_.cache_handle) {
    const bool no_io = (read_options.read_tier == kBlockCacheTier);
    if (no_io) {
      data_block_iter.SetStatus(Status::Incomplete("no blocking io"));
    } else {
      std::unique_ptr<Block, ptr_destruct_delete<Block>> raw_block_ptr;
      Block *raw_block = nullptr;
      Slice compression_dict;
      Cache* block_cache = rep_->table_options.block_cache.get();
      Cache* block_cache_compressed = rep_->table_options.block_cache_compressed.get();
      char cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
      char compressed_cache_key[kMaxCacheKeyPrefixSize + kMaxVarint64Length];
      Slice key, /* key to the block cache */
            ckey /* key to the compressed block cache */;

      // create key for block cache
      if (block_cache != nullptr) {
        key = GetCacheKey(rep_->cache_key_prefix,
                          rep_->cache_key_prefix_size,
                          handle.block_handle_,
                          cache_key);
      }

      if (block_cache_compressed != nullptr) {
        ckey = GetCacheKey(rep_->compressed_cache_key_prefix,
                           rep_->compressed_cache_key_prefix_size,
                           handle.block_handle_,
                           compressed_cache_key);
      }
      {
        if (FAILED(ReadBlockFromFile(rep_->file.get(),
                                     rep_->footer,
                                     read_options,
                                     handle.block_handle_,
                                     raw_block,
                                     rep_->ioptions,
                                     block_cache_compressed == nullptr,
                                     compression_dict,
                                     rep_->global_seqno,
                                     rep_->table_options.read_amp_bytes_per_bit,
                                     &handle.aio_handle_,
                                     nullptr /* allocator */).code())) {
          SE_LOG(WARN, "failed to read block from file", K(ret));
        }
        raw_block_ptr.reset(raw_block);
      }
      // put to cache
      if (SUCCED(ret)) {
        //if (rep_->internal_stats_ != nullptr) { // io of the subtable
          //rep_->internal_stats_->AddCFStats(InternalStats::BYTES_READ, raw_block->size());
        //}

        if (nullptr == add_blocks || *add_blocks < scan_add_blocks_limit) {
          if (FAILED(PutDataBlockToCache(key,
                                         ckey,
                                         block_cache,
                                         block_cache_compressed,
                                         read_options,
                                         rep_->ioptions,
                                         &handle.block_entry_,
                                         raw_block_ptr.release(),
                                         rep_->table_options.format_version,
                                         compression_dict,
                                         rep_->table_options.read_amp_bytes_per_bit,
                                         false /* is_index */,
                                         Cache::Priority::LOW).code())) {
            SE_LOG(WARN, "failed to put data block to cache", K(ret));
          } else if (nullptr != add_blocks) {
            ++(*add_blocks);
            handle.cache_ = rep_->table_options.block_cache.get();
            handle.need_do_cleanup_ = true;
          }
        } else {
          handle.block_entry_.value = raw_block_ptr.release();
          handle.need_do_cleanup_ = true;
        }
      }
    }
  }
  if (SUCCED(ret)) {
    if (IS_NULL(handle.block_entry_.value)) {
      ret = Status::kErrorUnexpected;
      SE_LOG(WARN, "block cache value is nullptr", K(ret));
    } else {
      handle.block_entry_.value->NewIterator(&rep_->internal_comparator,
                                             &data_block_iter,
                                             true,
                                             rep_->ioptions.statistics);
    }
  }
  return ret;
}

namespace {

void DeleteCachedIndexEntry(const Slice& key, void* value) {
  IndexReader* index_reader = reinterpret_cast<IndexReader*>(value);
  if (index_reader->statistics() != nullptr) {
    QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_INDEX_BYTES_EVICT,
                    index_reader->usable_size());
  }
//  delete index_reader;
  MOD_DELETE_OBJECT(IndexReader, index_reader);
}

}  // anonymous namespace

const std::string ExtentBasedTable::kFilterBlockPrefix = "filter.";
const std::string ExtentBasedTable::kFullFilterBlockPrefix = "fullfilter.";
const std::string ExtentBasedTable::kPartitionedFilterBlockPrefix =
    "partitionedfilter.";
}  // namespace table
}  // namespace smartengine
