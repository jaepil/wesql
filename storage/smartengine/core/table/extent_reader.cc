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

#include "table/extent_reader.h"

#include "memory/base_malloc.h"
#include "storage/extent_meta_manager.h"
#include "storage/extent_space_manager.h"
#include "storage/io_extent.h"
#include "table/bloom_filter.h"
#include "table/column_block_iterator.h"
#include "table/get_context.h"
#include "table/index_block_reader.h"
#include "table/large_object.h"
#include "table/row_block_writer.h"

namespace smartengine
{
using namespace cache;
using namespace common;
using namespace db;
using namespace memory;
using namespace monitor;
using namespace storage;
using namespace util;

namespace table
{

DEFINE_TO_STRING(ExtentReaderArgs, KV_(extent_id), KV_(use_full_prefetch_extent), KVP_(internal_key_comparator), KVP_(block_cache))

ExtentReader::ExtentReader()
    : is_inited_(false),
      internal_key_comparator_(nullptr),
      use_full_prefetch_extent_(false),
      extent_(nullptr),
      extent_meta_(nullptr),
      index_block_(nullptr),
      block_cache_(nullptr),
      cache_entry_key_()
{}

ExtentReader::~ExtentReader()
{
  destroy();
}

int ExtentReader::init(const ExtentReaderArgs &args)
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "ExtentReader has been inited", K(ret));
  } else if (UNLIKELY(!args.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(args));
  } else if (FAILED(IS_NULL(extent_meta_ = ExtentMetaManager::get_instance().get_meta(args.extent_id_)))) {
    SE_LOG(WARN, "fail to get extent meta", K(ret), K(args));
  } else if (FAILED(init_extent(*extent_meta_, args.use_full_prefetch_extent_))) {
    SE_LOG(WARN, "fail to init extent", K(ret), K(args));
  } else if (FAILED(cache_entry_key_.setup(extent_))) {
    SE_LOG(WARN, "fail to setup cache key", K(ret), K(args));
  } else {
    internal_key_comparator_ = args.internal_key_comparator_;
    block_cache_ = args.block_cache_;
    is_inited_ = true;
  }

  return ret;
}

void ExtentReader::destroy()
{
  if (is_inited_) {
    MOD_DELETE_OBJECT(RowBlock, index_block_);
    if (use_full_prefetch_extent_) {
      FullPrefetchExtent *full_prefetch_extent = reinterpret_cast<FullPrefetchExtent *>(extent_);
      MOD_DELETE_OBJECT(FullPrefetchExtent, full_prefetch_extent);
    } else {
      MOD_DELETE_OBJECT(ReadableExtent, extent_);
    }
    extent_ = nullptr;
    is_inited_ = false;
  }
}

// TODO(Zhao Dongsheng): unused skip_filters
int ExtentReader::get(const Slice &key, GetContext *get_context)
{
  int ret = Status::kOk;
  bool done = false; // meaning the completion of get process, not mean that the key is found.
  bool may_exist = true;
  IndexBlockReader index_block_reader;
  BlockInfo block_info;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (UNLIKELY(key.empty()) || IS_NULL(get_context)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(key), KP(get_context));
  } else if (FAILED(index_block_reader.init(index_block_, internal_key_comparator_))) {
    SE_LOG(WARN, "fail to init index block reader", K(ret));
  } else {
    index_block_reader.seek(key);
    // TODO(Zhao Dongsheng): the loop can optimize.
    while (SUCCED(ret) && !done && index_block_reader.valid()) {
      block_info.reset();
      may_exist = true;
      if (FAILED(index_block_reader.get_value(block_info))) {
        SE_LOG(WARN, "fail to get block index", K(ret));
      } else if (FAILED(check_in_bloom_filter(get_context->get_user_key(), block_info, may_exist))) {
        SE_LOG(WARN, "fail to check in bloom filter", K(ret), K(block_info));
      } else {
        if (may_exist) {
          if (FAILED(inner_get(key, block_info, get_context, done))) {
            SE_LOG(WARN, "fail to get from data block", K(ret), K(block_info));
          }
        } else {
          QUERY_COUNT(CountPoint::BLOOM_FILTER_USEFUL);
        }

        if (SUCCED(ret)) {
          index_block_reader.next();
        }
      }
    }
  }

  return ret;
}

InternalIterator *ExtentReader::NewIterator(const common::ReadOptions &read_options,
                                            memory::SimpleAllocator *allocaor,
                                            bool skip_filters,
                                            const uint64_t scan_add_blocks_limit)
{
  abort();
  return nullptr;
}

int ExtentReader::prefetch_index_block(BlockDataHandle<RowBlock> &handle)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (IS_NULL(index_block_)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "index block must not be nullptr", K(ret), KP(index_block_));
  } else {
    // The lifetimes of ExtentReader and IndexBlock are consistent.
    // The index block is released when destroy ExtentReader.
    handle.block_entry_.value_ = index_block_;
    handle.need_do_cleanup_ = false;
  }

  return ret;
}

int ExtentReader::prefetch_data_block(BlockDataHandle<RowBlock> &data_block_handle)
{
  int ret = Status::kOk;
  char cache_key_buf[CacheEntryKey::MAX_CACHE_KEY_SZIE] = {0};
  Slice cache_key;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (IS_NULL(block_cache_)) {
   // distable block cache, the prefetching does nothing.
  } else if (FAILED(cache_entry_key_.generate(data_block_handle.block_info_.get_offset(),
                                              cache_key_buf,
                                              cache_key))) {
    SE_LOG(WARN, "fail to generate cache entry key", K(ret));
  } else if (IS_NULL(data_block_handle.block_entry_.handle_ = block_cache_->Lookup(cache_key))) {
    // Cache miss, the data block will be read by aio_handle.
    QUERY_COUNT(CountPoint::BLOCK_CACHE_MISS);
  } else {
    // Cache hit, the SSTableScanIterator is responsible for releasing the cache handle.
    data_block_handle.block_entry_.value_ = reinterpret_cast<RowBlock*>(
        block_cache_->Value(data_block_handle.block_entry_.handle_));
    data_block_handle.cache_ = block_cache_;
    data_block_handle.need_do_cleanup_ = true;
    QUERY_COUNT(CountPoint::BLOCK_CACHE_HIT);
  }

  return ret;
}

int ExtentReader::do_io_prefetch(const int64_t offset, const int64_t size, AIOHandle *aio_handle)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (UNLIKELY(offset < 0) || UNLIKELY(size <= 0) || IS_NULL(aio_handle)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(offset), K(size), KP(aio_handle));
  } else if (FAILED(extent_->prefetch(offset, size, aio_handle))) {
    SE_LOG(WARN, "fail to prefetch", K(ret), K(offset), K(size));
  } else {
    // succeed
  }

  return ret;
}

// Reading the persistent state of a block on disk returns a block consistent
// with the data stored on disk, including the block trailer, and has not
// undergone decompression. It's used by compaction when the block is reused.
int ExtentReader::read_persistent_data_block(const BlockHandle &handle, Slice &block)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (FAILED(BlockIOHelper::read_block(extent_,
                                              handle,
                                              nullptr /*aio_handle=*/,
                                              block))) {
    SE_LOG(WARN, "fail to read block", K(ret));
  } else {
    // succeed
  }

  return ret;
}

int ExtentReader::setup_index_block_iterator(RowBlockIterator *index_block_iterator)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (IS_NULL(index_block_iterator)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(index_block_iterator));
  } else if (use_full_prefetch_extent_ && IS_NULL(index_block_) &&
             FAILED(BlockIOHelper::read_and_uncompress_block(extent_,
                                                             extent_meta_->index_block_handle_,
                                                             ModId::kExtentReader,
                                                             nullptr /*aio_handle*/,
                                                             index_block_))) {
    SE_LOG(WARN, "fail to read index block", K(ret), K(*extent_meta_));
  } else if (FAILED(index_block_iterator->setup(internal_key_comparator_, index_block_, true /*is_index_block*/))) {
    SE_LOG(WARN, "fail to setup index block iterator", K(ret));
  } else {
    index_block_iterator->set_source(extent_meta_->extent_id_.id());
  }

  return ret;
}

int ExtentReader::setup_data_block_iterator(BlockDataHandle<RowBlock> &data_block_handle, RowBlockIterator *data_block_iterator)
{
  int ret = Status::kOk;
  Slice data_block_content;
  Slice row_block_content;
  RowBlock *row_block = nullptr;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (UNLIKELY(!data_block_handle.block_info_.is_valid()) || IS_NULL(data_block_iterator)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(data_block_handle.block_info_), KP(data_block_iterator));
  } else if (FAILED(BlockIOHelper::read_and_uncompress_block(extent_,
                                                             data_block_handle.block_info_.handle_,
                                                             ModId::kExtentReader,
                                                             nullptr /*aio_handle*/,
                                                             data_block_content))) {
    SE_LOG(WARN, "fail to read data block", K(ret), K(data_block_handle.block_info_));
  } else {
    if (!data_block_handle.block_info_.is_column_block()) {
      row_block_content = data_block_content;
    } else if (FAILED(convert_to_row_block(data_block_content, data_block_handle.block_info_, row_block_content))) {
      SE_LOG(WARN, "fail to convert to row block", K(ret));
    } else {
      // free the memory of column block
      base_free(const_cast<char *>(data_block_content.data()));
    }

    if (SUCCED(ret)) {
      if (IS_NULL(row_block = MOD_NEW_OBJECT(ModId::kBlock,
                                             RowBlock,
                                             row_block_content.data(),
                                             row_block_content.size(),
                                             common::kNoCompression))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for RowBlock", K(ret));
      } else if (FAILED(data_block_iterator->setup(internal_key_comparator_,
                                                   row_block,
                                                   false /*is_index_block*/))) {
        SE_LOG(WARN, "fail to setup block iterator", K(ret), K(row_block), K(data_block_handle.block_info_));
      } else {
        data_block_handle.block_entry_.value_ = row_block;
        data_block_handle.need_do_cleanup_ = true;
      }
    }
  }

  return ret;
}

int ExtentReader::setup_data_block_iterator(BlockDataHandle<RowBlock> &data_block_handle,
                                            const int64_t scan_add_blocks_limit,
                                            int64_t &added_blocks,
                                            RowBlockIterator &data_block_iterator)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (IS_NULL(data_block_handle.block_entry_.handle_) &&
             FAILED(read_data_block(data_block_handle, scan_add_blocks_limit, added_blocks))) { 
    SE_LOG(WARN, "fail to read data block", K(ret));
  } else if (IS_NULL(data_block_handle.block_entry_.value_)) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the data block handle must not be nullptr", K(ret));
  } else if (FAILED(data_block_iterator.setup(internal_key_comparator_,
                                              data_block_handle.block_entry_.value_,
                                              false /*is_index_block*/))) {
    SE_LOG(WARN, "fail to setup data block iterator", K(ret));
  }
  
  return ret;
}

int ExtentReader::check_block_in_cache(const BlockHandle &block_handle, bool &in_cache)
{
  int ret = Status::kOk;
  char cache_key_buf[CacheEntryKey::MAX_CACHE_KEY_SZIE] = {0};
  Slice cache_key;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (UNLIKELY(!block_handle.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(block_handle));
  } else if (IS_NULL(block_cache_)) {
    in_cache = false;
  } else if (FAILED(cache_entry_key_.generate(block_handle.offset_, cache_key_buf, cache_key))) {
    SE_LOG(WARN, "fail to generate cache entry key", K(ret), K(block_handle));
  } else {
    in_cache = block_cache_->check_in_cache(cache_key);
  }

  return ret;
}

int ExtentReader::approximate_key_offet(const Slice &key, int64_t &offset)
{
  int ret = Status::kOk;
  IndexBlockReader index_block_reader;
  BlockInfo block_info;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (FAILED(index_block_reader.init(index_block_, internal_key_comparator_))) {
    SE_LOG(WARN, "fail to init index block reader", K(ret));
  } else {
    index_block_reader.seek(key);
    if (index_block_reader.valid()) {
      if (FAILED(index_block_reader.get_value(block_info))) {
        SE_LOG(WARN, "fail to get block stats", K(ret));
      } else {
        offset = block_info.get_offset();
      }
    } else {
      // key is past the last key in the extent. Approximate the offset
      // by returning the offset of the index block (which is right near
      // the end of the extent).
      offset = extent_meta_->index_block_handle_.offset_;
    }
  }

  return ret;
}

int64_t ExtentReader::get_usable_size() const
{
  return sizeof(*this) + sizeof(*index_block_) + index_block_->size_;
}

int ExtentReader::init_extent(const ExtentMeta &extent_meta, bool use_full_prefetch_extent)
{
  int ret = Status::kOk;

  if (use_full_prefetch_extent) {
    FullPrefetchExtent *full_prefetch_extent = nullptr;
    if (IS_NULL(full_prefetch_extent = MOD_NEW_OBJECT(ModId::kExtentReader, FullPrefetchExtent))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for FullPrefetchExtent", K(ret));
    } else if (FAILED(ExtentSpaceManager::get_instance().get_readable_extent(extent_meta.extent_id_, full_prefetch_extent))) {
      SE_LOG(WARN, "fail to get FullPrefetchExtent", K(ret), K(extent_meta));
    } else if (FAILED(full_prefetch_extent->full_prefetch())) {
      SE_LOG(WARN, "fail to full prefetch extent", K(ret), K(extent_meta));
    } else {
      extent_ = full_prefetch_extent;
      use_full_prefetch_extent_ = true;
    }

    if (FAILED(ret)) {
      if (IS_NOTNULL(full_prefetch_extent)) {
        MOD_DELETE_OBJECT(FullPrefetchExtent, full_prefetch_extent);
      }
    }
  }

  // There are two scenarios where "use_full_prefetch_extent_" is false:
  // 1. The input parameter 'use_full_prefetch_extent' is false.
  // 2. The input parameter 'use_full_prefetch_extent' is true, but fail to build FullPrefetchExtent,
  //    fallback to use ReadableExtent and overwrite ret.
  if (!use_full_prefetch_extent_) {
    if (IS_NULL(extent_ = MOD_NEW_OBJECT(ModId::kExtentReader, ReadableExtent))) {
      ret = Status::kMemoryLimit;
      SE_LOG(WARN, "fail to allocate memory for ReadableExtent", K(ret));
    } else if (FAILED(ExtentSpaceManager::get_instance().get_readable_extent(extent_meta.extent_id_, extent_))) {
      SE_LOG(WARN, "fail to get readable extent", K(ret), K(extent_meta));
    } else if (FAILED(BlockIOHelper::read_and_uncompress_block(extent_,
                                                               extent_meta.index_block_handle_,
                                                               ModId::kIndexBlockReader,
                                                               nullptr /*aio_handle*/,
                                                               index_block_))) {
      SE_LOG(WARN, "fail to read index block", K(ret), K(extent_meta));
    }

    if (FAILED(ret)) {
      if (IS_NOTNULL(extent_)) {
        MOD_DELETE_OBJECT(ReadableExtent, extent_);
      }
      if (IS_NOTNULL(index_block_)) {
        MOD_DELETE_OBJECT(RowBlock, index_block_);
      }
    }
  }

  return ret;
}

int ExtentReader::check_in_bloom_filter(const Slice &user_key, const BlockInfo &block_info, bool &may_exist)
{
  int ret = Status::kOk;
  BloomFilterReader bloom_filter_reader;

  if (UNLIKELY(user_key.empty()) || UNLIKELY(!block_info.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(user_key), K(block_info));
  } else if (FAILED(bloom_filter_reader.init(block_info.bloom_filter_, block_info.probe_num_))) {
    SE_LOG(WARN, "fail to init bloom filter reader", K(ret), K(block_info));
  } else if (FAILED(bloom_filter_reader.check(user_key, may_exist))) {
    SE_LOG(WARN, "fail to check through bloom filter reader", K(ret), K(user_key));
  }

  return ret;
}

int ExtentReader::inner_get(const Slice &key,
                            const BlockInfo &block_info,
                            GetContext *get_context,
                            bool &done)
{
  int ret = Status::kOk;
  int64_t added_blocks = 0;
  RowBlockIterator data_block_iterator;
  BlockDataHandle<RowBlock> data_block_handle;
  data_block_handle.extent_id_ = extent_meta_->extent_id_;
  data_block_handle.block_info_ = block_info;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "ExtentReader should be inited", K(ret));
  } else if (UNLIKELY(!block_info.is_valid()) || IS_NULL(get_context)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(block_info), KP(get_context));
  } else if (FAILED(setup_data_block_iterator(data_block_handle,
                                              INT64_MAX,
                                              added_blocks,
                                              data_block_iterator))) {
    SE_LOG(WARN, "fail to setup data block iterator", K(ret));
  } else {
    data_block_iterator.Seek(key);
    while (SUCCED(ret) && !done && data_block_iterator.Valid()) {
      db::ParsedInternalKey parsed_key;
      if (!db::ParseInternalKey(data_block_iterator.key(), &parsed_key)) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "fail to parse internal key", K(ret));
      } else if (!get_context->SaveValue(parsed_key,
                                         data_block_iterator.value(),
                                         &data_block_iterator)) {
        done = true;
        if (GetContext::kFound == get_context->State() && (kTypeValueLarge == parsed_key.type)) {
          LargeValue large_value;
          Slice large_result;
          if (FAILED(large_value.convert_to_normal_format(data_block_iterator.value(), large_result))) {
            SE_LOG(WARN, "fail to covert large value to normal format", K(ret));
          } else {
            get_context->SaveLargeValue(large_result);
          }
        }
      } else {
        data_block_iterator.Next();
      }
    }
  }

  // Release the data block cache handle.
  data_block_handle.reset();

  return ret;
}

int ExtentReader::read_data_block(BlockDataHandle<RowBlock> &data_block_handle,
                                  const int64_t scan_add_blocks_limit,
                                  int64_t &added_blocks)
{
  int ret = Status::kOk;
  char cache_key_buf[CacheEntryKey::MAX_CACHE_KEY_SZIE] = {0};
  Slice cache_key;
  Slice data_block;
  Slice row_block;

  if (UNLIKELY(!data_block_handle.block_info_.is_valid())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), "block_info", data_block_handle.block_info_);
  } else if (FAILED(cache_entry_key_.generate(data_block_handle.block_info_.get_offset(),
                                              cache_key_buf,
                                              cache_key))) {
    SE_LOG(WARN, "fail to generate cache entry key", K(ret));
  } else if (data_block_handle.has_prefetched_ || IS_NULL(block_cache_) ||
             (IS_NULL(data_block_handle.block_entry_.handle_ = block_cache_->Lookup(cache_key)))) {
    // If aio has already been initiated for this block previously,
    // then here it is necessary to first retrieve the aio result.
    assert(IS_NULL(data_block_handle.block_entry_.handle_));
    if (!data_block_handle.has_prefetched_ && IS_NOTNULL(block_cache_)) {
      // overall cache miss
      QUERY_COUNT(CountPoint::BLOCK_CACHE_MISS);
    }

    if (FAILED(BlockIOHelper::read_and_uncompress_block(extent_,
                                                        data_block_handle.block_info_.handle_,
                                                        ModId::kExtentReader,
                                                        data_block_handle.has_prefetched_ ? &(data_block_handle.aio_handle_) : nullptr,
                                                        data_block))) {
      SE_LOG(WARN, "fail to read and uncompress data block", K(ret));
    } else {
      if (!data_block_handle.block_info_.is_column_block()) {
        row_block = data_block;
      } else if (FAILED(convert_to_row_block(data_block, data_block_handle.block_info_, row_block))) {
        SE_LOG(WARN, "fail to covert column block to row block", K(ret));
      } else {
        // free the memory of column block
        base_free(const_cast<char *>(data_block.data()));
      }

      if (SUCCED(ret)) {
        if (IS_NULL(data_block_handle.block_entry_.value_ = MOD_NEW_OBJECT(ModId::kBlock,
                                                                                           RowBlock, 
                                                                                           row_block.data(),
                                                                                           row_block.size(),
                                                                                           kNoCompression))) {
          ret = Status::kMemoryLimit;
          SE_LOG(WARN, "fail to allocate memory for Block", K(ret));
        } else if (IS_NOTNULL(block_cache_) && added_blocks < scan_add_blocks_limit) {
          // TODO(Zhao Dongsheng): this function really work?
          update_mod_id(data_block_handle.block_entry_.value_->data_, ModId::kDataBlockCache);  
          if (FAILED(block_cache_->Insert(cache_key,
                                          data_block_handle.block_entry_.value_,
                                          data_block_handle.block_entry_.value_->usable_size(),
                                          &CacheEntryHelper::delete_entry<RowBlock>,
                                          &(data_block_handle.block_entry_.handle_),
                                          Cache::Priority::LOW).code())) {
            SE_LOG(WARN, "fail to insert into block cache", K(ret));
          } else {
            // The data_block_handle should be responsible to release block cache handle.
            ++added_blocks;
            data_block_handle.cache_ = block_cache_;
            data_block_handle.need_do_cleanup_ = true;
            QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD);
            QUERY_COUNT(CountPoint::BLOCK_CACHE_DATA_ADD);
          }
        } else {
          // The data_block_handle should be responsible to release the memory of block.
          data_block_handle.need_do_cleanup_ = true;
        }
      }
    }
  } else if (IS_NULL(data_block_handle.block_entry_.value_ = reinterpret_cast<RowBlock *>(
      block_cache_->Value(data_block_handle.block_entry_.handle_)))) {
    ret = Status::kErrorUnexpected;
    SE_LOG(WARN, "the block in cache must not be nullptr", K(ret));
  } else {
    // The data_block_handle should be responsible to release block cache handle.
    data_block_handle.cache_ = block_cache_;
    data_block_handle.need_do_cleanup_ = true;
  }

  return ret;
}

int ExtentReader::convert_to_row_block(const Slice &column_block, const BlockInfo &block_info, Slice &row_block)
{
  int ret = Status::kOk;
  RowBlockWriter row_block_writer;
  ColumnBlockIterator column_block_iterator;
  Slice key;
  Slice value;
  Slice tmp_row_block;
  char *row_block_buf = nullptr;
  BlockInfo dummy_block_info;

  if (UNLIKELY(column_block.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(column_block));
  // TODO(Zhao Dongsheng): the restart_interval should use the configed value.
  } else if (FAILED(row_block_writer.init(16 /*restart_interval*/))) {
    SE_LOG(WARN, "fail to init RowBlockIterator", K(ret));
  } else if (FAILED(column_block_iterator.init(column_block, block_info, extent_meta_->table_schema_))) {
    SE_LOG(WARN, "fail to init ColumnBlockIterator", K(ret));
  } else {
    while (SUCCED(ret)) {
      key.clear();
      value.clear();
      if (FAILED(column_block_iterator.get_next_row(key, value))) {
        if (Status::kIterEnd != ret) {
          SE_LOG(WARN, "fail to get next row", K(ret));
        } else {
          /**Overwrite the ret if reach to end of iterator.*/
          ret = Status::kOk;
          break;
        }
      } else if (FAILED(row_block_writer.append(key, value))) {
        SE_LOG(WARN, "fail to append row to row block", K(ret));
      }
    }

    if (SUCCED(ret)) {
      if (FAILED(row_block_writer.build(tmp_row_block, dummy_block_info))) {
        SE_LOG(WARN, "fail to build row block", K(ret));
      } else if (IS_NULL(row_block_buf = reinterpret_cast<char *>(base_malloc(tmp_row_block.size(), ModId::kBlock)))) {
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for row block buf", K(ret), "size", tmp_row_block.size());
      } else {
        memcpy(row_block_buf, tmp_row_block.data(), tmp_row_block.size());
        row_block.assign(row_block_buf, tmp_row_block.size());
      }
    }
  }
  
  return ret;
}

} // namespace table
} // namespace smartengine