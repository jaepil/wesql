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

#pragma  once

#include "storage/storage_meta_struct.h"
#include "table/sstable_scan_struct.h"

namespace smartengine
{
namespace storage
{
struct ExtentMeta;
class ReadableExtent;
} // namespace storage

namespace table
{
class GetContext;
class RowBlockIterator;

struct ExtentReaderArgs
{
  storage::ExtentId extent_id_;
  bool use_full_prefetch_extent_;
  const db::InternalKeyComparator *internal_key_comparator_;
  cache::Cache *block_cache_;

  ExtentReaderArgs(const storage::ExtentId &extent_id,
                   bool use_full_prefetch_extent,
                   const db::InternalKeyComparator *internal_key_comparator,
                   cache::Cache *block_cache)
      : extent_id_(extent_id),
        use_full_prefetch_extent_(use_full_prefetch_extent),
        internal_key_comparator_(internal_key_comparator),
        block_cache_(block_cache)
  {}

  bool is_valid() const { return IS_NOTNULL(internal_key_comparator_); }
  DECLARE_TO_STRING()
};

class ExtentReader
{
public:
  ExtentReader();
  ExtentReader(const ExtentReader &) = delete;
  ExtentReader(const ExtentReader &&) = delete;
  ExtentReader &operator=(const ExtentReader &) = delete;
  ExtentReader &operator=(const ExtentReader &&) = delete;
  ~ExtentReader();

  int init(const ExtentReaderArgs &args);
  void destroy();
  int get(const common::Slice &key, GetContext *get_context);
  InternalIterator *NewIterator(const common::ReadOptions &read_options,
                                memory::SimpleAllocator *allocaor,
                                bool skip_filters,
                                const uint64_t scan_add_blocks_limit);
  int prefetch_index_block(BlockDataHandle<RowBlock> &handle);
  int prefetch_data_block(BlockDataHandle<RowBlock> &handle);
  int do_io_prefetch(const int64_t offset, const int64_t size, util::AIOHandle *aio_handle);
  int read_persistent_data_block(const BlockHandle &handle, common::Slice &block);
  int setup_index_block_iterator(RowBlockIterator *index_block_iterator);
  // Setup data block iterator whose data block is read bypass data block cache.
  int setup_data_block_iterator(BlockDataHandle<RowBlock> &data_block_handle, RowBlockIterator *data_block_iterator);
  // Setup data block iterator whose data block is read through data block cache.
  int setup_data_block_iterator(BlockDataHandle<RowBlock> &data_block_handle,
                                const int64_t scan_add_blocks_limit,
                                int64_t &added_blocks,
                                RowBlockIterator &data_block_iterator);
  int check_block_in_cache(const BlockHandle &handle, bool &in_cache);
  int approximate_key_offet(const common::Slice &key, int64_t &offset);
  int64_t get_data_size() const { return extent_meta_->data_size_; }
  int64_t get_row_count() const { return extent_meta_->num_entries_; }
  int64_t get_usable_size() const;

private:
  int init_extent(const storage::ExtentMeta &extent_meta, bool use_full_prefetch_extent);
  int inner_get(const common::Slice &key,
                const BlockInfo &block_info,
                GetContext *get_context,
                bool &found);
  int read_data_block(BlockDataHandle<RowBlock> &data_block_handle,
                      const int64_t scan_add_blocks_limit,
                      int64_t &added_blocks);

private:
  bool is_inited_;
  const db::InternalKeyComparator *internal_key_comparator_;
  bool use_full_prefetch_extent_;
  storage::ReadableExtent *extent_;
  storage::ExtentMeta *extent_meta_;
  RowBlock *index_block_;
  cache::Cache *block_cache_;
  CacheEntryKey cache_entry_key_;
};

} // namespace table
} // namespace smartengine