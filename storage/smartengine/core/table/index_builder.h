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

#pragma once

#include <assert.h>
#include <inttypes.h>

#include <list>
#include <string>
#include <unordered_map>

#include "table/block_builder.h"
#include "table/format.h"

namespace smartengine {
namespace util {
class WritableBuffer;
}

namespace table {

// The interface for building index.
// Instruction for adding a new concrete IndexBuilder:
//  1. Create a subclass instantiated from IndexBuilder.
//  2. Add a new entry associated with that subclass in TableOptions::IndexType.
//  3. Add a create function for the new subclass in CreateIndexBuilder.
// Note: we can devise more advanced design to simplify the process for adding
// new subclass, which will, on the other hand, increase the code complexity and
// catch unwanted attention from readers. Given that we won't add/change
// indexes frequently, it makes sense to just embrace a more straightforward
// design that just works.
class IndexBuilder {
 public:
  static IndexBuilder* CreateIndexBuilder(
      BlockBasedTableOptions::IndexType index_type,
      const db::InternalKeyComparator* comparator,
      const BlockBasedTableOptions& table_opt,
      util::WritableBuffer* buf = nullptr);

  // Index builder will construct a set of blocks which contain:
  //  1. One primary index block.
  //  2. (Optional) a set of metablocks that contains the metadata of the
  //     primary index.
  struct IndexBlocks {
    common::Slice index_block_contents;
    std::unordered_map<std::string, common::Slice> meta_blocks;
  };
  explicit IndexBuilder(const db::InternalKeyComparator* comparator)
      : comparator_(comparator) {}

  virtual ~IndexBuilder() {}

  // Add a new index entry to index block.
  // To allow further optimization, we provide `last_key_in_current_block` and
  // `first_key_in_next_block`, based on which the specific implementation can
  // determine the best index key to be used for the index block.
  // @last_key_in_current_block: this parameter maybe overridden with the value
  //                             "substitute key".
  // @first_key_in_next_block: it will be nullptr if the entry being added is
  //                           the last one in the table
  //
  // REQUIRES: Finish() has not yet been called.
  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const common::Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) = 0;

  virtual bool SupportAddBlock() const { return false; }

  // Add a new index entry to index block.
  // Write block_stats to the value part.
  //
  // REQUIRES: Finish() has not yet been called.
  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const common::Slice* first_key_in_next_block,
                             const BlockHandle& block_handle,
                             const db::BlockStats& block_stats) {}

  // Add a new index entry for an optimized block.
  // @mid_key: store in the key part of the index entry
  // @block_stats: the stats of this block, should be stored in the value part
  //
  // REQUIRES: Finish() has not yet been called.
  virtual void AddIndexEntry(const common::Slice* mid_key,
                             const BlockHandle& block_handle,
                             const db::BlockStats& block_stats) {}

  // This method will be called whenever a key is added. The subclasses may
  // override OnKeyAdded() if they need to collect additional information.
  virtual void OnKeyAdded(const common::Slice& key) {}

  // Inform the index builder that all entries has been written. Block builder
  // may therefore perform any operation required for block finalization.
  //
  // REQUIRES: Finish() has not yet been called.
  inline common::Status Finish(IndexBlocks* index_blocks) {
    // Throw away the changes to last_partition_block_handle. It has no effect
    // on the first call to Finish anyway.
    BlockHandle last_partition_block_handle;
    return Finish(index_blocks, last_partition_block_handle);
  }

  // This override of Finish can be utilized to build the 2nd level index in
  // PartitionIndexBuilder.
  //
  // index_blocks will be filled with the resulting index data. If the return
  // value is common::Status::InComplete() then it means that the index is
  // partitioned
  // and the callee should keep calling Finish until common::Status::OK() is
  // returned.
  // In that case, last_partition_block_handle is pointer to the block written
  // with the result of the last call to Finish. This can be utilized to build
  // the second level index pointing to each block of partitioned indexes. The
  // last call to Finish() that returns common::Status::OK() populates
  // index_blocks with
  // the 2nd level index content.
  virtual common::Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) = 0;

  // Get the estimated size for index block.
  virtual size_t EstimatedSize() const = 0;
  virtual size_t EstimatedSizeAfter(const common::Slice& key,
                                    const db::BlockStats& block_stats) const {
    return EstimatedSize();
  }

 protected:
  const db::InternalKeyComparator* comparator_;
};

// This index builder builds space-efficient index block.
//
// Optimizations:
//  1. Made block's `block_restart_interval` to be 1, which will avoid linear
//     search when doing index lookup (can be disabled by setting
//     index_block_restart_interval).
//  2. Shorten the key length for index block. Other than honestly using the
//     last key in the data block as the index key, we instead find a shortest
//     substitute key that serves the same function.
class ShortenedIndexBuilder : public IndexBuilder {
 public:
  explicit ShortenedIndexBuilder(const db::InternalKeyComparator* comparator,
                                 int index_block_restart_interval,
                                 util::WritableBuffer* buf = nullptr)
      : IndexBuilder(comparator),
        index_block_builder_(index_block_restart_interval, true, buf) {}

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const common::Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override {
    if (first_key_in_next_block != nullptr) {
      comparator_->FindShortestSeparator(last_key_in_current_block,
                                         *first_key_in_next_block);
    } else {
      // comparator_->FindShortSuccessor(last_key_in_current_block);
    }

    std::string handle_encoding;
    block_handle.EncodeTo(&handle_encoding);
    index_block_builder_.Add(*last_key_in_current_block, handle_encoding);
  }

  virtual bool SupportAddBlock() const override { return true; }

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const common::Slice* first_key_in_next_block,
                             const BlockHandle& block_handle,
                             const db::BlockStats& block_stats) override {
    /*
#ifdef NDEBUG
    if (first_key_in_next_block != nullptr) {
      comparator_->FindShortestSeparator(last_key_in_current_block,
                                         *first_key_in_next_block);
    }
    else {
      comparator_->FindShortSuccessor(last_key_in_current_block);
    }
#endif
    */
    std::string handle_encoding;
    block_handle.EncodeTo(&handle_encoding);
    handle_encoding.append(block_stats.encode());
    index_block_builder_.Add(*last_key_in_current_block, handle_encoding);
  }

  virtual void AddIndexEntry(const common::Slice* mid_key,
                             const BlockHandle& block_handle,
                             const db::BlockStats& block_stats) override {
    std::string handle_encoding;
    block_handle.EncodeTo(&handle_encoding);
    handle_encoding.append(block_stats.encode());
    index_block_builder_.Add(*mid_key, handle_encoding);
  }

  using IndexBuilder::Finish;
  virtual common::Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override {
    index_blocks->index_block_contents = index_block_builder_.Finish();
    return common::Status::OK();
  }

  virtual size_t EstimatedSize() const override {
    return index_block_builder_.CurrentSizeEstimate();
  }

  virtual size_t EstimatedSizeAfter(const common::Slice& key,
                                    const db::BlockStats& block_stats) const override {
    size_t val_size = 2 * 10;  // max size of variable encoding for blockhandle
    val_size += block_stats.estimate_size();
    return index_block_builder_.EstimateSizeAfterKV(key.size(), val_size);
  }

  friend class PartitionedIndexBuilder;

 private:
  BlockBuilder index_block_builder_;
};


}  // namespace table
}  // namespace smartengine
