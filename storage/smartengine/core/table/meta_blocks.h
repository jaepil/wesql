//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "db/builder.h"
#include "options/options.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "util/kv_map.h"

namespace smartengine {
namespace util {
class Logger;
}

namespace table {

class BlockBuilder;
class BlockHandle;
class Footer;
struct TableProperties;
class InternalIterator;

class MetaIndexBuilder {
 public:
  MetaIndexBuilder(const MetaIndexBuilder&) = delete;
  MetaIndexBuilder& operator=(const MetaIndexBuilder&) = delete;

  MetaIndexBuilder();
  void Add(const std::string& key, const BlockHandle& handle);

  // Write all the added key/value pairs to the block and return the contents
  // of the block.
  common::Slice Finish();

 private:
  // store the sorted key/handle of the metablocks.
  util::stl_wrappers::KVMap meta_block_handles_;
  std::unique_ptr<BlockBuilder, memory::ptr_destruct_delete<BlockBuilder>> meta_index_block_;
};

class PropertyBlockBuilder {
 public:
  PropertyBlockBuilder(const PropertyBlockBuilder&) = delete;
  PropertyBlockBuilder& operator=(const PropertyBlockBuilder&) = delete;

  PropertyBlockBuilder();

  void AddTableProperty(const TableProperties& props);
  void Add(const std::string& key, uint64_t value);
  void Add(const std::string& key, const std::string& value);
  void Add(const UserCollectedProperties& user_collected_properties);

  // Write all the added entries to the block and return the block contents
  common::Slice Finish();

 private:
  std::unique_ptr<BlockBuilder, memory::ptr_destruct_delete<BlockBuilder>> properties_block_;
  util::stl_wrappers::KVMap props_;
};

// Read the properties from the table.
// @returns a status to indicate if the operation succeeded. On success,
//          *table_properties will point to a heap-allocated TableProperties
//          object, otherwise value of `table_properties` will not be modified.
common::Status ReadProperties(const common::Slice& handle_value,
                              util::RandomAccessFileReader* file,
                              const Footer& footer,
                              const common::ImmutableCFOptions& ioptions,
                              TableProperties** table_properties);

// Directly read the properties from the properties block of a plain table.
// @returns a status to indicate if the operation succeeded. On success,
//          *table_properties will point to a heap-allocated TableProperties
//          object, otherwise value of `table_properties` will not be modified.
common::Status ReadTableProperties(util::RandomAccessFileReader* file,
                                   uint64_t file_size,
                                   uint64_t table_magic_number,
                                   const common::ImmutableCFOptions& ioptions,
                                   TableProperties** properties);

// Find the meta block from the meta index block.
common::Status FindMetaBlock(InternalIterator* meta_index_iter,
                             const std::string& meta_block_name,
                             BlockHandle* block_handle);

// Find the meta block
common::Status FindMetaBlock(util::RandomAccessFileReader* file,
                             uint64_t file_size, uint64_t table_magic_number,
                             const common::ImmutableCFOptions& ioptions,
                             const std::string& meta_block_name,
                             BlockHandle* block_handle);

// Read the specified meta block with name meta_block_name
// from `file` and initialize `contents` with contents of this block.
// Return common::Status::OK in case of success.
common::Status ReadMetaBlock(util::RandomAccessFileReader* file,
                             uint64_t file_size, uint64_t table_magic_number,
                             const common::ImmutableCFOptions& ioptions,
                             const std::string& meta_block_name,
                             BlockContents* contents);

}  // namespace table
}  // namespace smartengine
