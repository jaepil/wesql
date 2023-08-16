/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// A checkpoint is an openable snapshot of a database at a point in time.

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <functional>
#include "smartengine/status.h"

namespace smartengine {

namespace db {
class DB;
}

namespace util {

class Checkpoint {
 public:
  // Creates a Checkpoint object to be used for creating openable sbapshots
  static common::Status Create(db::DB* db, Checkpoint** checkpoint_ptr);

  // Builds an openable snapshot of RocksDB on the same disk, which
  // accepts an output directory on the same disk, and under the directory
  // (1) hard-linked SST files pointing to existing live SST files
  // SST files will be copied if output directory is on a different filesystem
  // (2) a copied manifest files and other files
  // The directory should not already exist and will be created by this API.
  // The directory will be an absolute path
  // log_size_for_flush: if the total log file size is equal or larger than
  // this value, then a flush is triggered for all the column families. The
  // default value is 0, which means flush is always triggered. If you move
  // away from the default, the checkpoint may not contain up-to-date data
  // if WAL writing is not always enabled.
  // Flush will always trigger if it is 2PC.
  virtual common::Status CreateCheckpoint(const std::string& checkpoint_dir,
                                          uint64_t log_size_for_flush = 0);

  // do manual checkpoint for backup
  virtual int manual_checkpoint(const std::string &checkpoint_dir,
                                int32_t phase = 1,
                                int dest_fd = -1);

  virtual ~Checkpoint() {}
};

}  // namespace util
}  // namespace smartengine
#endif  // !ROCKSDB_LITE
