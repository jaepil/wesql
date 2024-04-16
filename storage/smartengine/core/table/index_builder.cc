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

#include "table/index_builder.h"

#include <assert.h>

// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace smartengine {
using namespace db;
using namespace util;
using namespace common;

namespace table {
// Create a index builder based on its type.
IndexBuilder* IndexBuilder::CreateIndexBuilder(const InternalKeyComparator* comparator,
                                               const BlockBasedTableOptions& table_opt,
                                                WritableBuffer* buf)
{
  return MOD_NEW_OBJECT(memory::ModId::kDefaultMod,
                        ShortenedIndexBuilder,
                        comparator,
                        table_opt.index_block_restart_interval,
                        buf);
}

}  // namespace table
}  // namespace smartengine
