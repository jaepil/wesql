/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#ifndef ROCKSDB_LITE
#include <string>
#include <vector>

#include "smartengine/db.h"
#include "smartengine/utilities/db_ttl.h"
#include "smartengine/utilities/stackable_db.h"

namespace smartengine {
namespace util {

// Please don't use this class. It's deprecated
class UtilityDB {
 public:
// This function is here only for backwards compatibility. Please use the
// functions defined in DBWithTTl (smartengine/utilities/db_ttl.h)
// (deprecated)
#if defined(__GNUC__) || defined(__clang__)
  __attribute__((deprecated))
#elif _WIN32
  __declspec(deprecated)
#endif
  static common::Status
  OpenTtlDB(const common::Options& options, const std::string& name,
            StackableDB** dbptr, int32_t ttl = 0, bool read_only = false);
};

}  // namespace util
}  // namespace smartengine
#endif  // ROCKSDB_LITE
