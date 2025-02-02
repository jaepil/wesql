// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <mutex>

#include "db/column_family.h"
#include "monitoring/thread_status_updater.h"

using namespace smartengine;
using namespace common;
using namespace db;
using namespace util;

namespace smartengine {
namespace util {

#ifndef NDEBUG
#ifdef ROCKSDB_USING_THREAD_STATUS
void ThreadStatusUpdater::TEST_VerifyColumnFamilyInfoMap(
    const std::vector<db::ColumnFamilyHandle*>& handles, bool check_exist) {
  std::unique_lock<std::mutex> lock(thread_list_mutex_);
  if (check_exist) {
    assert(cf_info_map_.size() == handles.size());
  }
  for (auto* handle : handles) {
    auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(handle)->cfd();
    auto iter __attribute__((unused)) = cf_info_map_.find(cfd);
    if (check_exist) {
      assert(iter != cf_info_map_.end());
      assert(iter->second);
    } else {
      assert(iter == cf_info_map_.end());
    }
  }
}

#else

void ThreadStatusUpdater::TEST_VerifyColumnFamilyInfoMap(
    const std::vector<db::ColumnFamilyHandle*>& handles, bool check_exist) {}

#endif  // ROCKSDB_USING_THREAD_STATUS
#endif  // !NDEBUG

}  // namespace util
}  // namespace smartengine
