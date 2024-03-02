//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/snapshot_impl.h"
#include "db/db.h"
#include "logger/log_module.h"
#include "storage/multi_version_extent_meta_layer.h"

namespace smartengine {
namespace db {

SnapshotImpl::SnapshotImpl()
    : number_(0),
      ref_(0)
{
  for (int64_t level = 0; level < storage::MAX_TIER_COUNT; ++level) {
    extent_layer_versions_[level] = nullptr;
  }
}

SnapshotImpl::~SnapshotImpl()
{
}
int SnapshotImpl::init(storage::ExtentLayerVersion **extent_layer_versions, common::SequenceNumber seq_num)
{
  int ret = common::Status::kOk;

  if (IS_NULL(extent_layer_versions)) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(extent_layer_versions));
  } else {
    number_ = seq_num;
    for (int64_t level = 0; level < storage::MAX_TIER_COUNT; ++level) {
      extent_layer_versions_[level] = extent_layer_versions[level];
      extent_layer_versions_[level]->ref();
    }
  }

  return ret;
}

void SnapshotImpl::destroy(util::autovector<storage::ExtentLayerVersion *> &recyle_extent_layer_versions)
{
  for (int64_t level = 0; level < storage::MAX_TIER_COUNT; ++level) {
    if (extent_layer_versions_[level]->unref()) {
      recyle_extent_layer_versions.push_back(extent_layer_versions_[level]);
    }
  }
}

int64_t SnapshotImpl::get_total_extent_count() const
{
  int64_t total_extent_count = 0;
  for (int64_t level = 0; level < storage::MAX_TIER_COUNT; ++level) {
    total_extent_count += extent_layer_versions_[level]->get_total_extent_size();
  }
  return total_extent_count;
}

ManagedSnapshot::ManagedSnapshot(DB* db)
    : db_(db), snapshot_(db->GetSnapshot()) {}

ManagedSnapshot::ManagedSnapshot(DB* db, const Snapshot* _snapshot)
    : db_(db), snapshot_(_snapshot) {}

ManagedSnapshot::~ManagedSnapshot() {
  if (snapshot_) {
    db_->ReleaseSnapshot(snapshot_);
  }
}

const Snapshot* ManagedSnapshot::snapshot() { return snapshot_; }
}  // namespace db
}  // namespace smartengine
