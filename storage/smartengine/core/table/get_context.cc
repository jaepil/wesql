//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/get_context.h"
#include "db/pinned_iterators_manager.h"
#include "monitoring/statistics.h"
#include "smartengine/env.h"
#include "smartengine/statistics.h"

using namespace smartengine;
using namespace common;
using namespace util;
using namespace db;
using namespace monitor;

namespace smartengine {
namespace table {

GetContext::GetContext(const Comparator* ucmp,
                       GetState init_state,
                       const Slice& user_key,
                       PinnableSlice* pinnable_val,
                       bool* value_found,
                       RangeDelAggregator* _range_del_agg,
                       Env* env,
                       SequenceNumber* seq)
    : ucmp_(ucmp),
      state_(init_state),
      user_key_(user_key),
      pinnable_val_(pinnable_val),
      value_found_(value_found),
      range_del_agg_(_range_del_agg),
      env_(env),
      seq_(seq)
{
  if (seq_) {
    *seq_ = kMaxSequenceNumber;
  }
}

// Called from TableCache::Get and Table::Get when file/block in which
// key may exist are not there in TableCache/BlockCache respectively. In this
// case we can't guarantee that key does not exist and are not permitted to do
// IO to be certain.Set the status=kFound and value_found=false to let the
// caller know that key may exist but is not there in memory
void GetContext::MarkKeyMayExist() {
  state_ = kFound;
  if (value_found_ != nullptr) {
    *value_found_ = false;
  }
}

void GetContext::SaveLargeValue(const Slice& value) {
  assert(state_ == kFound);

  if (LIKELY(pinnable_val_ != nullptr)) {
    pinnable_val_->Reset();
    pinnable_val_->PinSelf(value);
  }
}

bool GetContext::SaveValue(const ParsedInternalKey& parsed_key,
                           const Slice& value, Cleanable* value_pinner) {
  se_assert(kTypeMerge != parsed_key.type);
  if (ucmp_->Equal(parsed_key.user_key, user_key_)) {

    if (seq_ != nullptr) {
      // Set the sequence number if it is uninitialized
      if (*seq_ == kMaxSequenceNumber) {
        *seq_ = parsed_key.sequence;
      }
    }
    auto type = parsed_key.type;
    // Key matches. Process it
    if (type == kTypeValue &&
        range_del_agg_ != nullptr && range_del_agg_->ShouldDelete(parsed_key)) {
      type = kTypeRangeDeletion;
    }
    se_assert(kNotFound == state_);
    switch (type) {
      case kTypeValue:
      case kTypeValueLarge:
        state_ = kFound;
        if (LIKELY(pinnable_val_ != nullptr)) {
          if (LIKELY(value_pinner != nullptr)) {
            // If the backing resources for the value are provided, pin them
            pinnable_val_->PinSlice(value, value_pinner);
          } else {
            // Otherwise copy the value
            pinnable_val_->PinSelf(value);
          }
        }
        return false;

      case kTypeDeletion:
      case kTypeSingleDeletion:
      case kTypeRangeDeletion:
        // TODO(noetzli): Verify correctness once merge of single-deletes
        // is supported
        state_ = kDeleted;
        return false;
      default:
        se_assert(false);
        break;
    }
  }

  // state_ could be Corrupt, merge or notfound
  return false;
}

}  // namespace table
}  // namespace smartengine
