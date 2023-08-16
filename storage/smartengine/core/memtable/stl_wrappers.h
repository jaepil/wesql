//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <map>
#include <string>

#include "util/coding.h"
#include "util/murmurhash.h"
#include "smartengine/comparator.h"
#include "smartengine/memtablerep.h"
#include "smartengine/slice.h"

namespace smartengine {
namespace memtable {
namespace stl_wrappers {

class Base {
 protected:
  const MemTableRep::KeyComparator& compare_;
  explicit Base(const MemTableRep::KeyComparator& compare)
      : compare_(compare) {}
};

struct Compare : private Base {
  explicit Compare(const MemTableRep::KeyComparator& compare) : Base(compare) {}
  inline bool operator()(const char* a, const char* b) const {
    return compare_(a, b) < 0;
  }
};
}
}
}
