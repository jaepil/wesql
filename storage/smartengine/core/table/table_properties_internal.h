//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include "util/status.h"

namespace smartengine {
namespace table {
class InternalIterator;

// Seek to the properties block.
// If it successfully seeks to the properties block, "is_found" will be
// set to true.
common::Status SeekToPropertiesBlock(InternalIterator* meta_iter,
                                     bool* is_found);

// Seek to the compression dictionary block.
// If it successfully seeks to the properties block, "is_found" will be
// set to true.
common::Status SeekToCompressionDictBlock(InternalIterator* meta_iter,
                                          bool* is_found);

}  // namespace table
}  // namespace smartengine
