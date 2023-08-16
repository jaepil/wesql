//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include <string>

#include "smartengine/env.h"

namespace smartengine {
namespace util {

// Returns an Env that translates paths such that the root directory appears to
// be chroot_dir. chroot_dir should refer to an existing directory.
Env* NewChrootEnv(Env* base_env, const std::string& chroot_dir);

}  // namespace util
}  // namespace smartengine

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
