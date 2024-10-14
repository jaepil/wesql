//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "util/file_name.h"
#include <inttypes.h>
#include <stdio.h>
#include "logger/log_module.h"
#include "schema/table_schema.h"
#include "util/se_constants.h"
#include "util/string_util.h"

namespace smartengine
{
using namespace common;

namespace util
{

const char *FileNameUtil::DATA_FILE_SUFFIX = "sst";
const char *FileNameUtil::WAL_FILE_SUFFIX = "wal";
const char *FileNameUtil::MANIFEST_FILE_SUFFIX = "manifest";
const char *FileNameUtil::CHECKPOINT_FILE_SUFFIX = "checkpoint";
const char *FileNameUtil::TEMP_FILE_SUFFIX = "tmp";
const char *FileNameUtil::CURRENT_FILE_NAME = "CURRENT";
const char *FileNameUtil::LOCK_FILE_NAME = "LOCK";

int FileNameUtil::parse_file_name(const std::string &file_name, int64_t &file_number, FileType &file_type)
{
  int ret = common::Status::kOk;
  Slice rest(file_name);
  uint64_t number = 0;

  if (UNLIKELY(file_name.empty())) {
    ret = common::Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(file_name));
  } else if ("." == file_name || ".." == file_name) {
    ret = Status::kNotSupported;
  } else if (Slice(CURRENT_FILE_NAME) == rest) {
    file_number = 0;
    file_type = kCurrentFile;
  } else if (Slice(LOCK_FILE_NAME) == rest) {
    file_number = 0;
    file_type = kLockFile;
  } else if (!ConsumeDecimalNumber(&rest, &number)) {
    ret = common::Status::kErrorUnexpected;
#ifndef NDEBUG
    SE_LOG(WARN, "fail to consume decimal number", K(ret), K(file_name), K(rest));
#endif
  } else if (rest.size() <= 1 || DELIMITER != rest[0]) {
    ret = common::Status::kErrorUnexpected;
    SE_LOG(WARN, "the file name format is not correct", K(ret), K(file_name), K(rest));
  } else {
    file_number = static_cast<int64_t>(number);
    rest.remove_prefix(1);
    if (Slice(WAL_FILE_SUFFIX) == rest) {
      file_type = kWalFile;
    } else if (Slice(DATA_FILE_SUFFIX) == rest) {
      file_type = kDataFile;
    } else if (Slice(MANIFEST_FILE_SUFFIX) == rest) {
      file_type = kManifestFile;
    } else if (Slice(CHECKPOINT_FILE_SUFFIX) == rest) {
      file_type = kCheckpointFile;
    } else if (Slice(TEMP_FILE_SUFFIX) == rest) {
      file_type = kTempFile;
    } else {
      ret = common::Status::kNotSupported;
      SE_LOG(WARN, "Unsupport file type", K(ret), K(file_name), K(rest));
    }
  }

  return ret;
}

std::string FileNameUtil::current_file_path(const std::string &dir)
{
  return dir + "/" + CURRENT_FILE_NAME; 
}

std::string FileNameUtil::lock_file_path(const std::string &dir)
{
  return dir + "/" + LOCK_FILE_NAME;
}

std::string FileNameUtil::wal_file_name(int64_t file_number)
{
  return file_name(file_number, WAL_FILE_SUFFIX);
}

std::string FileNameUtil::wal_file_path(const std::string &dir, int64_t file_number)
{
  return dir + "/" + wal_file_name(file_number);
}

std::string FileNameUtil::data_file_name(int64_t file_number)
{
  return file_name(file_number, DATA_FILE_SUFFIX);
}

std::string FileNameUtil::data_file_path(const std::string &dir, int64_t file_number)
{
  return dir + "/" + data_file_name(file_number);
}

std::string FileNameUtil::manifest_file_name(int64_t file_number)
{
  return file_name(file_number, MANIFEST_FILE_SUFFIX);
}

std::string FileNameUtil::manifest_file_path(const std::string &dir, int64_t file_number)
{
  return dir + "/" + manifest_file_name(file_number);
}

std::string FileNameUtil::checkpoint_file_name(int64_t file_number)
{
  return file_name(file_number, CHECKPOINT_FILE_SUFFIX);
}

std::string FileNameUtil::checkpoint_file_path(const std::string &dir, int64_t file_number)
{
  return dir + "/" + checkpoint_file_name(file_number);
}

std::string FileNameUtil::temp_file_name(int64_t file_number)
{
  return file_name(file_number, TEMP_FILE_SUFFIX);
}

std::string FileNameUtil::temp_file_path(const std::string &dir, int64_t file_number)
{
  return dir + "/" + temp_file_name(file_number);
}

std::string FileNameUtil::file_name(int64_t file_number, const char *suffix)
{
  assert(nullptr != suffix);
  char buf[storage::MAX_FILE_PATH_SIZE] = {0};
  snprintf(buf, sizeof(buf), "%ld%c%s", file_number, DELIMITER, suffix);
  return std::string(buf);
}

}  // namespace util
}  // namespace smartengine
