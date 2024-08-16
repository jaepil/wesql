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

#include "util/filename.h"

#include "db/dbformat.h"
#include "port/port.h"
#include "util/testharness.h"

using namespace smartengine;
using namespace common;
using namespace util;
using namespace table;
using namespace cache;
using namespace memtable;
using namespace monitor;
using namespace storage;
namespace smartengine {
namespace db {

class FileNameTest : public testing::Test {};

TEST_F(FileNameTest, Parse) {
  Slice db;
  FileType type;
  uint64_t number;

  char kDefautInfoLogDir = 1;
  char kDifferentInfoLogDir = 2;
  char kNoCheckLogDir = 4;
  char kAllMode = kDefautInfoLogDir | kDifferentInfoLogDir | kNoCheckLogDir;

  // Successful parses
  static struct {
    const char* fname;
    uint64_t number;
    FileType type;
    char mode;
  } cases[] = {
      {"100.wal", 100, kLogFile, kAllMode},
      {"0.wal", 0, kLogFile, kAllMode},
      {"0.sst", 0, kTableFile, kAllMode},
      {"CURRENT", 0, kCurrentFile, kAllMode},
      {"LOCK", 0, kDBLockFile, kAllMode},
      {"MANIFEST-2", 2, kManifestFile, kAllMode},
      {"MANIFEST-7", 7, kManifestFile, kAllMode},
      {"METADB-2", 2, kMetaDatabase, kAllMode},
      {"METADB-7", 7, kMetaDatabase, kAllMode},
      {"18446744073709551615.wal", 18446744073709551615ull, kLogFile, kAllMode},
  };
  for (char mode : {kDifferentInfoLogDir, kDefautInfoLogDir, kNoCheckLogDir}) {
    for (unsigned int i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
      if (cases[i].mode & mode) {
        std::string f = cases[i].fname;
        if (mode == kNoCheckLogDir) {
          ASSERT_TRUE(ParseFileName(f, &number, &type)) << f;
        } else {
          ASSERT_TRUE(ParseFileName(f, &number, &type))
              << f;
        }
        ASSERT_EQ(cases[i].type, type) << f;
        ASSERT_EQ(cases[i].number, number) << f;
      }
    }
  }

  // Errors
  static const char* errors[] = {"",
                                 "foo",
                                 "foo-dx-100.log",
                                 ".log",
                                 "",
                                 "manifest",
                                 "CURREN",
                                 "CURRENTX",
                                 "MANIFES",
                                 "MANIFEST",
                                 "MANIFEST-",
                                 "XMANIFEST-3",
                                 "MANIFEST-3x",
                                 "META",
                                 "METADB",
                                 "METADB-",
                                 "XMETADB-3",
                                 "METADB-3x",
                                 "LOC",
                                 "LOCKx",
                                 "LO",
                                 "LOGx",
                                 "18446744073709551616.log",
                                 "184467440737095516150.log",
                                 "100",
                                 "100.",
                                 "100.lop"};
  for (unsigned int i = 0; i < sizeof(errors) / sizeof(errors[0]); i++) {
    std::string f = errors[i];
    ASSERT_TRUE(!ParseFileName(f, &number, &type)) << f;
  };
}

TEST_F(FileNameTest, Construction) {
  uint64_t number;
  FileType type;
  std::string fname;

  fname = CurrentFileName("foo");
  ASSERT_EQ("foo/", std::string(fname.data(), 4));
  ASSERT_TRUE(ParseFileName(fname.c_str() + 4, &number, &type));
  ASSERT_EQ(0U, number);
  ASSERT_EQ(kCurrentFile, type);

  fname = LockFileName("foo");
  ASSERT_EQ("foo/", std::string(fname.data(), 4));
  ASSERT_TRUE(ParseFileName(fname.c_str() + 4, &number, &type));
  ASSERT_EQ(0U, number);
  ASSERT_EQ(kDBLockFile, type);

  fname = LogFileName("foo", 192);
  ASSERT_EQ("foo/", std::string(fname.data(), 4));
  ASSERT_TRUE(ParseFileName(fname.c_str() + 4, &number, &type));
  ASSERT_EQ(192U, number);
  ASSERT_EQ(kLogFile, type);

  fname = TableFileName({DbPath("bar", 0)}, 200, 0);
  std::string fname1 =
      TableFileName({DbPath("foo", 0), DbPath("bar", 0)}, 200, 1);
  ASSERT_EQ(fname, fname1);
  ASSERT_EQ("bar/", std::string(fname.data(), 4));
  ASSERT_TRUE(ParseFileName(fname.c_str() + 4, &number, &type));
  ASSERT_EQ(200U, number);
  ASSERT_EQ(kTableFile, type);

  fname = ManifestFileName("bar", 100);
  ASSERT_EQ("bar/", std::string(fname.data(), 4));
  ASSERT_TRUE(ParseFileName(fname.c_str() + 4, &number, &type));
  ASSERT_EQ(100U, number);
  ASSERT_EQ(kManifestFile, type);

  fname = TempFileName("tmp", 999);
  ASSERT_EQ("tmp/", std::string(fname.data(), 4));
  ASSERT_TRUE(ParseFileName(fname.c_str() + 4, &number, &type));
  ASSERT_EQ(999U, number);
  ASSERT_EQ(kTempFile, type);

  fname = MetaDatabaseName("met", 100);
  ASSERT_EQ("met/", std::string(fname.data(), 4));
  ASSERT_TRUE(ParseFileName(fname.c_str() + 4, &number, &type));
  ASSERT_EQ(100U, number);
  ASSERT_EQ(kMetaDatabase, type);
}
}
}  // namespace smartengine

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
	smartengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
