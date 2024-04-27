/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "db/version_set.h"
#include "options/cf_options.h"
#include "storage/extent_meta_manager.h"
#include "storage/extent_space_manager.h"
#include "storage/io_extent.h"
#include "storage/storage_logger.h"
#include "table/extent_table_reader.h"
#include "table/get_context.h"
#include "table/table_builder.h"
#include "util/aio_wrapper.h"
#include "util/testharness.h"

static const std::string test_dir = smartengine::util::test::TmpDir() + "/in_mem_extent_table_test";

namespace smartengine
{
using namespace common;
using namespace db;
using namespace storage;
using namespace util;

namespace table
{

static std::string GetFromFile(TableReader* table_reader,
                               const std::string& key, ReadOptions& ro,
                               const Comparator* comparator) {
  PinnableSlice value;
  GetContext get_context(comparator,
                         GetContext::kNotFound,
                         Slice(key),
                         &value,
                         nullptr,
                         nullptr);
  LookupKey lk{key, kMaxSequenceNumber};
  table_reader->Get(ro, lk.internal_key(), &get_context);
  return std::string(value.data(), value.size());
}

static void get_data_block(TableReader* table_reader, const Slice& key,
                           unique_ptr<char[]>& block, size_t& size) {
  auto reader = dynamic_cast<ExtentBasedTable*>(table_reader);
  BlockIter iiter_on_stack;
  ExtentBasedTable::IndexReader* index_reader = nullptr;
  memory::ArenaAllocator alloc;
  auto iiter = reader->create_index_iterator(ReadOptions(), &iiter_on_stack,
                                             index_reader, alloc);
  std::unique_ptr<InternalIterator> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
    iiter_unique_ptr = std::unique_ptr<InternalIterator>(iiter);
  }
  EXPECT_TRUE(iiter->status().ok());
  iiter->Seek(key);
  EXPECT_TRUE(iiter->Valid());

  BlockHandle handle;
  Slice input = iiter->value();
  Status s = handle.DecodeFrom(&input);
  EXPECT_TRUE(s.ok());

  size = handle.size() + kBlockTrailerSize;
  block.reset(new char[size]);
  Slice data_block(block.get(), size);
  int ret = reader->get_data_block(handle, data_block, true);
  EXPECT_EQ(ret, Status::kOk);
  if (block.get() != data_block.data()) {
    memcpy(block.get(), data_block.data(), size);
  }
}

TEST(InMemExtent, sim) {
  // dirty preparation
  storage::ChangeInfo change_info;
  MiniTables mtables;
  mtables.change_info_ = &change_info;
  unique_ptr<TableBuilder> builder;
  std::string column_family_name;
  storage::LayerPosition output_layer_position;
  BlockBasedTableOptions table_options;
  Options options;
  options.table_factory.reset(NewExtentBasedTableFactory(table_options));
  const ImmutableCFOptions ioptions(options);
  const InternalKeyComparator internal_comparator(options.comparator);
  EnvOptions soptions;
  ReadOptions ro;
  Env* env = Env::Default();
  std::string dbname = test_dir;
  Status s;
  EXPECT_TRUE(s.ok()) << s.ToString();
  FileNumber file_number(2000);
  options.db_paths.emplace_back(dbname, 0);
  ImmutableDBOptions doption;
  VersionSet *vs = nullptr;
  vs = new VersionSet(dbname, &doption, soptions, nullptr, nullptr);

  StorageLogger::get_instance().init(env, dbname, soptions, doption, vs, 1 * 1024 * 1024 * 1024);
  ExtentMetaManager::get_instance().init();
  ExtentSpaceManager::get_instance().init(env, soptions, options);
  int ret = Status::kOk;
  ret = ExtentSpaceManager::get_instance().create_table_space(0);
  ASSERT_EQ(Status::kOk, ret);
  mtables.table_space_id_ = 0;
  StorageLogger::get_instance().begin(storage::SeEvent::FLUSH);
  builder.reset(ioptions.table_factory->NewTableBuilderExt(
      TableBuilderOptions(
          ioptions, internal_comparator,
          kNoCompression, CompressionOptions(),
          nullptr /* compression_dict */, false /* skip_filters */,
          column_family_name, output_layer_position, false /**is_flush*/),
      0,
      &mtables));

  // create an table/extent with one record
  InternalKey key("key", 0, kTypeValue);
  std::string value("val");
  builder->Add(key.Encode().ToString(), value);
  s = builder->Finish();
  EXPECT_TRUE(s.ok()) << s.ToString();

  ExtentId eid(mtables.metas[0].extent_id_);

  unique_ptr<char[]> block1;
  size_t size1;
  {
    // method 1: read it normally
    ReadableExtent *extent = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, ReadableExtent);
    s = ExtentSpaceManager::get_instance().get_readable_extent(eid, extent);
    EXPECT_TRUE(s.ok()) << s.ToString();

    TableReader *table_reader = nullptr;
    TableReaderOptions reader_options(ioptions,
                                      internal_comparator,
                                      eid,
                                      false,
                                      -1);
    s = ioptions.table_factory->NewTableReader(reader_options,
                                               extent,
                                               MAX_EXTENT_SIZE,
                                               table_reader);
    EXPECT_TRUE(s.ok()) << s.ToString();

    // verify
    std::string v =
        GetFromFile(table_reader, "key", ro, options.comparator);
    ASSERT_EQ(v, value);
    v = GetFromFile(table_reader, "ke", ro, options.comparator);
    ASSERT_EQ(v.size(), 0);  // not exist
    v = GetFromFile(table_reader, "keyx", ro, options.comparator);
    ASSERT_EQ(v.size(), 0);  // not exist

    get_data_block(table_reader, key.Encode(), block1, size1);
  }

  unique_ptr<char[]> block2;
  size_t size2;
  {
    // method 2: read it using the new added mem interface
    const int size = MAX_EXTENT_SIZE;
    FullPrefetchExtent *extent = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, FullPrefetchExtent);
    s = ExtentSpaceManager::get_instance().get_readable_extent(eid, extent);
    EXPECT_TRUE(s.ok()) << s.ToString();

    // async read it
    extent->full_prefetch();

    // the mem interface
    TableReader *in_mem_table_reader = nullptr;
    TableReaderOptions reader_options(ioptions, internal_comparator, eid, false, -1);
    s = ioptions.table_factory->NewTableReader(reader_options,
                                               extent,
                                               MAX_EXTENT_SIZE,
                                               in_mem_table_reader);
    EXPECT_TRUE(s.ok()) << s.ToString();

    // verify
    std::string v =
        GetFromFile(in_mem_table_reader, "key", ro, options.comparator);
    ASSERT_EQ(v, value);
    v = GetFromFile(in_mem_table_reader, "ke", ro, options.comparator);
    ASSERT_EQ(v.size(), 0);  // not exist
    v = GetFromFile(in_mem_table_reader, "keyx", ro, options.comparator);
    ASSERT_EQ(v.size(), 0);  // not exist

    get_data_block(in_mem_table_reader, key.Encode(), block2, size2);
  }
  ASSERT_EQ(size1, size2);
  ASSERT_EQ(memcmp(block1.get(), block2.get(), size1), 0);
  delete vs;
  StorageLogger::get_instance().destroy();
}

}  // table
}  // smartengine

int main(int argc, char** argv) {
  std::string log_path = smartengine::util::test::TmpDir() + "/in_mem_extent_table_test.log";
  smartengine::logger::Logger::get_log().init(log_path.c_str(), smartengine::logger::DEBUG_LEVEL);
  smartengine::util::test::remove_dir(test_dir.c_str());
  smartengine::util::Env::Default()->CreateDir(test_dir);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
