//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/db_test_util.h"
#include "monitoring/perf_level.h"
#include "monitoring/query_perf_context.h"
#include "options/options.h"
#include "table/iterator_wrapper.h"
#include "table/merging_iterator.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"


namespace smartengine {
using namespace common;
using namespace util;
using namespace table;
using namespace cache;
using namespace memtable;
using namespace monitor;
using namespace storage;
using namespace memory;

namespace db {
class TestIterator : public InternalIterator {
 public:
  explicit TestIterator(const Comparator* comparator)
      : initialized_(false),
        valid_(false),
        sequence_number_(0),
        iter_(0),
        cmp(comparator) {}

  void AddPut(std::string argkey, std::string argvalue) {
    Add(argkey, kTypeValue, argvalue);
  }

  void AddDeletion(std::string argkey) {
    Add(argkey, kTypeDeletion, std::string());
  }

  void AddSingleDeletion(std::string argkey) {
    Add(argkey, kTypeSingleDeletion, std::string());
  }


  void Add(std::string argkey, ValueType type, std::string argvalue) {
    Add(argkey, type, argvalue, sequence_number_++);
  }

  void Add(std::string argkey, ValueType type, std::string argvalue,
           size_t seq_num, bool update_iter = false) {
    valid_ = true;
    ParsedInternalKey internal_key(argkey, seq_num, type);
    data_.push_back(
        std::pair<std::string, std::string>(std::string(), argvalue));
    AppendInternalKey(&data_.back().first, internal_key);
    if (update_iter && valid_ && cmp.Compare(data_.back().first, key()) < 0) {
      // insert a key smaller than current key
      Finish();
      // data_[iter_] is not anymore the current element of the iterator.
      // Increment it to reposition it to the right position.
      iter_++;
    }
  }

  // should be called before operations with iterator
  void Finish() {
    initialized_ = true;
    std::sort(data_.begin(), data_.end(),
              [this](std::pair<std::string, std::string> a,
                     std::pair<std::string, std::string> b) {
                return (cmp.Compare(a.first, b.first) < 0);
              });
  }

  virtual bool Valid() const override {
    assert(initialized_);
    return valid_;
  }

  virtual void SeekToFirst() override {
    assert(initialized_);
    valid_ = (data_.size() > 0);
    iter_ = 0;
  }

  virtual void SeekToLast() override {
    assert(initialized_);
    valid_ = (data_.size() > 0);
    iter_ = data_.size() - 1;
  }

  virtual void Seek(const Slice& target) override {
    assert(initialized_);
    SeekToFirst();
    if (!valid_) {
      return;
    }
    while (iter_ < data_.size() &&
           (cmp.Compare(data_[iter_].first, target) < 0)) {
      ++iter_;
    }

    if (iter_ == data_.size()) {
      valid_ = false;
    }
  }

  virtual void SeekForPrev(const Slice& target) override {
    assert(initialized_);
    SeekForPrevImpl(target, &cmp);
  }

  virtual void Next() override {
    assert(initialized_);
    if (data_.empty() || (iter_ == data_.size() - 1)) {
      valid_ = false;
    } else {
      ++iter_;
    }
  }

  virtual void Prev() override {
    assert(initialized_);
    if (iter_ == 0) {
      valid_ = false;
    } else {
      --iter_;
    }
  }

  virtual Slice key() const override {
    assert(initialized_);
    return data_[iter_].first;
  }

  virtual Slice value() const override {
    assert(initialized_);
    return data_[iter_].second;
  }

  virtual Status status() const override {
    assert(initialized_);
    return Status::OK();
  }

  virtual bool IsKeyPinned() const override { return true; }
  virtual bool IsValuePinned() const override { return true; }

 private:
  bool initialized_;
  bool valid_;
  size_t sequence_number_;
  size_t iter_;

  InternalKeyComparator cmp;
  std::vector<std::pair<std::string, std::string>> data_;
};

class DBIteratorTest : public testing::Test {
 public:
  Env* env_;

  DBIteratorTest() : env_(Env::Default()) {
    smartengine::monitor::QueryPerfContext::opt_enable_count_ = true;
  }
};

TEST_F(DBIteratorTest, DBIteratorPrevNext) {
  Options options;
  ImmutableCFOptions cf_options = ImmutableCFOptions(options);

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddDeletion("a");
    internal_iter->AddDeletion("a");
    internal_iter->AddDeletion("a");
    internal_iter->AddDeletion("a");
    internal_iter->AddPut("a", "val_a");

    internal_iter->AddPut("b", "val_b");
    internal_iter->Finish();

    ReadOptions ro;
    std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "val_b");

    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "val_a");

    db_iter->Next();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "val_b");

    db_iter->Next();
    ASSERT_TRUE(!db_iter->Valid());
  }
  // Test to check the SeekToLast() with iterate_upper_bound not set
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("b", "val_b");
    internal_iter->AddPut("b", "val_b");
    internal_iter->AddPut("c", "val_c");
    internal_iter->Finish();

    ReadOptions ro;
    std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "c");
  }

  // Test to check the SeekToLast() with iterate_upper_bound set
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());

    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("b", "val_b");
    internal_iter->AddPut("c", "val_c");
    internal_iter->AddPut("d", "val_d");
    internal_iter->AddPut("e", "val_e");
    internal_iter->AddPut("f", "val_f");
    internal_iter->Finish();

    Slice prefix("d");

    ReadOptions ro;
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "c");

    db_iter->Next();
    ASSERT_TRUE(!db_iter->Valid());

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "c");
  }
  // Test to check the SeekToLast() iterate_upper_bound set to a key that
  // is not Put yet
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());

    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("b", "val_b");
    internal_iter->AddPut("c", "val_c");
    internal_iter->AddPut("d", "val_d");
    internal_iter->Finish();

    Slice prefix("z");

    ReadOptions ro;
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "d");

    db_iter->Next();
    ASSERT_TRUE(!db_iter->Valid());

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "d");

    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "c");
  }
  // Test to check the SeekToLast() with iterate_upper_bound set to the
  // first key
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("b", "val_b");
    internal_iter->AddPut("b", "val_b");
    internal_iter->Finish();

    Slice prefix("a");

    ReadOptions ro;
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));

    db_iter->SeekToLast();
    ASSERT_TRUE(!db_iter->Valid());
  }
  // Test case to check SeekToLast with iterate_upper_bound set
  // (same key put may times - SeekToLast should start with the
  // maximum sequence id of the upper bound)

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("b", "val_b");
    internal_iter->AddPut("c", "val_c");
    internal_iter->AddPut("c", "val_c");
    internal_iter->AddPut("c", "val_c");
    internal_iter->AddPut("c", "val_c");
    internal_iter->AddPut("c", "val_c");
    internal_iter->AddPut("c", "val_c");
    internal_iter->AddPut("c", "val_c");
    internal_iter->Finish();

    Slice prefix("c");

    ReadOptions ro;
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      7, 100));

    SetPerfLevel(kEnableCount);
    ASSERT_TRUE(GetPerfLevel() == kEnableCount);

    QUERY_TRACE_RESET();
    db_iter->SeekToLast();
    auto perf_ctx = get_tls_query_perf_context();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(perf_ctx->get_count(CountPoint::INTERNAL_KEY_SKIPPED), 7);
    ASSERT_EQ(db_iter->key().ToString(), "b");

    SetPerfLevel(kDisable);
  }
  // Test to check the SeekToLast() with the iterate_upper_bound set
  // (Checking the value of the key which has sequence ids greater than
  // and less that the iterator's sequence id)
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());

    internal_iter->AddPut("a", "val_a1");
    internal_iter->AddPut("a", "val_a2");
    internal_iter->AddPut("b", "val_b1");
    internal_iter->AddPut("c", "val_c1");
    internal_iter->AddPut("c", "val_c2");
    internal_iter->AddPut("c", "val_c3");
    internal_iter->AddPut("b", "val_b2");
    internal_iter->AddPut("d", "val_d1");
    internal_iter->Finish();

    Slice prefix("c");

    ReadOptions ro;
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      4, 100));

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "val_b1");
  }

  // Test to check the SeekToLast() with the iterate_upper_bound set to the
  // key that is deleted
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddDeletion("a");
    internal_iter->AddPut("b", "val_b");
    internal_iter->AddPut("c", "val_c");
    internal_iter->Finish();

    Slice prefix("a");

    ReadOptions ro;
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));

    db_iter->SeekToLast();
    ASSERT_TRUE(!db_iter->Valid());
  }
  // Test to check the SeekToLast() with the iterate_upper_bound set
  // (Deletion cases)
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("b", "val_b");
    internal_iter->AddDeletion("b");
    internal_iter->AddPut("c", "val_c");
    internal_iter->Finish();

    Slice prefix("c");

    ReadOptions ro;
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");

    db_iter->Next();
    ASSERT_TRUE(!db_iter->Valid());

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
  }
  // Test to check the SeekToLast() with iterate_upper_bound set
  // (Deletion cases - Lot of internal keys after the upper_bound
  // is deleted)
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("b", "val_b");
    internal_iter->AddDeletion("c");
    internal_iter->AddDeletion("d");
    internal_iter->AddDeletion("e");
    internal_iter->AddDeletion("f");
    internal_iter->AddDeletion("g");
    internal_iter->AddDeletion("h");
    internal_iter->Finish();

    Slice prefix("c");

    ReadOptions ro;
    ro.iterate_upper_bound = &prefix;

    std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      7, 100));

    SetPerfLevel(kEnableCount);
    ASSERT_TRUE(GetPerfLevel() == kEnableCount);

    QUERY_TRACE_RESET();
    db_iter->SeekToLast();
    auto perf_ctx = get_tls_query_perf_context();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(perf_ctx->get_count(CountPoint::INTERNAL_DEL_SKIPPED), 1);
    ASSERT_EQ(db_iter->key().ToString(), "b");

    SetPerfLevel(kDisable);
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddDeletion("a");
    internal_iter->AddDeletion("a");
    internal_iter->AddDeletion("a");
    internal_iter->AddDeletion("a");
    internal_iter->AddPut("a", "val_a");

    internal_iter->AddPut("b", "val_b");
    internal_iter->Finish();

    ReadOptions ro;
    std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));

    db_iter->SeekToFirst();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "val_a");

    db_iter->Next();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "val_b");

    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "val_a");

    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("b", "val_b");

    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("b", "val_b");

    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("b", "val_b");

    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("b", "val_b");

    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("b", "val_b");
    internal_iter->Finish();

    ReadOptions ro;
    std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      2, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "val_b");

    db_iter->Next();
    ASSERT_TRUE(!db_iter->Valid());

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "val_b");
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("a", "val_a");

    internal_iter->AddPut("b", "val_b");

    internal_iter->AddPut("c", "val_c");
    internal_iter->Finish();

    ReadOptions ro;
    std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "c");
    ASSERT_EQ(db_iter->value().ToString(), "val_c");

    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "val_b");

    db_iter->Next();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "c");
    ASSERT_EQ(db_iter->value().ToString(), "val_c");
  }
}

TEST_F(DBIteratorTest, DBIteratorEmpty) {
  Options options;
  ImmutableCFOptions cf_options = ImmutableCFOptions(options);
  ReadOptions ro;

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->Finish();

    std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      0, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->Finish();

    std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      0, 100));
    db_iter->SeekToFirst();
    ASSERT_TRUE(!db_iter->Valid());
  }
}

TEST_F(DBIteratorTest, DBIteratorUseSkipCountSkips) {
  ReadOptions ro;
  Options options;
  options.statistics = CreateDBStatistics();

  TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
  for (size_t i = 0; i < 200; ++i) {
    internal_iter->AddPut("a", "a");
    internal_iter->AddPut("b", "b");
    internal_iter->AddPut("c", "c");
  }
  internal_iter->Finish();

  QUERY_TRACE_RESET();
  std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
      env_, ro, ImmutableCFOptions(options), BytewiseComparator(),
      internal_iter, 2, 100));
  db_iter->SeekToLast();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "c");
  ASSERT_EQ(db_iter->value().ToString(), "c");
  ASSERT_EQ(TestGetThreadCount(CountPoint::NUMBER_OF_RESEEKS_IN_ITERATION), 1u);

  db_iter->Prev();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "b");
  ASSERT_EQ(db_iter->value().ToString(), "b");
  ASSERT_EQ(TestGetThreadCount(CountPoint::NUMBER_OF_RESEEKS_IN_ITERATION), 2u);

  db_iter->Prev();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "a");
  ASSERT_EQ(db_iter->value().ToString(), "a");
  ASSERT_EQ(TestGetThreadCount(CountPoint::NUMBER_OF_RESEEKS_IN_ITERATION), 3u);

  db_iter->Prev();
  ASSERT_TRUE(!db_iter->Valid());
  ASSERT_EQ(TestGetThreadCount(CountPoint::NUMBER_OF_RESEEKS_IN_ITERATION), 3u);
}

TEST_F(DBIteratorTest, DBIteratorUseSkip) {
  ReadOptions ro;
  Options options;
  ImmutableCFOptions cf_options = ImmutableCFOptions(options);

  {
    for (size_t i = 0; i < 200; ++i) {
      TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
      for (size_t k = 0; k < 200; ++k) {
        internal_iter->AddPut("c", ToString(k));
      }
      internal_iter->Finish();

      options.statistics = CreateDBStatistics();
      std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
          env_, ro, cf_options, BytewiseComparator(), internal_iter, i,
          100));
      db_iter->SeekToLast();
      ASSERT_TRUE(db_iter->Valid());

      ASSERT_EQ(db_iter->key().ToString(), "c");
      ASSERT_EQ(db_iter->value().ToString(), ToString(i));
      db_iter->Prev();
      ASSERT_TRUE(!db_iter->Valid());
    }
  }

  {
    for (size_t i = 0; i < 200; ++i) {
      TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
      internal_iter->AddPut("b", "merge_1");
      internal_iter->AddPut("a", "merge_2");
      for (size_t k = 0; k < 200; ++k) {
        internal_iter->AddDeletion("c");
      }
      internal_iter->AddPut("c", "200");
      internal_iter->Finish();

      std::unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
          env_, ro, cf_options, BytewiseComparator(), internal_iter, i + 2,
          100));
      db_iter->SeekToLast();
      ASSERT_TRUE(db_iter->Valid());

      ASSERT_EQ(db_iter->key().ToString(), "b");
      ASSERT_EQ(db_iter->value().ToString(), "merge_1");
      db_iter->Prev();
      ASSERT_TRUE(db_iter->Valid());

      ASSERT_EQ(db_iter->key().ToString(), "a");
      ASSERT_EQ(db_iter->value().ToString(), "merge_2");
      db_iter->Prev();

      ASSERT_TRUE(!db_iter->Valid());
    }

    {
      TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
      internal_iter->AddPut("b", "merge_1");
      internal_iter->AddPut("a", "merge_2");
      for (size_t i = 0; i < 200; ++i) {
        internal_iter->AddDeletion("c");
      }
      internal_iter->AddPut("c", "200");
      internal_iter->Finish();

      unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
          env_, ro, cf_options, BytewiseComparator(), internal_iter, 202,
          100));
      db_iter->SeekToLast();
      ASSERT_TRUE(db_iter->Valid());

      ASSERT_EQ(db_iter->key().ToString(), "c");
      ASSERT_EQ(db_iter->value().ToString(), "200");
      db_iter->Prev();
      ASSERT_TRUE(db_iter->Valid());

      ASSERT_EQ(db_iter->key().ToString(), "b");
      ASSERT_EQ(db_iter->value().ToString(), "merge_1");
      db_iter->Prev();
      ASSERT_TRUE(db_iter->Valid());

      ASSERT_EQ(db_iter->key().ToString(), "a");
      ASSERT_EQ(db_iter->value().ToString(), "merge_2");
      db_iter->Prev();

      ASSERT_TRUE(!db_iter->Valid());
    }
  }

  {
    for (size_t i = 0; i < 200; ++i) {
      TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
      for (size_t k = 0; k < 200; ++k) {
        internal_iter->AddDeletion("c");
      }
      internal_iter->AddPut("c", "200");
      internal_iter->Finish();
      unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
          env_, ro, cf_options, BytewiseComparator(), internal_iter, i,
          100));
      db_iter->SeekToLast();
      ASSERT_TRUE(!db_iter->Valid());

      db_iter->SeekToFirst();
      ASSERT_TRUE(!db_iter->Valid());
    }

    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    for (size_t i = 0; i < 200; ++i) {
      internal_iter->AddDeletion("c");
    }
    internal_iter->AddPut("c", "200");
    internal_iter->Finish();
    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      200, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "c");
    ASSERT_EQ(db_iter->value().ToString(), "200");

    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());

    db_iter->SeekToFirst();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "c");
    ASSERT_EQ(db_iter->value().ToString(), "200");

    db_iter->Next();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    for (size_t i = 0; i < 200; ++i) {
      TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
      internal_iter->AddPut("b", "merge_1");
      internal_iter->AddPut("a", "merge_2");
      for (size_t k = 0; k < 200; ++k) {
        internal_iter->AddPut("d", ToString(k));
      }

      for (size_t k = 0; k < 200; ++k) {
        internal_iter->AddPut("c", ToString(k));
      }
      internal_iter->Finish();

      unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
          env_, ro, cf_options, BytewiseComparator(), internal_iter, i + 2,
          100));
      db_iter->SeekToLast();
      ASSERT_TRUE(db_iter->Valid());

      ASSERT_EQ(db_iter->key().ToString(), "d");
      ASSERT_EQ(db_iter->value().ToString(), ToString(i));
      db_iter->Prev();
      ASSERT_TRUE(db_iter->Valid());

      ASSERT_EQ(db_iter->key().ToString(), "b");
      ASSERT_EQ(db_iter->value().ToString(), "merge_1");
      db_iter->Prev();
      ASSERT_TRUE(db_iter->Valid());

      ASSERT_EQ(db_iter->key().ToString(), "a");
      ASSERT_EQ(db_iter->value().ToString(), "merge_2");
      db_iter->Prev();

      ASSERT_TRUE(!db_iter->Valid());
    }
  }
}

TEST_F(DBIteratorTest, DBIteratorSkipInternalKeys) {
  Options options;
  ImmutableCFOptions cf_options = ImmutableCFOptions(options);
  ReadOptions ro;

  // Basic test case ... Make sure explicityly passing the default value works.
  // Skipping internal keys is disabled by default, when the value is 0.
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddDeletion("b");
    internal_iter->AddDeletion("b");
    internal_iter->AddPut("c", "val_c");
    internal_iter->AddPut("c", "val_c");
    internal_iter->AddDeletion("c");
    internal_iter->AddPut("d", "val_d");
    internal_iter->Finish();

    ro.max_skippable_internal_keys = 0;
    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));

    db_iter->SeekToFirst();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "val_a");

    db_iter->Next();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "d");
    ASSERT_EQ(db_iter->value().ToString(), "val_d");

    db_iter->Next();
    ASSERT_TRUE(!db_iter->Valid());
    ASSERT_TRUE(db_iter->status().ok());

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "d");
    ASSERT_EQ(db_iter->value().ToString(), "val_d");

    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "val_a");

    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
    ASSERT_TRUE(db_iter->status().ok());
  }

  // Test to make sure that the request will *not* fail as incomplete if
  // num_internal_keys_skipped is *equal* to max_skippable_internal_keys
  // threshold. (It will fail as incomplete only when the threshold is
  // exceeded.)
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddDeletion("b");
    internal_iter->AddDeletion("b");
    internal_iter->AddPut("c", "val_c");
    internal_iter->Finish();

    ro.max_skippable_internal_keys = 2;
    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));

    db_iter->SeekToFirst();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "val_a");

    db_iter->Next();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "c");
    ASSERT_EQ(db_iter->value().ToString(), "val_c");

    db_iter->Next();
    ASSERT_TRUE(!db_iter->Valid());
    ASSERT_TRUE(db_iter->status().ok());

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "c");
    ASSERT_EQ(db_iter->value().ToString(), "val_c");

    db_iter->Prev();
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "val_a");

    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
    ASSERT_TRUE(db_iter->status().ok());
  }

  // Fail the request as incomplete when num_internal_keys_skipped >
  // max_skippable_internal_keys
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddDeletion("b");
    internal_iter->AddDeletion("b");
    internal_iter->AddDeletion("b");
    internal_iter->AddPut("c", "val_c");
    internal_iter->Finish();

    ro.max_skippable_internal_keys = 2;
    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));

    db_iter->SeekToFirst();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "val_a");

    db_iter->Next();
    ASSERT_TRUE(!db_iter->Valid());
    ASSERT_TRUE(db_iter->status().IsIncomplete());

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "c");
    ASSERT_EQ(db_iter->value().ToString(), "val_c");

    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
    ASSERT_TRUE(db_iter->status().IsIncomplete());
  }

  // Test that the num_internal_keys_skipped counter resets after a successful
  // read.
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddDeletion("b");
    internal_iter->AddDeletion("b");
    internal_iter->AddPut("c", "val_c");
    internal_iter->AddDeletion("d");
    internal_iter->AddDeletion("d");
    internal_iter->AddDeletion("d");
    internal_iter->AddPut("e", "val_e");
    internal_iter->Finish();

    ro.max_skippable_internal_keys = 2;
    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));

    db_iter->SeekToFirst();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "val_a");

    db_iter->Next();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "c");
    ASSERT_EQ(db_iter->value().ToString(), "val_c");

    db_iter->Next();  // num_internal_keys_skipped counter resets here.
    ASSERT_TRUE(!db_iter->Valid());
    ASSERT_TRUE(db_iter->status().IsIncomplete());
  }

  // Test that the num_internal_keys_skipped counter resets after a successful
  // read.
  // Reverse direction
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddDeletion("b");
    internal_iter->AddDeletion("b");
    internal_iter->AddDeletion("b");
    internal_iter->AddPut("c", "val_c");
    internal_iter->AddDeletion("d");
    internal_iter->AddDeletion("d");
    internal_iter->AddPut("e", "val_e");
    internal_iter->Finish();

    ro.max_skippable_internal_keys = 2;
    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "e");
    ASSERT_EQ(db_iter->value().ToString(), "val_e");

    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "c");
    ASSERT_EQ(db_iter->value().ToString(), "val_c");

    db_iter->Prev();  // num_internal_keys_skipped counter resets here.
    ASSERT_TRUE(!db_iter->Valid());
    ASSERT_TRUE(db_iter->status().IsIncomplete());
  }

  // Test that skipping separate keys is handled
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddDeletion("b");
    internal_iter->AddDeletion("c");
    internal_iter->AddDeletion("d");
    internal_iter->AddPut("e", "val_e");
    internal_iter->Finish();

    ro.max_skippable_internal_keys = 2;
    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));

    db_iter->SeekToFirst();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "val_a");

    db_iter->Next();
    ASSERT_TRUE(!db_iter->Valid());
    ASSERT_TRUE(db_iter->status().IsIncomplete());

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "e");
    ASSERT_EQ(db_iter->value().ToString(), "val_e");

    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
    ASSERT_TRUE(db_iter->status().IsIncomplete());
  }

  // Test if alternating puts and deletes of the same key are handled correctly.
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddPut("b", "val_b");
    internal_iter->AddDeletion("b");
    internal_iter->AddPut("c", "val_c");
    internal_iter->AddDeletion("c");
    internal_iter->AddPut("d", "val_d");
    internal_iter->AddDeletion("d");
    internal_iter->AddPut("e", "val_e");
    internal_iter->Finish();

    ro.max_skippable_internal_keys = 2;
    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));

    db_iter->SeekToFirst();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "val_a");

    db_iter->Next();
    ASSERT_TRUE(!db_iter->Valid());
    ASSERT_TRUE(db_iter->status().IsIncomplete());

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "e");
    ASSERT_EQ(db_iter->value().ToString(), "val_e");

    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
    ASSERT_TRUE(db_iter->status().IsIncomplete());
  }

  // Test for large number of skippable internal keys with *default*
  {
    for (size_t i = 1; i <= 200; ++i) {
      TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
      internal_iter->AddPut("a", "val_a");
      for (size_t j = 1; j <= i; ++j) {
        internal_iter->AddPut("b", "val_b");
        internal_iter->AddDeletion("b");
      }
      internal_iter->AddPut("c", "val_c");
      internal_iter->Finish();

      ro.max_skippable_internal_keys = i;
      unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
          env_, ro, cf_options, BytewiseComparator(), internal_iter, 2 * i + 1,
          100));

      db_iter->SeekToFirst();
      ASSERT_TRUE(db_iter->Valid());
      ASSERT_EQ(db_iter->key().ToString(), "a");
      ASSERT_EQ(db_iter->value().ToString(), "val_a");

      db_iter->Next();
      if ((100 + 1) >=
          ro.max_skippable_internal_keys) {
        ASSERT_TRUE(!db_iter->Valid());
        ASSERT_TRUE(db_iter->status().IsIncomplete());
      } else {
        ASSERT_TRUE(db_iter->Valid());
        ASSERT_EQ(db_iter->key().ToString(), "c");
        ASSERT_EQ(db_iter->value().ToString(), "val_c");
      }

      db_iter->SeekToLast();
      ASSERT_TRUE(db_iter->Valid());
      ASSERT_EQ(db_iter->key().ToString(), "c");
      ASSERT_EQ(db_iter->value().ToString(), "val_c");

      db_iter->Prev();
      if ((100 + 1) >=
          ro.max_skippable_internal_keys) {
        ASSERT_TRUE(!db_iter->Valid());
        ASSERT_TRUE(db_iter->status().IsIncomplete());
      } else {
        ASSERT_TRUE(db_iter->Valid());
        ASSERT_EQ(db_iter->key().ToString(), "a");
        ASSERT_EQ(db_iter->value().ToString(), "val_a");
      }
    }
  }

  // Test for large number of skippable internal keys with a *non-default*
  {
    for (size_t i = 1; i <= 200; ++i) {
      TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
      internal_iter->AddPut("a", "val_a");
      for (size_t j = 1; j <= i; ++j) {
        internal_iter->AddPut("b", "val_b");
        internal_iter->AddDeletion("b");
      }
      internal_iter->AddPut("c", "val_c");
      internal_iter->Finish();

      ro.max_skippable_internal_keys = i;
      unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
          env_, ro, cf_options, BytewiseComparator(), internal_iter, 2 * i + 1,
          1000));

      db_iter->SeekToFirst();
      ASSERT_TRUE(db_iter->Valid());
      ASSERT_EQ(db_iter->key().ToString(), "a");
      ASSERT_EQ(db_iter->value().ToString(), "val_a");

      db_iter->Next();
      ASSERT_TRUE(!db_iter->Valid());
      ASSERT_TRUE(db_iter->status().IsIncomplete());

      db_iter->SeekToLast();
      ASSERT_TRUE(db_iter->Valid());
      ASSERT_EQ(db_iter->key().ToString(), "c");
      ASSERT_EQ(db_iter->value().ToString(), "val_c");

      db_iter->Prev();
      ASSERT_TRUE(!db_iter->Valid());
      ASSERT_TRUE(db_iter->status().IsIncomplete());
    }
  }
}

TEST_F(DBIteratorTest, DBIterator1) {
  ReadOptions ro;
  Options options;

  TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
  internal_iter->AddPut("a", "0");
  internal_iter->AddPut("b", "0");
  internal_iter->AddDeletion("b");
  internal_iter->Finish();

  unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
      env_, ro, ImmutableCFOptions(options), BytewiseComparator(),
      internal_iter, 1, 100));
  db_iter->SeekToFirst();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "a");
  ASSERT_EQ(db_iter->value().ToString(), "0");
  db_iter->Next();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "b");
  db_iter->Next();
  ASSERT_FALSE(db_iter->Valid());
}

TEST_F(DBIteratorTest, DBIterator2) {
  ReadOptions ro;
  Options options;

  TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
  internal_iter->AddPut("a", "0");
  internal_iter->AddPut("b", "0");
  internal_iter->AddDeletion("b");
  internal_iter->Finish();

  unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
      env_, ro, ImmutableCFOptions(options), BytewiseComparator(),
      internal_iter, 0, 100));
  db_iter->SeekToFirst();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "a");
  ASSERT_EQ(db_iter->value().ToString(), "0");
  db_iter->Next();
  ASSERT_TRUE(!db_iter->Valid());
}

TEST_F(DBIteratorTest, DBIterator3) {
  ReadOptions ro;
  Options options;

  TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
  internal_iter->AddPut("a", "0");
  internal_iter->AddPut("b", "0");
  internal_iter->AddDeletion("b");
  internal_iter->Finish();

  unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
      env_, ro, ImmutableCFOptions(options), BytewiseComparator(),
      internal_iter, 2, 100));
  db_iter->SeekToFirst();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "a");
  ASSERT_EQ(db_iter->value().ToString(), "0");
  db_iter->Next();
  ASSERT_TRUE(!db_iter->Valid());
}

TEST_F(DBIteratorTest, DBIterator4) {
  ReadOptions ro;
  Options options;

  TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
  internal_iter->AddPut("a", "0");
  internal_iter->AddPut("b", "0");
  internal_iter->AddDeletion("b");
  internal_iter->AddPut("a", "1");
  internal_iter->AddPut("b", "2");
  internal_iter->Finish();

  unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
      env_, ro, ImmutableCFOptions(options), BytewiseComparator(),
      internal_iter, 4, 100));
  db_iter->SeekToFirst();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "a");
  ASSERT_EQ(db_iter->value().ToString(), "1");
  db_iter->Next();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "b");
  ASSERT_EQ(db_iter->value().ToString(), "2");
  db_iter->Next();
  ASSERT_TRUE(!db_iter->Valid());
}

TEST_F(DBIteratorTest, DBIterator5) {
  ReadOptions ro;
  Options options;
  ImmutableCFOptions cf_options = ImmutableCFOptions(options);

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      0, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      1, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      2, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      3, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      4, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      5, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      6, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    // put, singledelete, merge
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "val_a");
    internal_iter->AddSingleDeletion("a");
    internal_iter->AddPut("b", "val_b");
    internal_iter->Finish();
    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      10, 100));
    db_iter->Seek("b");
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }
}

TEST_F(DBIteratorTest, DBIterator6) {
  ReadOptions ro;
  Options options;
  ImmutableCFOptions cf_options = ImmutableCFOptions(options);

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("a", "put_2");
    internal_iter->AddPut("a", "put_3");
    internal_iter->AddDeletion("a");
    internal_iter->AddPut("a", "put_4");
    internal_iter->AddPut("a", "put_5");
    internal_iter->AddPut("a", "put_6");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      0, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("a", "put_2");
    internal_iter->AddPut("a", "put_3");
    internal_iter->AddDeletion("a");
    internal_iter->AddPut("a", "put_4");
    internal_iter->AddPut("a", "put_5");
    internal_iter->AddPut("a", "put_6");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      1, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_2");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("a", "put_2");
    internal_iter->AddPut("a", "put_3");
    internal_iter->AddDeletion("a");
    internal_iter->AddPut("a", "put_4");
    internal_iter->AddPut("a", "put_5");
    internal_iter->AddPut("a", "put_6");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      2, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_3");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("a", "put_2");
    internal_iter->AddPut("a", "put_3");
    internal_iter->AddDeletion("a");
    internal_iter->AddPut("a", "put_4");
    internal_iter->AddPut("a", "put_5");
    internal_iter->AddPut("a", "put_6");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      3, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("a", "put_2");
    internal_iter->AddPut("a", "put_3");
    internal_iter->AddDeletion("a");
    internal_iter->AddPut("a", "put_4");
    internal_iter->AddPut("a", "put_5");
    internal_iter->AddPut("a", "put_6");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      4, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_4");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("a", "put_2");
    internal_iter->AddPut("a", "put_3");
    internal_iter->AddDeletion("a");
    internal_iter->AddPut("a", "put_4");
    internal_iter->AddPut("a", "put_5");
    internal_iter->AddPut("a", "put_6");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      5, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_5");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("a", "put_2");
    internal_iter->AddPut("a", "put_3");
    internal_iter->AddDeletion("a");
    internal_iter->AddPut("a", "put_4");
    internal_iter->AddPut("a", "put_5");
    internal_iter->AddPut("a", "put_6");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      6, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_6");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }
}

TEST_F(DBIteratorTest, DBIterator7) {
  ReadOptions ro;
  Options options;
  ImmutableCFOptions cf_options = ImmutableCFOptions(options);

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("b", "val");
    internal_iter->AddPut("b", "put_2");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_3");

    internal_iter->AddPut("c", "put_4");
    internal_iter->AddPut("c", "put_5");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_6");
    internal_iter->AddPut("b", "put_7");
    internal_iter->AddPut("b", "put_8");
    internal_iter->AddPut("b", "put_9");
    internal_iter->AddPut("b", "put_10");
    internal_iter->AddPut("b", "put_11");

    internal_iter->AddDeletion("c");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      0, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("b", "val");
    internal_iter->AddPut("b", "put_2");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_3");

    internal_iter->AddPut("c", "put_4");
    internal_iter->AddPut("c", "put_5");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_6");
    internal_iter->AddPut("b", "put_7");
    internal_iter->AddPut("b", "put_8");
    internal_iter->AddPut("b", "put_9");
    internal_iter->AddPut("b", "put_10");
    internal_iter->AddPut("b", "put_11");

    internal_iter->AddDeletion("c");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      2, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "put_2");
    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("b", "val");
    internal_iter->AddPut("b", "put_2");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_3");

    internal_iter->AddPut("c", "put_4");
    internal_iter->AddPut("c", "put_5");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_6");
    internal_iter->AddPut("b", "put_7");
    internal_iter->AddPut("b", "put_8");
    internal_iter->AddPut("b", "put_9");
    internal_iter->AddPut("b", "put_10");
    internal_iter->AddPut("b", "put_11");

    internal_iter->AddDeletion("c");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      4, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "put_3");
    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("b", "val");
    internal_iter->AddPut("b", "put_2");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_3");

    internal_iter->AddPut("c", "put_4");
    internal_iter->AddPut("c", "put_5");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_6");
    internal_iter->AddPut("b", "put_7");
    internal_iter->AddPut("b", "put_8");
    internal_iter->AddPut("b", "put_9");
    internal_iter->AddPut("b", "put_10");
    internal_iter->AddPut("b", "put_11");

    internal_iter->AddDeletion("c");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      5, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "c");
    ASSERT_EQ(db_iter->value().ToString(), "put_4");
    db_iter->Prev();

    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "put_3");
    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("b", "val");
    internal_iter->AddPut("b", "put_2");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_3");

    internal_iter->AddPut("c", "put_4");
    internal_iter->AddPut("c", "put_5");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_6");
    internal_iter->AddPut("b", "put_7");
    internal_iter->AddPut("b", "put_8");
    internal_iter->AddPut("b", "put_9");
    internal_iter->AddPut("b", "put_10");
    internal_iter->AddPut("b", "put_11");

    internal_iter->AddDeletion("c");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      6, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "c");
    ASSERT_EQ(db_iter->value().ToString(), "put_5");
    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "put_3");
    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("b", "val");
    internal_iter->AddPut("b", "put_2");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_3");

    internal_iter->AddPut("c", "put_4");
    internal_iter->AddPut("c", "put_5");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_6");
    internal_iter->AddPut("b", "put_7");
    internal_iter->AddPut("b", "put_8");
    internal_iter->AddPut("b", "put_9");
    internal_iter->AddPut("b", "put_10");
    internal_iter->AddPut("b", "put_11");

    internal_iter->AddDeletion("c");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      7, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "c");
    ASSERT_EQ(db_iter->value().ToString(), "put_5");
    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("b", "val");
    internal_iter->AddPut("b", "put_2");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_3");

    internal_iter->AddPut("c", "put_4");
    internal_iter->AddPut("c", "put_5");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_6");
    internal_iter->AddPut("b", "put_7");
    internal_iter->AddPut("b", "put_8");
    internal_iter->AddPut("b", "put_9");
    internal_iter->AddPut("b", "put_10");
    internal_iter->AddPut("b", "put_11");

    internal_iter->AddDeletion("c");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      9, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "c");
    ASSERT_EQ(db_iter->value().ToString(), "put_5");
    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "put_7");
    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("b", "val");
    internal_iter->AddPut("b", "put_2");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_3");

    internal_iter->AddPut("c", "put_4");
    internal_iter->AddPut("c", "put_5");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_6");
    internal_iter->AddPut("b", "put_7");
    internal_iter->AddPut("b", "put_8");
    internal_iter->AddPut("b", "put_9");
    internal_iter->AddPut("b", "put_10");
    internal_iter->AddPut("b", "put_11");

    internal_iter->AddDeletion("c");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      13, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "c");
    ASSERT_EQ(db_iter->value().ToString(), "put_5");
    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "put_11");
    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }

  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("b", "val");
    internal_iter->AddPut("b", "put_2");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_3");

    internal_iter->AddPut("c", "put_4");
    internal_iter->AddPut("c", "put_5");

    internal_iter->AddDeletion("b");
    internal_iter->AddPut("b", "put_6");
    internal_iter->AddPut("b", "put_7");
    internal_iter->AddPut("b", "put_8");
    internal_iter->AddPut("b", "put_9");
    internal_iter->AddPut("b", "put_10");
    internal_iter->AddPut("b", "put_11");

    internal_iter->AddDeletion("c");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
        NewDBIterator(env_, ro, cf_options, BytewiseComparator(), internal_iter,
                      14, 100));
    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "put_11");
    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());

    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_1");
    db_iter->Prev();
    ASSERT_TRUE(!db_iter->Valid());
  }
}

TEST_F(DBIteratorTest, DBIterator8) {
  ReadOptions ro;
  Options options;

  TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
  internal_iter->AddDeletion("a");
  internal_iter->AddPut("a", "0");
  internal_iter->AddPut("b", "0");
  internal_iter->Finish();

  unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
      env_, ro, ImmutableCFOptions(options), BytewiseComparator(),
      internal_iter, 10, 100));
  db_iter->SeekToLast();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "b");
  ASSERT_EQ(db_iter->value().ToString(), "0");

  db_iter->Prev();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "a");
  ASSERT_EQ(db_iter->value().ToString(), "0");
}

// TODO(3.13): fix the issue of Seek() then Prev() which might not necessary
//             return the biggest element smaller than the seek key.
TEST_F(DBIteratorTest, DBIterator9) {
  ReadOptions ro;
  Options options;
  {
    TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter->AddPut("a", "put_1");
    internal_iter->AddPut("a", "put_2");
    internal_iter->AddPut("b", "put_3");
    internal_iter->AddPut("b", "put_4");
    internal_iter->AddPut("d", "put_5");
    internal_iter->AddPut("d", "put_6");
    internal_iter->Finish();

    unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
        env_, ro, ImmutableCFOptions(options), BytewiseComparator(),
        internal_iter, 10, 100));

    db_iter->SeekToLast();
    ASSERT_TRUE(db_iter->Valid());
    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "put_4");
    db_iter->Next();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "d");
    ASSERT_EQ(db_iter->value().ToString(), "put_6");

    db_iter->Seek("b");
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "put_4");
    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "a");
    ASSERT_EQ(db_iter->value().ToString(), "put_2");

    db_iter->SeekForPrev("b");
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "put_4");
    db_iter->Next();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "d");
    ASSERT_EQ(db_iter->value().ToString(), "put_6");

    db_iter->Seek("c");
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "d");
    ASSERT_EQ(db_iter->value().ToString(), "put_6");
    db_iter->Prev();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "put_4");

    db_iter->SeekForPrev("c");
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "b");
    ASSERT_EQ(db_iter->value().ToString(), "put_4");
    db_iter->Next();
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_EQ(db_iter->key().ToString(), "d");
    ASSERT_EQ(db_iter->value().ToString(), "put_6");
  }
}

// TODO(3.13): fix the issue of Seek() then Prev() which might not necessary
//             return the biggest element smaller than the seek key.
TEST_F(DBIteratorTest, DBIterator10) {
  ReadOptions ro;
  Options options;

  TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
  internal_iter->AddPut("a", "1");
  internal_iter->AddPut("b", "2");
  internal_iter->AddPut("c", "3");
  internal_iter->AddPut("d", "4");
  internal_iter->Finish();

  unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
      env_, ro, ImmutableCFOptions(options), BytewiseComparator(),
      internal_iter, 10, 100));

  db_iter->Seek("c");
  ASSERT_TRUE(db_iter->Valid());
  db_iter->Prev();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "b");
  ASSERT_EQ(db_iter->value().ToString(), "2");

  db_iter->Next();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "c");
  ASSERT_EQ(db_iter->value().ToString(), "3");

  db_iter->SeekForPrev("c");
  ASSERT_TRUE(db_iter->Valid());
  db_iter->Next();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "d");
  ASSERT_EQ(db_iter->value().ToString(), "4");

  db_iter->Prev();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "c");
  ASSERT_EQ(db_iter->value().ToString(), "3");
}

TEST_F(DBIteratorTest, SeekToLastOccurrenceSeq0) {
  ReadOptions ro;
  Options options;

  TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
  internal_iter->AddPut("a", "1");
  internal_iter->AddPut("b", "2");
  internal_iter->Finish();

  unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
      NewDBIterator(env_, ro, ImmutableCFOptions(options), BytewiseComparator(),
                    internal_iter, 10, 0 /* force seek */));
  db_iter->SeekToFirst();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "a");
  ASSERT_EQ(db_iter->value().ToString(), "1");
  db_iter->Next();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "b");
  ASSERT_EQ(db_iter->value().ToString(), "2");
  db_iter->Next();
  ASSERT_FALSE(db_iter->Valid());
}

TEST_F(DBIteratorTest, DBIterator11) {
  ReadOptions ro;
  Options options;

  TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
  internal_iter->AddPut("a", "0");
  internal_iter->AddPut("b", "0");
  internal_iter->AddSingleDeletion("b");
  internal_iter->Finish();

  unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(NewDBIterator(
      env_, ro, ImmutableCFOptions(options), BytewiseComparator(),
      internal_iter, 1, 100));
  db_iter->SeekToFirst();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "a");
  ASSERT_EQ(db_iter->value().ToString(), "0");
  db_iter->Next();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "b");
  db_iter->Next();
  ASSERT_FALSE(db_iter->Valid());
}

TEST_F(DBIteratorTest, DBIterator12) {
  ReadOptions ro;
  Options options;

  TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
  internal_iter->AddPut("a", "1");
  internal_iter->AddPut("b", "2");
  internal_iter->AddPut("c", "3");
  internal_iter->AddSingleDeletion("b");
  internal_iter->Finish();

  unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
      NewDBIterator(env_, ro, ImmutableCFOptions(options), BytewiseComparator(),
                    internal_iter, 10, 0));
  db_iter->SeekToLast();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "c");
  ASSERT_EQ(db_iter->value().ToString(), "3");
  db_iter->Prev();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "a");
  ASSERT_EQ(db_iter->value().ToString(), "1");
  db_iter->Prev();
  ASSERT_FALSE(db_iter->Valid());
}

TEST_F(DBIteratorTest, DBIterator13) {
  ReadOptions ro;
  Options options;

  std::string key;
  key.resize(9);
  key.assign(9, static_cast<char>(0));
  key[0] = 'b';

  TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
  internal_iter->AddPut(key, "0");
  internal_iter->AddPut(key, "1");
  internal_iter->AddPut(key, "2");
  internal_iter->AddPut(key, "3");
  internal_iter->AddPut(key, "4");
  internal_iter->AddPut(key, "5");
  internal_iter->AddPut(key, "6");
  internal_iter->AddPut(key, "7");
  internal_iter->AddPut(key, "8");
  internal_iter->Finish();

  unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
      NewDBIterator(env_, ro, ImmutableCFOptions(options), BytewiseComparator(),
                    internal_iter, 2, 3));
  db_iter->Seek("b");
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), key);
  ASSERT_EQ(db_iter->value().ToString(), "2");
}

TEST_F(DBIteratorTest, DBIterator14) {
  ReadOptions ro;
  Options options;

  std::string key("b");
  TestIterator* internal_iter = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
  internal_iter->AddPut("b", "0");
  internal_iter->AddPut("b", "1");
  internal_iter->AddPut("b", "2");
  internal_iter->AddPut("b", "3");
  internal_iter->AddPut("a", "4");
  internal_iter->AddPut("a", "5");
  internal_iter->AddPut("a", "6");
  internal_iter->AddPut("c", "7");
  internal_iter->AddPut("c", "8");
  internal_iter->AddPut("c", "9");
  internal_iter->Finish();

  unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter(
      NewDBIterator(env_, ro, ImmutableCFOptions(options), BytewiseComparator(),
                    internal_iter, 4, 1));
  db_iter->Seek("b");
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key().ToString(), "b");
  ASSERT_EQ(db_iter->value().ToString(), "3");
  db_iter->SeekToFirst();
  ASSERT_EQ(db_iter->key().ToString(), "a");
  ASSERT_EQ(db_iter->value().ToString(), "4");
}

class DBIterWithMergeIterTest : public testing::Test {
 public:
  DBIterWithMergeIterTest()
      : env_(Env::Default()), icomp_(BytewiseComparator()) {

    internal_iter1_ = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter1_->Add("a", kTypeValue, "1", 3u);
    internal_iter1_->Add("f", kTypeValue, "2", 5u);
    internal_iter1_->Add("g", kTypeValue, "3", 7u);
    internal_iter1_->Finish();

    internal_iter2_ = MOD_NEW_OBJECT(ModId::kDefaultMod, TestIterator, BytewiseComparator());
    internal_iter2_->Add("a", kTypeValue, "4", 6u);
    internal_iter2_->Add("b", kTypeValue, "5", 1u);
    internal_iter2_->Add("c", kTypeValue, "6", 2u);
    internal_iter2_->Add("d", kTypeValue, "7", 3u);
    internal_iter2_->Finish();

    std::vector<InternalIterator*> child_iters;
    child_iters.push_back(internal_iter1_);
    child_iters.push_back(internal_iter2_);
    InternalKeyComparator icomp(BytewiseComparator());
    InternalIterator* merge_iter =
        NewMergingIterator(&icomp_, &child_iters[0], 2u, nullptr /*arena*/);

    db_iter_.reset(NewDBIterator(env_, ro_, ImmutableCFOptions(options_),
                                 BytewiseComparator(), merge_iter,
                                 8 /* read data earlier than seqId 8 */,
                                 3 /* max iterators before reseek */));
  }

  Env* env_;
  ReadOptions ro_;
  Options options_;
  TestIterator* internal_iter1_;
  TestIterator* internal_iter2_;
  InternalKeyComparator icomp_;
  Iterator* merge_iter_;
  unique_ptr<Iterator, ptr_destruct_delete<Iterator>> db_iter_;
};

TEST_F(DBIterWithMergeIterTest, InnerMergeIterator1) {
  db_iter_->SeekToFirst();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "a");
  ASSERT_EQ(db_iter_->value().ToString(), "4");
  db_iter_->Next();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "b");
  ASSERT_EQ(db_iter_->value().ToString(), "5");
  db_iter_->Next();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "c");
  ASSERT_EQ(db_iter_->value().ToString(), "6");
  db_iter_->Next();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "d");
  ASSERT_EQ(db_iter_->value().ToString(), "7");
  db_iter_->Next();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "f");
  ASSERT_EQ(db_iter_->value().ToString(), "2");
  db_iter_->Next();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "g");
  ASSERT_EQ(db_iter_->value().ToString(), "3");
  db_iter_->Next();
  ASSERT_FALSE(db_iter_->Valid());
}

TEST_F(DBIterWithMergeIterTest, InnerMergeIterator2) {
  // Test Prev() when one child iterator is at its end.
  db_iter_->SeekForPrev("g");
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "g");
  ASSERT_EQ(db_iter_->value().ToString(), "3");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "f");
  ASSERT_EQ(db_iter_->value().ToString(), "2");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "d");
  ASSERT_EQ(db_iter_->value().ToString(), "7");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "c");
  ASSERT_EQ(db_iter_->value().ToString(), "6");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "b");
  ASSERT_EQ(db_iter_->value().ToString(), "5");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "a");
  ASSERT_EQ(db_iter_->value().ToString(), "4");
}

TEST_F(DBIterWithMergeIterTest, InnerMergeIteratorDataRace1) {
  // Test Prev() when one child iterator is at its end but more rows
  // are added.
  db_iter_->Seek("f");
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "f");
  ASSERT_EQ(db_iter_->value().ToString(), "2");

  // Test call back inserts a key in the end of the mem table after
  // MergeIterator::Prev() realized the mem table iterator is at its end
  // and before an SeekToLast() is called.
  SyncPoint::GetInstance()->SetCallBack(
      "MergeIterator::Prev:BeforeSeekToLast",
      [&](void* arg) { internal_iter2_->Add("z", kTypeValue, "7", 12u); });
  SyncPoint::GetInstance()->EnableProcessing();

  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "d");
  ASSERT_EQ(db_iter_->value().ToString(), "7");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "c");
  ASSERT_EQ(db_iter_->value().ToString(), "6");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "b");
  ASSERT_EQ(db_iter_->value().ToString(), "5");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "a");
  ASSERT_EQ(db_iter_->value().ToString(), "4");

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBIterWithMergeIterTest, InnerMergeIteratorDataRace2) {
  // Test Prev() when one child iterator is at its end but more rows
  // are added.
  db_iter_->Seek("f");
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "f");
  ASSERT_EQ(db_iter_->value().ToString(), "2");

  // Test call back inserts entries for update a key in the end of the
  // mem table after MergeIterator::Prev() realized the mem tableiterator is at
  // its end and before an SeekToLast() is called.
  SyncPoint::GetInstance()->SetCallBack(
      "MergeIterator::Prev:BeforeSeekToLast", [&](void* arg) {
        internal_iter2_->Add("z", kTypeValue, "7", 12u);
        internal_iter2_->Add("z", kTypeValue, "7", 11u);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "d");
  ASSERT_EQ(db_iter_->value().ToString(), "7");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "c");
  ASSERT_EQ(db_iter_->value().ToString(), "6");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "b");
  ASSERT_EQ(db_iter_->value().ToString(), "5");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "a");
  ASSERT_EQ(db_iter_->value().ToString(), "4");

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBIterWithMergeIterTest, InnerMergeIteratorDataRace3) {
  // Test Prev() when one child iterator is at its end but more rows
  // are added and max_skipped is triggered.
  db_iter_->Seek("f");
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "f");
  ASSERT_EQ(db_iter_->value().ToString(), "2");

  // Test call back inserts entries for update a key in the end of the
  // mem table after MergeIterator::Prev() realized the mem table iterator is at
  // its end and before an SeekToLast() is called.
  SyncPoint::GetInstance()->SetCallBack(
      "MergeIterator::Prev:BeforeSeekToLast", [&](void* arg) {
        internal_iter2_->Add("z", kTypeValue, "7", 16u, true);
        internal_iter2_->Add("z", kTypeValue, "7", 15u, true);
        internal_iter2_->Add("z", kTypeValue, "7", 14u, true);
        internal_iter2_->Add("z", kTypeValue, "7", 13u, true);
        internal_iter2_->Add("z", kTypeValue, "7", 12u, true);
        internal_iter2_->Add("z", kTypeValue, "7", 11u, true);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "d");
  ASSERT_EQ(db_iter_->value().ToString(), "7");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "c");
  ASSERT_EQ(db_iter_->value().ToString(), "6");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "b");
  ASSERT_EQ(db_iter_->value().ToString(), "5");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "a");
  ASSERT_EQ(db_iter_->value().ToString(), "4");

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBIterWithMergeIterTest, InnerMergeIteratorDataRace4) {
  // Test Prev() when one child iterator has more rows inserted
  // between Seek() and Prev() when changing directions.
  internal_iter2_->Add("z", kTypeValue, "9", 4u);

  db_iter_->Seek("g");
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "g");
  ASSERT_EQ(db_iter_->value().ToString(), "3");

  // Test call back inserts entries for update a key before "z" in
  // mem table after MergeIterator::Prev() calls mem table iterator's
  // Seek() and before calling Prev()
  SyncPoint::GetInstance()->SetCallBack(
      "MergeIterator::Prev:BeforePrev", [&](void* arg) {
        IteratorWrapper* it = reinterpret_cast<IteratorWrapper*>(arg);
        if (it->key().starts_with("z")) {
          internal_iter2_->Add("x", kTypeValue, "7", 16u, true);
          internal_iter2_->Add("x", kTypeValue, "7", 15u, true);
          internal_iter2_->Add("x", kTypeValue, "7", 14u, true);
          internal_iter2_->Add("x", kTypeValue, "7", 13u, true);
          internal_iter2_->Add("x", kTypeValue, "7", 12u, true);
          internal_iter2_->Add("x", kTypeValue, "7", 11u, true);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "f");
  ASSERT_EQ(db_iter_->value().ToString(), "2");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "d");
  ASSERT_EQ(db_iter_->value().ToString(), "7");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "c");
  ASSERT_EQ(db_iter_->value().ToString(), "6");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "b");
  ASSERT_EQ(db_iter_->value().ToString(), "5");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "a");
  ASSERT_EQ(db_iter_->value().ToString(), "4");

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBIterWithMergeIterTest, InnerMergeIteratorDataRace5) {
  internal_iter2_->Add("z", kTypeValue, "9", 4u);

  // Test Prev() when one child iterator has more rows inserted
  // between Seek() and Prev() when changing directions.
  db_iter_->Seek("g");
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "g");
  ASSERT_EQ(db_iter_->value().ToString(), "3");

  // Test call back inserts entries for update a key before "z" in
  // mem table after MergeIterator::Prev() calls mem table iterator's
  // Seek() and before calling Prev()
  SyncPoint::GetInstance()->SetCallBack(
      "MergeIterator::Prev:BeforePrev", [&](void* arg) {
        IteratorWrapper* it = reinterpret_cast<IteratorWrapper*>(arg);
        if (it->key().starts_with("z")) {
          internal_iter2_->Add("x", kTypeValue, "7", 16u, true);
          internal_iter2_->Add("x", kTypeValue, "7", 15u, true);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "f");
  ASSERT_EQ(db_iter_->value().ToString(), "2");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "d");
  ASSERT_EQ(db_iter_->value().ToString(), "7");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "c");
  ASSERT_EQ(db_iter_->value().ToString(), "6");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "b");
  ASSERT_EQ(db_iter_->value().ToString(), "5");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "a");
  ASSERT_EQ(db_iter_->value().ToString(), "4");

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBIterWithMergeIterTest, InnerMergeIteratorDataRace6) {
  internal_iter2_->Add("z", kTypeValue, "9", 4u);

  // Test Prev() when one child iterator has more rows inserted
  // between Seek() and Prev() when changing directions.
  db_iter_->Seek("g");
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "g");
  ASSERT_EQ(db_iter_->value().ToString(), "3");

  // Test call back inserts an entry for update a key before "z" in
  // mem table after MergeIterator::Prev() calls mem table iterator's
  // Seek() and before calling Prev()
  SyncPoint::GetInstance()->SetCallBack(
      "MergeIterator::Prev:BeforePrev", [&](void* arg) {
        IteratorWrapper* it = reinterpret_cast<IteratorWrapper*>(arg);
        if (it->key().starts_with("z")) {
          internal_iter2_->Add("x", kTypeValue, "7", 16u, true);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "f");
  ASSERT_EQ(db_iter_->value().ToString(), "2");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "d");
  ASSERT_EQ(db_iter_->value().ToString(), "7");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "c");
  ASSERT_EQ(db_iter_->value().ToString(), "6");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "b");
  ASSERT_EQ(db_iter_->value().ToString(), "5");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "a");
  ASSERT_EQ(db_iter_->value().ToString(), "4");

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBIterWithMergeIterTest, InnerMergeIteratorDataRace7) {
  internal_iter1_->Add("u", kTypeValue, "10", 4u);
  internal_iter1_->Add("v", kTypeValue, "11", 4u);
  internal_iter1_->Add("w", kTypeValue, "12", 4u);
  internal_iter2_->Add("z", kTypeValue, "9", 4u);

  // Test Prev() when one child iterator has more rows inserted
  // between Seek() and Prev() when changing directions.
  db_iter_->Seek("g");
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "g");
  ASSERT_EQ(db_iter_->value().ToString(), "3");

  // Test call back inserts entries for update a key before "z" in
  // mem table after MergeIterator::Prev() calls mem table iterator's
  // Seek() and before calling Prev()
  SyncPoint::GetInstance()->SetCallBack(
      "MergeIterator::Prev:BeforePrev", [&](void* arg) {
        IteratorWrapper* it = reinterpret_cast<IteratorWrapper*>(arg);
        if (it->key().starts_with("z")) {
          internal_iter2_->Add("x", kTypeValue, "7", 16u, true);
          internal_iter2_->Add("x", kTypeValue, "7", 15u, true);
          internal_iter2_->Add("x", kTypeValue, "7", 14u, true);
          internal_iter2_->Add("x", kTypeValue, "7", 13u, true);
          internal_iter2_->Add("x", kTypeValue, "7", 12u, true);
          internal_iter2_->Add("x", kTypeValue, "7", 11u, true);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "f");
  ASSERT_EQ(db_iter_->value().ToString(), "2");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "d");
  ASSERT_EQ(db_iter_->value().ToString(), "7");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "c");
  ASSERT_EQ(db_iter_->value().ToString(), "6");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "b");
  ASSERT_EQ(db_iter_->value().ToString(), "5");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "a");
  ASSERT_EQ(db_iter_->value().ToString(), "4");

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBIterWithMergeIterTest, InnerMergeIteratorDataRace8) {
  // internal_iter1_: a, f, g
  // internal_iter2_: a, b, c, d, adding (z)
  internal_iter2_->Add("z", kTypeValue, "9", 4u);

  // Test Prev() when one child iterator has more rows inserted
  // between Seek() and Prev() when changing directions.
  db_iter_->Seek("g");
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "g");
  ASSERT_EQ(db_iter_->value().ToString(), "3");

  // Test call back inserts two keys before "z" in mem table after
  // MergeIterator::Prev() calls mem table iterator's Seek() and
  // before calling Prev()
  SyncPoint::GetInstance()->SetCallBack(
      "MergeIterator::Prev:BeforePrev", [&](void* arg) {
        IteratorWrapper* it = reinterpret_cast<IteratorWrapper*>(arg);
        if (it->key().starts_with("z")) {
          internal_iter2_->Add("x", kTypeValue, "7", 16u, true);
          internal_iter2_->Add("y", kTypeValue, "7", 17u, true);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "f");
  ASSERT_EQ(db_iter_->value().ToString(), "2");
  db_iter_->Prev();
  ASSERT_TRUE(db_iter_->Valid());
  ASSERT_EQ(db_iter_->key().ToString(), "d");
  ASSERT_EQ(db_iter_->value().ToString(), "7");

  SyncPoint::GetInstance()->DisableProcessing();
}
}
}  // namespace smartengine

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
	smartengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
