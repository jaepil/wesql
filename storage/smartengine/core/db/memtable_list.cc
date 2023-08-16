//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "db/memtable_list.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <string>
#include "db/memtable.h"
#include "db/version_set.h"
#include "monitoring/thread_status_util.h"
#include "table/merging_iterator.h"
#include "util/coding.h"
#include "util/log_buffer.h"
#include "util/sync_point.h"
#include "smartengine/db.h"
#include "smartengine/env.h"
#include "smartengine/iterator.h"
#include "storage/storage_manager.h"
#include "storage/extent_space_manager.h"

using namespace smartengine;
using namespace util;
using namespace common;
using namespace table;
using namespace monitor;
using namespace storage;

namespace smartengine {
namespace prot {
class Mutex;
}
namespace db {

class InternalKeyComparator;
class VersionSet;
struct MiniTables;

void MemTableListVersion::AddMemTable(MemTable* m) {
  memlist_.push_front(m);
  *parent_memtable_list_memory_usage_ += m->ApproximateMemoryUsage();
}

void MemTableListVersion::UnrefMemTable(autovector<MemTable*>* to_delete,
                                        MemTable* m) {
  if (m->Unref()) {
    to_delete->push_back(m);
    assert(*parent_memtable_list_memory_usage_ >= m->ApproximateMemoryUsage());
    *parent_memtable_list_memory_usage_ -= m->ApproximateMemoryUsage();
  } else {
  }
}

MemTableListVersion::MemTableListVersion(
    size_t* parent_memtable_list_memory_usage, MemTableListVersion* old)
    : max_write_buffer_number_to_maintain_(
          old->max_write_buffer_number_to_maintain_),
      parent_memtable_list_memory_usage_(parent_memtable_list_memory_usage) {
  if (old != nullptr) {
    memlist_ = old->memlist_;
    for (auto& m : memlist_) {
      m->Ref();
    }

    memlist_history_ = old->memlist_history_;
    for (auto& m : memlist_history_) {
      m->Ref();
    }
  }
}

MemTableListVersion::MemTableListVersion(
    size_t* parent_memtable_list_memory_usage,
    int max_write_buffer_number_to_maintain)
    : max_write_buffer_number_to_maintain_(max_write_buffer_number_to_maintain),
      parent_memtable_list_memory_usage_(parent_memtable_list_memory_usage) {}

void MemTableListVersion::Ref() { ++refs_; }

// called by superversion::clean()
void MemTableListVersion::Unref(autovector<MemTable*>* to_delete) {
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    // if to_delete is equal to nullptr it means we're confident
    // that refs_ will not be zero
    assert(to_delete != nullptr);
    for (const auto& m : memlist_) {
      UnrefMemTable(to_delete, m);
    }
    for (const auto& m : memlist_history_) {
      UnrefMemTable(to_delete, m);
    }
//    delete this;
    auto ptr = this;
    MOD_DELETE_OBJECT(MemTableListVersion, ptr);
  }
}

int MemTableList::NumNotFlushed() const {
  int size = static_cast<int>(current_->memlist_.size());
  assert(num_flush_not_started_ <= size);
  return size;
}

int MemTableList::NumFlushed() const {
  return static_cast<int>(current_->memlist_history_.size());
}

// Search all the memtables starting from the most recent one.
// Return the most recent value found, if any.
// Operands stores the list of merge operations to apply, so far.
bool MemTableListVersion::Get(LookupKey& key, std::string* value, Status* s,
                              MergeContext* merge_context,
                              RangeDelAggregator* range_del_agg,
                              SequenceNumber* seq,
                              const ReadOptions& read_opts) {
  return GetFromList(&memlist_, key, value, s, merge_context, range_del_agg,
                     seq, read_opts);
}

bool MemTableListVersion::GetFromHistory(LookupKey& key, std::string* value,
                                         Status* s, MergeContext* merge_context,
                                         RangeDelAggregator* range_del_agg,
                                         SequenceNumber* seq,
                                         const ReadOptions& read_opts) {
  return GetFromList(&memlist_history_, key, value, s, merge_context,
                     range_del_agg, seq, read_opts);
}

bool MemTableListVersion::GetFromList(std::list<MemTable*>* list,
                                      LookupKey& key, std::string* value,
                                      Status* s, MergeContext* merge_context,
                                      RangeDelAggregator* range_del_agg,
                                      SequenceNumber* seq,
                                      const ReadOptions& read_opts) {
  *seq = kMaxSequenceNumber;

  for (auto& memtable : *list) {
    SequenceNumber current_seq = kMaxSequenceNumber;

    bool done = memtable->Get(key, value, s, merge_context, range_del_agg,
                              &current_seq, read_opts);
    if (*seq == kMaxSequenceNumber) {
      // Store the most recent sequence number of any operation on this key.
      // Since we only care about the most recent change, we only need to
      // return the first operation found when searching memtables in
      // reverse-chronological order.
      *seq = current_seq;
    }

    if (done) {
      assert(*seq != kMaxSequenceNumber);
      return true;
    }
    if (!done && !s->ok() && !s->IsMergeInProgress() && !s->IsNotFound()) {
      return false;
    }
  }
  return false;
}

Status MemTableListVersion::AddRangeTombstoneIterators(
    const ReadOptions& read_opts, Arena* arena,
    RangeDelAggregator* range_del_agg) {
  assert(range_del_agg != nullptr);
  for (auto& m : memlist_) {
//    std::unique_ptr<InternalIterator, memory::ptr_delete<InternalIterator>> range_del_iter(
//        m->NewRangeTombstoneIterator(read_opts));
    Status s = range_del_agg->AddTombstones(m->NewRangeTombstoneIterator(read_opts));
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

void MemTableListVersion::AddIterators(
    const ReadOptions& options, std::vector<InternalIterator*>* iterator_list,
    Arena* arena) {
  for (auto& m : memlist_) {
    iterator_list->push_back(m->NewIterator(options, arena));
  }
}

void MemTableListVersion::AddIterators(
    const ReadOptions& options, MergeIteratorBuilder* merge_iter_builder) {
  for (auto& m : memlist_) {
    merge_iter_builder->AddIterator(
        m->NewIterator(options, merge_iter_builder->GetArena()));
  }
}

uint64_t MemTableListVersion::GetTotalNumEntries() const {
  uint64_t total_num = 0;
  for (auto& m : memlist_) {
    total_num += m->num_entries();
  }
  return total_num;
}

MemTable::MemTableStats MemTableListVersion::ApproximateStats(
    const Slice& start_ikey, const Slice& end_ikey) {
  MemTable::MemTableStats total_stats = {0, 0};
  for (auto& m : memlist_) {
    auto mStats = m->ApproximateStats(start_ikey, end_ikey);
    total_stats.size += mStats.size;
    total_stats.count += mStats.count;
  }
  return total_stats;
}

uint64_t MemTableListVersion::GetTotalNumDeletes() const {
  uint64_t total_num = 0;
  for (auto& m : memlist_) {
    total_num += m->num_deletes();
  }
  return total_num;
}

SequenceNumber MemTableListVersion::GetEarliestSequenceNumber(
    bool include_history) const {
  if (include_history && !memlist_history_.empty()) {
    return memlist_history_.back()->GetEarliestSequenceNumber();
  } else if (!memlist_.empty()) {
    return memlist_.back()->GetEarliestSequenceNumber();
  } else {
    return kMaxSequenceNumber;
  }
}

// caller is responsible for referencing m
void MemTableListVersion::Add(MemTable* m, autovector<MemTable*>* to_delete) {
  assert(refs_ == 1);  // only when refs_ == 1 is MemTableListVersion mutable
  AddMemTable(m);

  TrimHistory(to_delete);
}

// Removes m from list of memtables not flushed.  Caller should NOT Unref m.
void MemTableListVersion::Remove(MemTable* m,
                                 autovector<MemTable*>* to_delete) {
  assert(refs_ == 1);  // only when refs_ == 1 is MemTableListVersion mutable
  memlist_.remove(m);

  if (max_write_buffer_number_to_maintain_ > 0) {
    memlist_history_.push_front(m);
    TrimHistory(to_delete);
  } else {
    UnrefMemTable(to_delete, m);
  }
}

// Make sure we don't use up too much space in history
void MemTableListVersion::TrimHistory(autovector<MemTable*>* to_delete) {
  while (memlist_.size() + memlist_history_.size() >
             static_cast<size_t>(max_write_buffer_number_to_maintain_) &&
         !memlist_history_.empty()) {
    MemTable* x = memlist_history_.back();
    memlist_history_.pop_back();

    UnrefMemTable(to_delete, x);
  }
}

void MemTableListVersion::trim_all_history(autovector<MemTable*>* to_delete) {
  while (!memlist_history_.empty()) {
    MemTable* x = memlist_history_.back();
    memlist_history_.pop_back();
    UnrefMemTable(to_delete, x);
  }
}

size_t MemTableListVersion::TrimHistoryOlderThan(
    autovector<MemTable*>* to_delete, SequenceNumber seqno) {
  size_t num = 0;
  while (!memlist_history_.empty()) {
    MemTable* x = memlist_history_.back();
    if (x->GetEarliestSequenceNumber() > seqno) break;

    ++num;
    memlist_history_.pop_back();
    UnrefMemTable(to_delete, x);
  }

  return num;
}

// Returns true if there is at least one memtable on which flush has
// not yet started.
bool MemTableList::IsFlushPending() const {
  if ((flush_requested_ && num_flush_not_started_ >= 1) ||
      (num_flush_not_started_ >= min_write_buffer_number_to_merge_)) {
    assert(imm_flush_needed.load(std::memory_order_relaxed));
    return true;
  }
  return false;
}

void MemTableList::calc_flush_info(bool &delete_trigger, int64_t &size, int64_t &cnt) {
  delete_trigger = false;
  size = 0;
  cnt = 0;
  const auto& memlist = current_->memlist_;
  auto it = memlist.rbegin();
//  uint64_t schema_version = (*it)->get_schema().schema_version;
  for (; it != memlist.rend() /*&& (*it)->get_schema().schema_version == schema_version*/; ++it) {
    MemTable* m = *it;
    if (!m->flush_in_progress_) {
      size += m->data_size_;
      cnt += 1;
    }
    if (!delete_trigger) {
      m->update_delete_trigger();
      delete_trigger = m->delete_triggered();
    }
  }
}

// Returns the memtables that need to be flushed.
void MemTableList::PickMemtablesToFlush(autovector<MemTable*>* ret) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_PICK_MEMTABLES_TO_FLUSH);
  const auto& memlist = current_->memlist_;
  for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
    MemTable* m = *it;
    if (!m->flush_in_progress_) {
      assert(!m->flush_completed_);
      m->update_delete_trigger();
      num_flush_not_started_--;
      if (num_flush_not_started_ == 0) {
        imm_flush_needed.store(false, std::memory_order_release);
      }
      m->flush_in_progress_ = true;  // flushing will start very soon
      ret->push_back(m);
    }
  }
  flush_requested_ = false;  // start-flush request is complete
}

void MemTableList::RollbackMemtableFlush(const autovector<MemTable*>& mems,
                                         uint64_t file_number) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_MEMTABLE_ROLLBACK);
  assert(!mems.empty());

  // If the flush was not successful, then just reset state.
  // Maybe a succeeding attempt to flush will be successful.
  for (MemTable* m : mems) {
    assert(m->flush_in_progress_);
    assert(m->file_number_ == 0);

    m->flush_in_progress_ = false;
    m->flush_completed_ = false;
    //m->edit_.Clear();
    num_flush_not_started_++;
  }
  imm_flush_needed.store(true, std::memory_order_release);
}

/*
// Record a successful flush in the manifest file
Status MemTableList::InstallMemtableFlushResults(
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    const autovector<MemTable*>& mems, VersionSet* vset, InstrumentedMutex* mu,
    uint64_t file_number, autovector<MemTable*>* to_delete,
    Directory* db_directory, LogBuffer* log_buffer, MiniTables &mtables) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS);
  mu->AssertHeld();

  // flush was successful
  for (size_t i = 0; i < mems.size(); ++i) {
    // All the edits are associated with the first memtable of this batch.
    assert(i == 0 || mems[i]->get_change_info() == nullptr);

    mems[i]->flush_completed_ = true;
    mems[i]->file_number_ = file_number;
  }

  // if some other thread is already committing, then return
  Status s;
  if (commit_in_progress_) {
    TEST_SYNC_POINT("MemTableList::InstallMemtableFlushResults:InProgress");
    return s;
  }

  // Only a single thread can be executing this piece of code
  commit_in_progress_ = true;

  // Retry until all completed flushes are committed. New flushes can finish
  // while the current thread is writing manifest where mutex is released.
  while (s.ok()) {
    auto& memlist = current_->memlist_;
    if (memlist.empty() || !memlist.back()->flush_completed_) {
      break;
    }
    // scan all memtables from the earliest, and commit those
    // (in that order) that have finished flushing. Memetables
    // are always committed in the order that they were created.
    uint64_t batch_file_number = 0;
    size_t batch_count = 0;
    autovector<VersionEdit*> edit_list;
    // enumerate from the last (earliest) element to see how many batch finished
    for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
      MemTable* m = *it;
      if (!m->flush_completed_) {
        break;
      }
      if (it == memlist.rbegin() || batch_file_number != m->file_number_) {
        batch_file_number = m->file_number_;
        __SE_LOG(INFO,
                         "[%s] Level-0 commit table #%" PRIu64 " started",
                         cfd->GetName().c_str(), m->file_number_);
        assert(m->get_change_info() != nullptr);
        uint64_t next_file_number = 
        vset->GetColumnFamilySet()->get_extent_space_manager()->get_file_number_generator().load();
        m->edit_.set_change_info(cfd->GetID(), m->get_change_info().get(), next_file_number);     
        edit_list.push_back(&m->edit_);
      }
      batch_count++;
    }

    if (batch_count > 0) {
      // this can release and reacquire the mutex.
      s = vset->LogAndApply(cfd, mutable_cf_options, edit_list, mu,
                            db_directory);
      if (s.ok()) {
        int ret = Status::kOk;
        for (VersionEdit *edit : edit_list) {
          ret = cfd->get_storage_manager()->apply(*edit->get_change_info(), false);
          if (ret != Status::kOk) {
            __SE_LOG(WARN,
                         "[%s] Level-0 commit faild %d", cfd->GetName().c_str(), ret);
            s = Status(static_cast<Status::Code>(ret));
            break;
          }
          // check compaction delete percent 
          //TODO:yuanfeng
          for (auto& extent_meta : edit->get_change_info()->extent_meta_) {
            if (mutable_cf_options.compaction_delete_percent > 0 &&
                extent_meta.num_entries_ * mutable_cf_options.compaction_delete_percent <= 100 * extent_meta.num_deletes_) {
              cfd->set_delete_triggered_compaction(true);
              cfd->set_pending_priority_l0_layer_sequence(file_number);
              SE_LOG(INFO, "Level-0 extent delete triggered ",
                  K(cfd->GetName().c_str()), K(file_number), K(extent_meta));
            }
          }
        }
      }
      // we will be changing the version in the next code path,
      // so we better create a new one, since versions are immutable
      InstallNewVersion();

      // All the later memtables that have the same filenum
      // are part of the same batch. They can be committed now.
      uint64_t mem_id = 1;  // how many memtables have been flushed.
      if (s.ok()) {         // commit new state
        while (batch_count-- > 0) {
          MemTable* m = current_->memlist_.back();
          __SE_LOG(INFO, "[%s] Level-0 commit table #%" PRIu64
                                       ": memtable #%" PRIu64 " done",
                           cfd->GetName().c_str(), m->file_number_, mem_id);
          assert(m->file_number_ > 0);
          current_->Remove(m, to_delete);
          ++mem_id;

          // promt the compaction priority if m is flushed by deletion
          if (m->delete_triggered_) {
            __SE_LOG(INFO, "[%s] Level-0 delete triggered #%"
                PRIu64 ": memtable #%" PRIu64 ", trim all %d memtables",
                cfd->GetName().c_str(), m->file_number_, mem_id,
                current_->memlist_history_.size());
            cfd->set_delete_triggered_compaction(true);
            cfd->set_pending_priority_l0_layer_sequence(m->file_number_);
            current_->trim_all_history(to_delete);
          }
        }
      } else {
        for (auto it = current_->memlist_.rbegin(); batch_count-- > 0; it++) {
          MemTable* m = *it;
          // commit failed. setup state so that we can flush again.
          __SE_LOG(INFO, "Level-0 commit table #%" PRIu64
                                       ": memtable #%" PRIu64 " failed",
                           m->file_number_, mem_id);
          m->flush_completed_ = false;
          m->flush_in_progress_ = false;
          m->edit_.Clear();
          //TODO:yuanfeng
          //m->get_change_info()->clear();
          num_flush_not_started_++;
          m->file_number_ = 0;
          imm_flush_needed.store(true, std::memory_order_release);
          ++mem_id;
        }
      }
    }
  }
  commit_in_progress_ = false;
  return s;
}
*/

// New memtables are inserted at the front of the list.
void MemTableList::Add(MemTable* m, autovector<MemTable*>* to_delete) {
  assert(static_cast<int>(current_->memlist_.size()) >= num_flush_not_started_);
  InstallNewVersion();
  // this method is used to move mutable memtable into an immutable list.
  // since mutable memtable is already refcounted by the DBImpl,
  // and when moving to the imutable list we don't unref it,
  // we don't have to ref the memtable here. we just take over the
  // reference from the DBImpl.
  current_->Add(m, to_delete);
  m->MarkImmutable();
  num_flush_not_started_++;
  if (num_flush_not_started_ == 1) {
    imm_flush_needed.store(true, std::memory_order_release);
  }
}

size_t MemTableList::TrimOlderThan(autovector<MemTable*>* to_delete,
                                   SequenceNumber seqno) {
  if (!current_->memlist_history_.empty() &&
      current_->memlist_history_.back()->GetEarliestSequenceNumber() <= seqno) {
    InstallNewVersion();
    return current_->TrimHistoryOlderThan(to_delete, seqno);
  }
  return 0;
}

// Returns an estimate of the number of bytes of data in use.
size_t MemTableList::ApproximateUnflushedMemTablesMemoryAllocated() {
  size_t total_size = 0;
  for (auto& memtable : current_->memlist_) {
    total_size += memtable->ApproximateMemoryAllocated();
  }
  return total_size;
}
// Returns an estimate of the number of bytes of data in use.
size_t MemTableList::ApproximateUnflushedMemTablesMemoryUsage() {
  size_t total_size = 0;
  for (auto& memtable : current_->memlist_) {
    total_size += memtable->ApproximateMemoryUsage();
  }
  return total_size;
}

size_t MemTableList::ApproximateMemoryUsage() { return current_memory_usage_; }

void MemTableList::InstallNewVersion() {
  if (current_->refs_ == 1) {
    // we're the only one using the version, just keep using it
  } else {
    // somebody else holds the current version, we need to create new one
    MemTableListVersion* version = current_;
//    current_ = new MemTableListVersion(&current_memory_usage_, current_);
    current_ = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, MemTableListVersion, &current_memory_usage_, current_);
    current_->Ref();
    version->Unref();
  }
}

uint64_t MemTableList::GetMinLogContainingPrepSection() {
  uint64_t min_log = UINT64_MAX;

  for (auto& m : current_->memlist_) {
    // this mem has been flushed it no longer
    // needs to hold on the its prep section
    if (m->flush_completed_) {
      continue;
    }
    auto log = std::min(m->GetMinLogContainingPrepSection(), m->get_temp_min_prep_log());
    if (log > 0 && (min_log == 0 || log < min_log)) {
      min_log = log;
    }
  }

  return min_log;
}

uint64_t MemTableList::get_largest_lognumber() {
  uint64_t max_log = UINT64_MAX;
  if (nullptr != current_ && (int64_t)current_->memlist_.size() >= min_write_buffer_number_to_merge_) {
    max_log = current_->memlist_.back()->get_recovery_point().log_file_number_;
  }
  return max_log;
}

int MemTableList::purge_flushed_memtable(const util::autovector<MemTable *> &flushed_memtables,
                                         util::autovector<MemTable *> *to_delete)
{
  int ret = Status::kOk;
  MemTable *memtable = nullptr;

  if (0 >= flushed_memtables.size()) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), "memtable_count", flushed_memtables.size());
  } else {
    InstallNewVersion();
    for (uint64_t i = 0; i < flushed_memtables.size(); ++i) {
      memtable = current_->memlist_.back();
      current_->Remove(memtable, to_delete);
    }
  }
  return ret;
  }
  }
}  // namespace smartengine
