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
#include "db/dbformat.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <stdio.h>
#include "monitoring/query_perf_context.h"
#include "port/port.h"
#include "storage/extent_space_manager.h"
#include "util/coding.h"
#include "util/serialization.h"
#include "util/string_util.h"

namespace smartengine
{
using namespace common;
using namespace monitor;
using namespace storage;
using namespace util;

namespace db
{

// TODO(Zhao Dongsheng): the code related to large object move to specific file
int64_t LargeValue::get_serialize_size() const {
  int64_t ret = util::get_serialize_size(version_, compression_type_, size_) +
                util::get_serialize_v_size(oob_extents_);
  return ret;
}

int LargeValue::serialize(char *buffer, int64_t bufsiz, int64_t &pos) const {
  int ret = util::serialize(buffer, bufsiz, pos, version_, compression_type_, size_) ||
            util::serialize_v(buffer, bufsiz, pos, oob_extents_);
  return ret;
}

int LargeValue::deserialize(const char *buffer, int64_t bufsiz, int64_t &pos) {
  oob_extents_.clear();
  int ret = util::deserialize(buffer, bufsiz, pos, version_, compression_type_, size_) ||
            util::deserialize_v(buffer, bufsiz, pos, oob_extents_);
  return ret;
}

DEFINE_SERIALIZATION(LargeObject, key_, value_);

int get_oob_large_value(const Slice &value_in_kv,
                        db::LargeValue &large_value,
                        std::unique_ptr<char[], void(&)(void *)> &oob_uptr,
                        int64_t &oob_size)
{
  int ret = Status::kOk;
  Slice result;
  int64_t result_size = 0;
  int64_t pos = 0;
  char *current_read_buf = nullptr;
  storage::RandomAccessExtent extent;

  if (FAILED(large_value.deserialize(value_in_kv.data(), value_in_kv.size(), pos))) {
    SE_LOG(WARN, "fail to deserialize large value", K(ret));
  } else {
    result_size = large_value.oob_extents_.size() * storage::MAX_EXTENT_SIZE;
    if (result_size > oob_size) {
      oob_uptr.reset(reinterpret_cast<char *>(base_memalign(4096, result_size, memory::ModId::kLargeObject)));
      if (IS_NULL(oob_uptr.get())) {
        oob_size = 0;
        ret = Status::kMemoryLimit;
        SE_LOG(WARN, "fail to allocate memory for large value", K(ret), K(result_size));
      } else {
        oob_size = result_size;
      }
    }

    if (SUCCED(ret)) {
      for (uint32_t i = 0; SUCCED(ret) && i < large_value.oob_extents_.size(); ++i) {
        current_read_buf = oob_uptr.get() + i * storage::MAX_EXTENT_SIZE;
        if (FAILED(ExtentSpaceManager::get_instance().get_random_access_extent(large_value.oob_extents_[i], extent).code())) {
          SE_LOG(WARN, "fail to get random access extent for large object", K(ret), "extent_id", large_value.oob_extents_[i]);
        } else if (FAILED(extent.Read(0, storage::MAX_EXTENT_SIZE, &result, current_read_buf).code())) {
          SE_LOG(WARN, "fail to read data for large object", K(ret));
        }
      }
    }
  }

  return ret; 
}

const int32_t BlockStats::LATEST_VERSION = 0;

DEFINE_SERIALIZATION(BlockStats, version_, data_size_, key_size_, value_size_,
                     rows_, actual_disk_size_, entry_put_, entry_deletes_,
                     entry_single_deletes_, entry_merges_, entry_others_,
                     smallest_seqno_, largest_seqno_, first_key_);

DEFINE_TO_STRING(BlockStats, KV_(version), KV_(data_size), KV_(key_size),
                 KV_(value_size), KV_(rows), KV_(actual_disk_size),
                 KV_(entry_put), KV_(entry_deletes), KV_(entry_single_deletes),
                 KV_(entry_merges), KV_(entry_others), KV_(smallest_seqno),
                 KV_(largest_seqno));

BlockStats::BlockStats()
    : version_(BlockStats::LATEST_VERSION),
      data_size_(0),
      key_size_(0),
      value_size_(0),
      rows_(0),
      actual_disk_size_(0),
      entry_put_(0),
      entry_deletes_(0),
      entry_single_deletes_(0),
      entry_merges_(0),
      entry_others_(0),
      smallest_seqno_(kMaxSequenceNumber),
      largest_seqno_(0) {}

int BlockStats::decode(const Slice& index_entry_slice) {
  int64_t end_pos = 0;
  int ret =
      deserialize(index_entry_slice.data(), index_entry_slice.size(), end_pos);
  if (ret != Status::kOk) return Status::kCorruption;
  return Status::kOk;
}

int BlockStats::decode(const Slice& index_entry_slice,
                       Slice& first_key,
                       int64_t &delete_percent) {
  int64_t end_pos = 0;
  int32_t version = 0;
  int64_t data_size = 0;
  int64_t key_size = 0;
  int64_t value_size = 0;
  int64_t rows = 0;
  int64_t actual_disk_size = 0;
  int64_t entry_put = 0;
  int64_t entry_deletes = 0;
  int64_t entry_single_deletes = 0;
  int64_t entry_merges = 0;
  int64_t entry_others = 0;
  SequenceNumber smallest_seqno;
  SequenceNumber largest_seqno;
  int ret = util::deserialize(
      index_entry_slice.data(), index_entry_slice.size(), end_pos, version,
      data_size, key_size, value_size, rows, actual_disk_size, entry_put,
      entry_deletes, entry_single_deletes, entry_merges, entry_others,
      smallest_seqno, largest_seqno, first_key);
  if (rows > 0) {
    delete_percent = (entry_deletes + entry_single_deletes) * 100 / rows;
  }
  return ret;
}

std::string BlockStats::encode() const {
  int64_t sz = get_serialize_size();
  std::string block_stats_encoding;
  block_stats_encoding.resize(sz);
  int64_t end_pos = 0;
  serialize(&block_stats_encoding[0], sz, end_pos);
  return block_stats_encoding;
}

void BlockStats::reset() {
  version_ = BlockStats::LATEST_VERSION;
  data_size_ = 0;
  key_size_ = 0;
  value_size_ = 0;
  rows_ = 0;
  actual_disk_size_ = 0;
  entry_put_ = 0;
  entry_deletes_ = 0;
  entry_single_deletes_ = 0;
  entry_merges_ = 0;
  entry_others_ = 0;
  smallest_seqno_ = kMaxSequenceNumber;
  largest_seqno_ = 0;
  first_key_.clear();
  distinct_keys_per_prefix_.clear();
}

bool BlockStats::equal(const BlockStats& block_stats) const {
  return version_ == block_stats.version_ &&
         data_size_ == block_stats.data_size_ &&
         key_size_ == block_stats.key_size_ &&
         value_size_ == block_stats.value_size_ && rows_ == block_stats.rows_ &&
         actual_disk_size_ == block_stats.actual_disk_size_ &&
         entry_put_ == block_stats.entry_put_ &&
         entry_deletes_ == block_stats.entry_deletes_ &&
         entry_single_deletes_ == block_stats.entry_single_deletes_ &&
         entry_merges_ == block_stats.entry_merges_ &&
         entry_others_ == block_stats.entry_others_ &&
         first_key_ == block_stats.first_key_ &&
         distinct_keys_per_prefix_ == block_stats.distinct_keys_per_prefix_;
}

int BlockStats::estimate_size() const {
  return sizeof(BlockStats) + first_key_.size();
}

// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
const ValueType kValueTypeForSeek = kTypeValueLarge;
const ValueType kValueTypeForSeekForPrev = kTypeDeletion;

uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(IsValueType(t));
  return (seq << 8) | t;
}

void UnPackSequenceAndType(uint64_t packed, uint64_t* seq, ValueType* t) {
  *seq = packed >> 8;
  *t = static_cast<ValueType>(packed & 0xff);

  assert(*seq <= kMaxSequenceNumber);
  assert(IsValueType(*t));
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

// This function is used for compatibility when doing iter next.
void AppendInternalKeyForNext(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());
  result->append(1, 0);
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

void AppendInternalKeyFooter(std::string* result, SequenceNumber s,
                             ValueType t) {
  PutFixed64(result, PackSequenceAndType(s, t));
}

int64_t internal_key_to_string(const common::Slice& internal_key, char *buf, int64_t buf_len) {
  int64_t pos = 0;
  assert(internal_key.size() >= 8);
  const size_t n = internal_key.size();
  uint64_t num = util::DecodeFixed64(internal_key.data() + n - 8);
  int64_t sequence = num >> 8;
  int64_t c = num & 0xff;
  util::databuff_printf(buf, buf_len, pos, "{user_key:");
  pos += ExtractUserKey(internal_key).to_string(buf + pos, buf_len - pos);
  util::databuff_printf(buf, buf_len, pos, "} {seq:%ld} {type:%ld}", sequence, c);
  return pos;
}

std::string ParsedInternalKey::DebugString(bool hex) const {
  char buf[50];
  snprintf(buf, sizeof(buf), "' seq:%" PRIu64 ", type:%d", sequence,
           static_cast<int>(type));
  std::string result = "'";
  result += user_key.ToString(hex);
  result += buf;
  return result;
}

DEFINE_TO_STRING(ParsedInternalKey, KV(user_key), KV(sequence));

std::string InternalKey::DebugString(bool hex) const {
  std::string result;
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {
    result = parsed.DebugString(hex);
  } else {
    result = "(bad)";
    result.append(EscapeString(rep_));
  }
  return result;
}

DEFINE_SERIALIZATION(InternalKey, rep_);

const char* InternalKeyComparator::Name() const {
  if (user_comparator_ != nullptr) {
    return user_comparator_->Name();
  } else {
    return "";
  }
}

int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

int InternalKeyComparator::Compare(const ParsedInternalKey& a,
                                   const ParsedInternalKey& b) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  QUERY_COUNT(CountPoint::USER_KEY_COMPARE);
  int r = user_comparator_->Compare(a.user_key, b.user_key);
  if (r == 0) {
    if (a.sequence > b.sequence) {
      r = -1;
    } else if (a.sequence < b.sequence) {
      r = +1;
    } else if (a.type > b.type) {
      r = -1;
    } else if (a.type < b.type) {
      r = +1;
    }
  }
  return r;
}

void InternalKeyComparator::FindShortestSeparator(std::string* start,
                                                  const Slice& limit) const {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() < user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() < user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

LookupKey::LookupKey(const Slice& _user_key, SequenceNumber s) {
  size_t usize = _user_key.size();
  size_t needed = usize + 13;  // A conservative estimate
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
//    dst = new char[needed];
    dst = static_cast<char *>(memory::base_malloc(needed, memory::ModId::kLookupKey));
  }
  start_ = dst;
  // NOTE: We don't support users keys of more than 2GB :)
  dst = EncodeVarint32(dst, static_cast<uint32_t>(usize + 8));
  kstart_ = dst;
  memcpy(dst, _user_key.data(), usize);
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
  this->bloom_hash_set_ = false;
}

DEFINE_TO_STRING(FileMetaData, KV(smallest), KV(largest), KV(smallest_seqno),
                 KV(largest_seqno), KV(fd.extent_id.offset),
                 KV(fd.extent_id.file_number), KV(fd.file_size),
                 KV(compensated_file_size), KV(num_entries), KV(num_deletions),
                 KV(raw_key_size), KV(raw_value_size), KV(fd.extent_id.offset),
                 KV(fd.extent_id.file_number));

}
}  // namespace smartengine
