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

DEFINE_TO_STRING(ParsedInternalKey, KV(user_key), KV(sequence))

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

DEFINE_SERIALIZATION(InternalKey, rep_)

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

DEFINE_TO_STRING(FileMetaData, KV_(extent_id), KV_(data_size), KV(smallest),
                 KV(largest), KV(smallest_seqno), KV(largest_seqno),
                 KV_(extent_id), KV(num_entries), KV(num_deletions),
                 KV(raw_key_size), KV(raw_value_size))

}
}  // namespace smartengine
