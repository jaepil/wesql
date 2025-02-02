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
//
// Endian-neutral encoding:
// * Fixed-length numbers are encoded with least-significant byte first
// * In addition we support variable length "varint" encoding
// * Strings are encoded prefixed by their length in varint format

#pragma once
#include <stdint.h>
#include <string>

#include "port/port_posix.h"
#include "write_batch/write_batch.h"

namespace smartengine {
namespace util {

// The maximum length of a varint in bytes for 64-bit.
const unsigned int kMaxVarint64Length = 10;

// Standard Put... routines append to a string-like
template <typename T>
extern void PutFixed32(T* dst, uint32_t value);
template <typename T>
extern void PutVarint32(T* dst, uint32_t value);

extern void PutFixed64(std::string* dst, uint64_t value);
extern void PutVarint32Varint32(std::string* dst, uint32_t value1,
                                uint32_t value2);

template <typename T>
extern void PutVarint32Varint32Varint32(T* dst, uint32_t value1,
                                        uint32_t value2, uint32_t value3);

template <typename T>
extern void PutVarint64(T* dst, uint64_t value);
extern void PutVarint64Varint64(std::string* dst, uint64_t value1,
                                uint64_t value2);
extern void PutVarint32Varint64(std::string* dst, uint32_t value1,
                                uint64_t value2);
extern void PutVarint32Varint32Varint64(std::string* dst, uint32_t value1,
                                        uint32_t value2, uint64_t value3);
extern void PutLengthPrefixedSlice(std::string* dst,
                                   const common::Slice& value);
extern void PutLengthPrefixedSlice(memory::xstring* dst,
                                   const common::Slice& value);
extern void PutLengthPrefixedSliceParts(std::string* dst,
                                        const common::SliceParts& slice_parts);

extern void PutLengthPrefixedSliceParts(memory::xstring* dst,
                                        const common::SliceParts& slice_parts);

// Standard Get... routines parse a value from the beginning of a common::Slice
// and advance the slice past the parsed value.
extern bool GetFixed64(common::Slice* input, uint64_t* value);
extern bool GetFixed32(common::Slice* input, uint32_t* value);
extern bool GetVarint32(common::Slice* input, uint32_t* value);
extern bool GetVarint64(common::Slice* input, uint64_t* value);
extern bool GetLengthPrefixedSlice(common::Slice* input, common::Slice* result);
extern bool GetLengthPrefixedString(common::Slice* input, std::string& result);
// This function assumes data is well-formed.
extern common::Slice GetLengthPrefixedSlice(const char* data);

extern common::Slice GetSliceUntil(common::Slice* slice, char delimiter);

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// nullptr on error.  These routines only look at bytes in the range
// [p..limit-1]
extern const char* GetVarint32Ptr(const char* p, const char* limit,
                                  uint32_t* v);
extern const char* GetVarint64Ptr(const char* p, const char* limit,
                                  uint64_t* v);

// Returns the length of the varint32 or varint64 encoding of "v"
extern int VarintLength(uint64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// REQUIRES: dst has enough space for the value being written
extern void EncodeFixed32(char* dst, uint32_t value);
extern void EncodeFixed64(char* dst, uint64_t value);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
extern char* EncodeVarint32(char* dst, uint32_t value);
extern char* EncodeVarint64(char* dst, uint64_t value);

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

inline uint32_t DecodeFixed32(const char* ptr) {
  if (port::kLittleEndian) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0]))) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
  }
}

inline uint64_t DecodeFixed64(const char* ptr) {
  if (port::kLittleEndian) {
    // Load the raw bytes
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    uint64_t lo = DecodeFixed32(ptr);
    uint64_t hi = DecodeFixed32(ptr + 4);
    return (hi << 32) | lo;
  }
}

// Internal routine for use by fallback path of GetVarint32Ptr
extern const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                          uint32_t* value);
inline const char* GetVarint32Ptr(const char* p, const char* limit,
                                  uint32_t* value) {
  if (p < limit) {
    uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}

// -- Implementation of the functions declared above
inline void EncodeFixed32(char* buf, uint32_t value) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  memcpy(buf, &value, sizeof(value));
#else
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
#endif
}

inline void EncodeFixed64(char* buf, uint64_t value) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  memcpy(buf, &value, sizeof(value));
#else
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
  buf[4] = (value >> 32) & 0xff;
  buf[5] = (value >> 40) & 0xff;
  buf[6] = (value >> 48) & 0xff;
  buf[7] = (value >> 56) & 0xff;
#endif
}

// Pull the last 8 bits and cast it to a character
template <typename T>
inline void PutFixed32(T* dst, uint32_t value) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)),
              sizeof(value));
#else
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
#endif
}

inline void PutFixed64(std::string* dst, uint64_t value) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)),
              sizeof(value));
#else
  char buf[sizeof(value)];
  EncodeFixed64(buf, value);
  dst->append(buf, sizeof(buf));
#endif
}

template <typename T>
inline void PutVarint32(T* dst, uint32_t v) {
  char buf[5];
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint32Varint32(std::string* dst, uint32_t v1, uint32_t v2) {
  char buf[10];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

template <typename T>
inline void PutVarint32Varint32Varint32(T* dst, uint32_t v1, uint32_t v2,
                                        uint32_t v3) {
  char buf[15];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  ptr = EncodeVarint32(ptr, v3);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline char* EncodeVarint64(char* dst, uint64_t v) {
  static const unsigned int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B - 1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

template <typename T>
inline void PutVarint64(T* dst, uint64_t v) {
  char buf[10];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint64Varint64(std::string* dst, uint64_t v1, uint64_t v2) {
  char buf[20];
  char* ptr = EncodeVarint64(buf, v1);
  ptr = EncodeVarint64(ptr, v2);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}


inline void PutVarint32Varint64(std::string* dst, uint32_t v1, uint64_t v2) {
  char buf[15];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint64(ptr, v2);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint32Varint32Varint64(std::string* dst, uint32_t v1,
                                        uint32_t v2, uint64_t v3) {
  char buf[20];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  ptr = EncodeVarint64(ptr, v3);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutLengthPrefixedSlice(memory::xstring* dst,
                                   const common::Slice& value) {
  PutVarint32(dst, static_cast<uint32_t>(value.size()));
  dst->append(value.data(), value.size());
}

inline void PutLengthPrefixedSlice(std::string* dst,
                                   const common::Slice& value) {
  PutVarint32(dst, static_cast<uint32_t>(value.size()));
  dst->append(value.data(), value.size());
}

inline void PutLengthPrefixedSliceParts(memory::xstring* dst,
                                        const common::SliceParts& slice_parts) {
  size_t total_bytes = 0;
  for (int i = 0; i < slice_parts.num_parts; ++i) {
    total_bytes += slice_parts.parts[i].size();
  }
  PutVarint32(dst, static_cast<uint32_t>(total_bytes));
  for (int i = 0; i < slice_parts.num_parts; ++i) {
    dst->append(slice_parts.parts[i].data(), slice_parts.parts[i].size());
  }
}

inline void PutLengthPrefixedSliceParts(std::string* dst,
                                        const common::SliceParts& slice_parts) {
  size_t total_bytes = 0;
  for (int i = 0; i < slice_parts.num_parts; ++i) {
    total_bytes += slice_parts.parts[i].size();
  }
  PutVarint32(dst, static_cast<uint32_t>(total_bytes));
  for (int i = 0; i < slice_parts.num_parts; ++i) {
    dst->append(slice_parts.parts[i].data(), slice_parts.parts[i].size());
  }
}

inline int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

inline bool GetFixed64(common::Slice* input, uint64_t* value) {
  if (input->size() < sizeof(uint64_t)) {
    return false;
  }
  *value = DecodeFixed64(input->data());
  input->remove_prefix(sizeof(uint64_t));
  return true;
}

inline bool GetFixed32(common::Slice* input, uint32_t* value) {
  if (input->size() < sizeof(uint32_t)) {
    return false;
  }
  *value = DecodeFixed32(input->data());
  input->remove_prefix(sizeof(uint32_t));
  return true;
}

inline bool GetVarint32(common::Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = common::Slice(q, static_cast<size_t>(limit - q));
    return true;
  }
}

inline bool GetVarint64(common::Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = common::Slice(q, static_cast<size_t>(limit - q));
    return true;
  }
}

// Provide an interface for platform independent endianness transformation
inline uint64_t EndianTransform(uint64_t input, size_t size) {
  char* pos = reinterpret_cast<char*>(&input);
  uint64_t ret_val = 0;
  for (size_t i = 0; i < size; ++i) {
    ret_val |= (static_cast<uint64_t>(static_cast<unsigned char>(pos[i]))
                << ((size - i - 1) << 3));
  }
  return ret_val;
}

inline bool GetLengthPrefixedString(common::Slice* input, std::string& result) {
  uint32_t len = 0;
  if (GetVarint32(input, &len) && input->size() >= len) {
    result.assign(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

inline bool GetLengthPrefixedSlice(common::Slice* input,
                                   common::Slice* result) {
  uint32_t len = 0;
  if (GetVarint32(input, &len) && input->size() >= len) {
    *result = common::Slice(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

inline common::Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len = 0;
  // +5: we assume "data" is not corrupted
  // unsigned char is 7 bits, uint32_t is 32 bits, need 5 unsigned char
  auto p = GetVarint32Ptr(data, data + 5 /* limit */, &len);
  return common::Slice(p, len);
}

inline common::Slice GetSliceUntil(common::Slice* slice, char delimiter) {
  uint32_t len = 0;
  for (len = 0; len < slice->size() && slice->data()[len] != delimiter; ++len) {
    // nothing
  }

  common::Slice ret(slice->data(), len);
  slice->remove_prefix(len + ((len < slice->size()) ? 1 : 0));
  return ret;
}

}  // namespace util
}  // namespace smartengine
