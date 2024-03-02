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

#include "table/format.h"

#include <inttypes.h>
#include <string>

#include "monitoring/query_perf_context.h"
#include "monitoring/statistics.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/xxhash.h"

namespace smartengine {
using namespace common;
using namespace util;
using namespace monitor;

namespace table {

extern const uint64_t kExtentBasedTableMagicNumber;
const uint32_t DefaultStackBufferSize = 5000;

bool ShouldReportDetailedTime(Env* env, Statistics* stats) {
  return env != nullptr && stats != nullptr &&
         stats->stats_level_ > kExceptDetailedTimers;
}

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64Varint64(dst, offset_, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    // reset in case failure after partially decoding
    offset_ = 0;
    size_ = 0;
    return Status::Corruption("bad block handle");
  }
}

// Return a string that contains the copy of handle.
std::string BlockHandle::ToString(bool hex) const {
  std::string handle_str;
  EncodeTo(&handle_str);
  if (hex) {
    return Slice(handle_str).ToString(true);
  } else {
    return handle_str;
  }
}

const BlockHandle BlockHandle::kNullBlockHandle(0, 0);

// legacy footer format:
//    metaindex handle (varint64 offset, varint64 size)
//    index handle     (varint64 offset, varint64 size)
//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength
//    table_magic_number (8 bytes)
// new footer format:
//    checksum (char, 1 byte)
//    metaindex handle (varint64 offset, varint64 size)
//    index handle     (varint64 offset, varint64 size)
//    <padding> to make the total size 2 * BlockHandle::kMaxEncodedLength + 1
//    footer version (4 bytes)
//    table_magic_number (8 bytes)
void Footer::EncodeTo(std::string* dst) const {
  assert(HasInitializedTableMagicNumber());
  assert(kExtentBasedTableMagicNumber == table_magic_number());
  const size_t original_size = dst->size();
  PutFixed32(dst, valid_size_);
  PutFixed64(dst, next_extent_);
  dst->push_back(static_cast<char>(checksum_));
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(original_size + kVersion3EncodedLength - 12);  // Padding
  PutFixed32(dst, version());
  PutFixed32(dst, static_cast<uint32_t>(table_magic_number() & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(table_magic_number() >> 32));
  assert(dst->size() == original_size + kVersion3EncodedLength);
}

Footer::Footer(uint64_t _table_magic_number, uint32_t _version)
    : version_(_version),
      checksum_(kCRC32c),
      table_magic_number_(_table_magic_number) {
}

Footer::Footer(uint64_t _table_magic_number, uint32_t _valid_size,
               uint64_t _next_extent, uint32_t _version)
    : version_(_version),
      valid_size_(_valid_size),
      next_extent_(_next_extent),
      checksum_(kCRC32c),
      table_magic_number_(_table_magic_number) {
}

Status Footer::DecodeFrom(Slice* input) {
  assert(!HasInitializedTableMagicNumber());
  assert(input != nullptr);
  assert(input->size() >= kMinEncodedLength);

  const char* magic_ptr =
      input->data() + input->size() - kMagicNumberLengthByte;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                    (static_cast<uint64_t>(magic_lo)));

  set_table_magic_number(magic);

  if (magic == table::kExtentBasedTableMagicNumber) {
    version_ = DecodeFixed32(magic_ptr - 4);
    if (input->size() < kVersion3EncodedLength) {
      return Status::Corruption("input is too short to be an extent sstable");
    } else {
      input->remove_prefix(input->size() - kVersion3EncodedLength);
    }
    GetFixed32(input, &valid_size_);
    GetFixed64(input, &next_extent_);
    uint32_t chksum;
    if (!GetVarint32(input, &chksum)) {
      return Status::Corruption("bad checksum type");
    }
    checksum_ = static_cast<ChecksumType>(chksum);
  } else {
    abort();
    return Status::Corruption("unknown magic");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + kMagicNumberLengthByte;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

std::string Footer::ToString() const {
  std::string result;
  result.reserve(1024);

  result.append("checksum: " + util::ToString(checksum_) + "\n  ");
  result.append("metaindex handle: " + metaindex_handle_.ToString() + " [" +
                std::to_string(metaindex_handle_.offset()) + ", " +
                std::to_string(metaindex_handle_.size()) + "]\n  ");
  result.append("index handle: " + index_handle_.ToString() + " [" +
                std::to_string(index_handle_.offset()) + ", " +
                std::to_string(index_handle_.size()) + "]\n  ");
  result.append("footer version: " + util::ToString(version_) + "\n  ");
  result.append("table_magic_number: " + util::ToString(table_magic_number_) +
                "\n  ");
  return result;
}

Status ReadFooterFromFile(RandomAccessFileReader* file, uint64_t file_size,
                          Footer* footer, uint64_t enforce_table_magic_number) {
  if (file_size < Footer::kMinEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kMaxEncodedLength];
  Slice footer_input;
  // the footer is always at the end of Extent
  size_t read_offset =
      static_cast<size_t>(storage::MAX_EXTENT_SIZE - Footer::kMaxEncodedLength);
  Status s = file->Read(read_offset, Footer::kMaxEncodedLength, &footer_input,
                        footer_space);
  if (!s.ok()) return s;

  // Check that we actually read the whole footer from the file. It may be
  // that size isn't correct.
  if (footer_input.size() < Footer::kMinEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  s = footer->DecodeFrom(&footer_input);
  if (!s.ok()) {
    return s;
  }
  if (enforce_table_magic_number != 0 &&
      enforce_table_magic_number != footer->table_magic_number()) {
    return Status::Corruption("Bad table magic number");
  }
  return Status::OK();
}

// Read a block and check its CRC
// contents is the result of reading.
// According to the implementation of file->Read, contents may not point to buf
Status ReadBlock(RandomAccessFileReader* file, const Footer& footer,
                 const ReadOptions& options, const BlockHandle& handle,
                 Slice* contents, /* result of reading */ char* buf,
                 AIOHandle *aio_handle) {
  size_t n = static_cast<size_t>(handle.size());
  Status s;

  s = file->read(handle.offset(), n + kBlockTrailerSize, contents, buf, aio_handle);

  if (!s.ok()) {
    return s;
  }
  if (contents->size() != n + kBlockTrailerSize) {
    return Status::Corruption("truncated block read");
  }

  // Check the crc of the type and the block contents
  const char* data = contents->data();  // Pointer to where Read put the data
  if (options.verify_checksums) {
    uint32_t value = DecodeFixed32(data + n + 1);
    uint32_t actual = 0;
    switch (footer.checksum()) {
      case kCRC32c:
        value = crc32c::Unmask(value);
        actual = crc32c::Value(data, n + 1);
        break;
      case kxxHash:
        actual = XXH32(data, static_cast<int>(n) + 1, 0);
        break;
      default:
        s = Status::Corruption("unknown checksum type");
    }
    if (s.ok() && actual != value) {
      s = Status::Corruption("block checksum mismatch");
    }
    if (!s.ok()) {
      return s;
    }
  }
  return s;
}

Status ReadBlockContents(RandomAccessFileReader* file,
                         const Footer& footer,
                         const ReadOptions& read_options,
                         const BlockHandle& handle,
                         BlockContents* contents,
                         const ImmutableCFOptions& ioptions,
                         bool decompression_requested,
                         const Slice& compression_dict,
                         AIOHandle *aio_handle) {
  Status status;
  Slice slice;
  size_t n = static_cast<size_t>(handle.size());
  std::unique_ptr<char[], memory::ptr_delete<char>> heap_buf;
  char stack_buf[DefaultStackBufferSize];
  char* used_buf = nullptr;
  CompressionType compression_type;

    // cache miss read from device
  if (decompression_requested &&
      n + kBlockTrailerSize < DefaultStackBufferSize) {
    // If we've got a small enough hunk of data, read it in to the
    // trivially allocated stack buffer instead of needing a full malloc()
    used_buf = &stack_buf[0];
  } else {
    char* obj_ptr = static_cast<char*>(
        base_malloc(n + kBlockTrailerSize, memory::ModId::kPersistentCache));
    if (nullptr == obj_ptr) {
      return Status::MemoryLimit();
    } else {
      heap_buf.reset(obj_ptr);
      used_buf = heap_buf.get();
    }
  }
  status = ReadBlock(file, footer, read_options, handle, &slice, used_buf, aio_handle);

  if (!status.ok()) {
    return status;
  }

  compression_type = static_cast<CompressionType>(slice.data()[n]);

  if (decompression_requested && compression_type != kNoCompression) {
    // compressed page, uncompress, update cache
    status =
        UncompressBlockContents(slice.data(), n, contents, footer.version(),
                                compression_dict, ioptions);
  } else if (slice.data() != used_buf) {
    // the slice content is not the buffer provided
    *contents = BlockContents(Slice(slice.data(), n), false, compression_type);
  } else {
    // page is uncompressed, the buffer either stack or heap provided
    if (used_buf == &stack_buf[0]) {
      char* buf_obj =
          static_cast<char*>(base_malloc(n, memory::ModId::kPersistentCache));
      if (nullptr != buf_obj) {
        heap_buf.reset(buf_obj);
        memcpy(heap_buf.get(), stack_buf, n);
      } else {
        return Status::MemoryLimit();
      }
    }
    *contents = BlockContents(std::move(heap_buf), n, true, compression_type);
  }


  return status;
}

Status UncompressBlockContentsForCompressionType(
    const char* data, size_t n, BlockContents* contents,
    uint32_t format_version, const Slice& compression_dict,
    CompressionType compression_type) {
  std::unique_ptr<char[], memory::ptr_delete<char>> ubuf;

  assert(compression_type != kNoCompression && "Invalid compression type");

  int decompress_size = 0;
  switch (compression_type) {
    case kSnappyCompression: {
      size_t ulength = 0;
      static char snappy_corrupt_msg[] =
          "Snappy not supported or corrupted Snappy compressed block contents";
      if (!Snappy_GetUncompressedLength(data, n, &ulength)) {
        return Status::Corruption(snappy_corrupt_msg);
      }
      char* obj_ptr = static_cast<char*>(memory::base_malloc(ulength));
      if (nullptr == obj_ptr) {
        return Status::MemoryLimit();
      } else {
        ubuf.reset(obj_ptr);
        if (!Snappy_Uncompress(data, n, ubuf.get())) {
          return Status::Corruption(snappy_corrupt_msg);
        }
        *contents =
            BlockContents(std::move(ubuf), ulength, true, kNoCompression);
      }
      break;
    }
    case kZlibCompression:
      ubuf.reset(Zlib_Uncompress(
          data, n, &decompress_size,
          GetCompressFormatForVersion(kZlibCompression, format_version),
          compression_dict));
      if (!ubuf) {
        static char zlib_corrupt_msg[] =
            "Zlib not supported or corrupted Zlib compressed block contents";
        return Status::Corruption(zlib_corrupt_msg);
      }
      *contents =
          BlockContents(std::move(ubuf), decompress_size, true, kNoCompression);
      break;
    case kBZip2Compression:
      ubuf.reset(BZip2_Uncompress(
          data, n, &decompress_size,
          GetCompressFormatForVersion(kBZip2Compression, format_version)));
      if (!ubuf) {
        static char bzip2_corrupt_msg[] =
            "Bzip2 not supported or corrupted Bzip2 compressed block contents";
        return Status::Corruption(bzip2_corrupt_msg);
      }
      *contents =
          BlockContents(std::move(ubuf), decompress_size, true, kNoCompression);
      break;
    case kLZ4Compression:
      ubuf.reset(LZ4_Uncompress(
          data, n, &decompress_size,
          GetCompressFormatForVersion(kLZ4Compression, format_version),
          compression_dict));
      if (!ubuf) {
        static char lz4_corrupt_msg[] =
            "LZ4 not supported or corrupted LZ4 compressed block contents";
        return Status::Corruption(lz4_corrupt_msg);
      }
      *contents =
          BlockContents(std::move(ubuf), decompress_size, true, kNoCompression);
      break;
    case kLZ4HCCompression:
      ubuf.reset(LZ4_Uncompress(
          data, n, &decompress_size,
          GetCompressFormatForVersion(kLZ4HCCompression, format_version),
          compression_dict));
      if (!ubuf) {
        static char lz4hc_corrupt_msg[] =
            "LZ4HC not supported or corrupted LZ4HC compressed block contents";
        return Status::Corruption(lz4hc_corrupt_msg);
      }
      *contents =
          BlockContents(std::move(ubuf), decompress_size, true, kNoCompression);
      break;
    case kXpressCompression:
      ubuf.reset(XPRESS_Uncompress(data, n, &decompress_size));
      if (!ubuf) {
        static char xpress_corrupt_msg[] =
            "XPRESS not supported or corrupted XPRESS compressed block "
            "contents";
        return Status::Corruption(xpress_corrupt_msg);
      }
      *contents =
          BlockContents(std::move(ubuf), decompress_size, true, kNoCompression);
      break;
    case kZSTD:
    case kZSTDNotFinalCompression:
      ubuf.reset(ZSTD_Uncompress(data, n, &decompress_size, compression_dict));
      if (!ubuf) {
        static char zstd_corrupt_msg[] =
            "ZSTD not supported or corrupted ZSTD compressed block contents";
        return Status::Corruption(zstd_corrupt_msg);
      }
      *contents =
          BlockContents(std::move(ubuf), decompress_size, true, kNoCompression);
      break;
    default:
      return Status::Corruption("bad block type");
  }

  QUERY_COUNT_ADD(CountPoint::BYTES_DECOMPRESSED, contents->data.size());
  QUERY_COUNT(CountPoint::NUMBER_BLOCK_DECOMPRESSED);
  return Status::OK();
}

//
// The 'data' points to the raw block contents that was read in from file.
// This method allocates a new heap buffer and the raw block
// contents are uncompresed into this buffer. This
// buffer is returned via 'result' and it is upto the caller to
// free this buffer.
// format_version is the block format as defined in include/smartengine/table.h
Status UncompressBlockContents(const char* data, size_t n,
                               BlockContents* contents, uint32_t format_version,
                               const Slice& compression_dict,
                               const ImmutableCFOptions& ioptions) {
  QUERY_TRACE_SCOPE(TracePoint::DECOMPRESS_BLOCK);
  assert(data[n] != kNoCompression);
  return UncompressBlockContentsForCompressionType(
      data, n, contents, format_version, compression_dict,
      (CompressionType)data[n]);
}

// just a data blob
int unzip_data(const char* data, size_t n,
               uint32_t format_version, CompressionType compression_type,
               std::unique_ptr<char[], memory::ptr_delete<char>>& unzip_buf,
               size_t& unzip_buf_size) {
  QUERY_TRACE_SCOPE(TracePoint::DECOMPRESS_BLOCK);
  BlockContents contents;
  int ret = UncompressBlockContentsForCompressionType(
              data, n, &contents, format_version, "", compression_type).code();
  if (Status::kOk != ret) {
    __SE_LOG(ERROR, "cannot unzip for large object");
    return ret;
  }
  unzip_buf = std::move(contents.allocation);
  unzip_buf_size = contents.data.size_;
  return Status::kOk;
}
}  // namespace table
}  // namespace smartengine
