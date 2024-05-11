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

#include "util/compress/compressor_helper.h"
#include "logger/log_module.h"
#include "memory/base_malloc.h"
#include "options/advanced_options.h"
#include "options/options.h"
#include "util/compress/compressor_factory.h"
#include "util/compress/compressor.h"
#include "util/se_constants.h"
#include "util/status.h"

namespace smartengine
{
using namespace common;
using namespace memory;

namespace util
{

int CompressHelper::compress(const Slice &raw_data,
                             Slice &compressed_data,
                             CompressionType &compress_type)
{
  int ret = Status::kOk;
  char *compressed_buf = nullptr;
  int64_t compressed_buf_size = 0;

  if (kNoCompression == compress_type) {
    compressed_data.assign(raw_data.data(), raw_data.size());
  } else {
    if (FAILED(compress_internal(raw_data.data(),
                                 raw_data.size(),
                                 compressed_buf,
                                 compressed_buf_size,
                                 compress_type))) {
      SE_LOG(WARN, "fail to compress", K(ret));
    } else {
      compressed_data.assign(compressed_buf, compressed_buf_size);
    }
  }

  return ret;
}

int CompressHelper::compress_internal(const char *raw_data,
                                      const int64_t raw_data_size,
                                      char *&compressed_data,
                                      int64_t &compressed_data_size,
                                      CompressionType &compress_type)
{
  int ret = Status::kOk;
  bool not_compress = false;
  int64_t max_compress_size = 0;
  int64_t compressed_size = 0;
  Compressor *compressor = nullptr;

  compress_buf_.reuse();
  if (IS_NULL(raw_data) || (raw_data_size <= 0)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "Invalid argument", K(ret), KP(raw_data), K(raw_data_size));
  } else if (FAILED(CompressorFactory::get_instance().get_compressor(compress_type, compressor))) {
    SE_LOG(WARN, "fail to get compressor", K(ret), KE(compress_type));
  } else if (FAILED(compressor->get_max_compress_size(raw_data_size, max_compress_size))) {
    SE_LOG(WARN, "Failed to get max compress size", K(ret), K(raw_data_size));
  } else if (FAILED(compress_buf_.reserve(max_compress_size))) {
    SE_LOG(WARN, "Failed to reserve compress buffer", K(ret), K(max_compress_size));
  } else if (FAILED(compressor->compress(raw_data,
                                         raw_data_size,
                                         compress_buf_.data(),
                                         compress_buf_.capacity(),
                                         compressed_size))) {
    if (Status::kNotCompress == ret) {
      not_compress = true;
      //override ret
      ret = Status::kOk;
    } else {
      SE_LOG(WARN, "Failed to compress data", K(ret));
    }
  } else if (compressed_size >= raw_data_size) {
    not_compress = true;
  } else {
    compressed_data = compress_buf_.data();
    compressed_data_size = compressed_size;
    compress_type = compressor->get_compress_type();
  }

  if (SUCCED(ret) && not_compress) {
    compressed_data = const_cast<char*>(raw_data);
    compressed_data_size = raw_data_size;
    compress_type = kNoCompression;
  }

  return ret;
}

int UncompressHelper::uncompress(const Slice &compresed_data,
                                 const CompressionType compress_type,
                                 const int64_t mod_id,
                                 const int64_t raw_data_size,
                                 Slice &raw_data)
{
  int ret = Status::kOk;
  char *uncompressed_data = nullptr;

  if (kNoCompression == compress_type) {
    raw_data.assign(compresed_data.data(), compresed_data.size());
  } else if (FAILED(uncompress_internal(compresed_data.data(),
                                        compresed_data.size(),
                                        compress_type,
                                        mod_id,
                                        raw_data_size,
                                        uncompressed_data))) {
    SE_LOG(WARN, "fail to internal uncompress data", K(ret));
  } else {
    raw_data.assign(uncompressed_data, raw_data_size);
  }

  return ret;
}

int UncompressHelper::uncompress_internal(const char *compressed_data,
                                          const int64_t compressed_data_size,
                                          const CompressionType compress_type,
                                          const int64_t mod_id,
                                          const int64_t raw_data_size,
                                          char *&raw_data)
{
  int ret = Status::kOk;
  char *uncompress_buf = nullptr;
  int64_t uncompressed_size = 0;
  Compressor *compressor = nullptr;

  if (IS_NULL(compressed_data) || (compressed_data_size <= 0)
      || (raw_data_size <= compressed_data_size)) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), KP(compressed_data),
        K(compressed_data_size), K(raw_data_size));
  } else if (FAILED(CompressorFactory::get_instance().get_compressor(compress_type, compressor))) {
    SE_LOG(WARN, "fail to get compressor", K(ret), KE(compress_type));
  } else if (IS_NULL(uncompress_buf = reinterpret_cast<char *>(base_malloc(raw_data_size, mod_id)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for uncompress buf", K(ret), K(raw_data_size));
  } else if (FAILED(compressor->uncompress(compressed_data,
                                           compressed_data_size,
                                           uncompress_buf,
                                           raw_data_size,
                                           uncompressed_size))) {
    SE_LOG(WARN, "Failed to uncompress data", K(ret));
  } else if (raw_data_size != uncompressed_size) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "The data maybe corrupted", K(ret), K(raw_data_size), K(uncompressed_size));
  } else {
    raw_data = uncompress_buf;
  }

  // release resource
  if (FAILED(ret)) {
    if (IS_NOTNULL(uncompress_buf)) {
      base_free(uncompress_buf);
      uncompress_buf = nullptr;
    }
  }

  return ret;
}

} // namespace util
} // namespace smartengine
