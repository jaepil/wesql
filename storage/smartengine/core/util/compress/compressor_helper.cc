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

CompressHelper::CompressHelper()
    : is_inited_(false),
      compressor_(nullptr),
      compress_buf_(ModId::kCompressor)
{}

CompressHelper::~CompressHelper()
{
  destroy();
}

void CompressHelper::destroy()
{
  if (is_inited_) {
    CompressorFactory::get_instance().free_compressor(compressor_);
    compressor_ = nullptr;
    is_inited_ = false;
  }
}

int CompressHelper::init(CompressionType compress_type)
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    SE_LOG(WARN, "the CompressHelper has been inited", K(ret));
  } else if ((kNoCompression != compress_type) &&
              FAILED(CompressorFactory::get_instance().alloc_compressor(compress_type, compressor_))) {
    SE_LOG(WARN, "fail to allocate compressor", K(ret), KE(compress_type));
  } else {
    compress_type_ = compress_type;
    is_inited_ = true;
  }

  return ret;
}

int CompressHelper::compress(const Slice &raw_data, Slice &compressed_data, CompressionType &actual_compress_type)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    SE_LOG(WARN, "the CompressHelper has not been inited", K(ret));
  } else if (UNLIKELY(raw_data.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret));
  } else {
    if (kNoCompression == compress_type_) {
      compressed_data.assign(raw_data.data(), raw_data.size());
      actual_compress_type = kNoCompression;
    } else {
      if (FAILED(actual_compress(raw_data, compressed_data, actual_compress_type))) {
        SE_LOG(WARN, "fail to compress", K(ret));
      }
    }
  }

  return ret;
}

int CompressHelper::actual_compress(const Slice &raw_data, Slice &compressed_data, CompressionType &actual_compress_type)
{
  int ret = Status::kOk;
  bool not_compress = false;
  int64_t max_compress_size = 0;
  int64_t compressed_size = 0;

  compress_buf_.reuse();
  if (UNLIKELY(raw_data.empty())) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "Invalid argument", K(ret), K(raw_data));
  } else if (FAILED(compressor_->get_max_compress_size(raw_data.size(), max_compress_size))) {
    SE_LOG(WARN, "Failed to get max compress size", K(ret), "raw_data_size", raw_data.size());
  } else if (FAILED(compress_buf_.reserve(max_compress_size))) {
    SE_LOG(WARN, "Failed to reserve compress buffer", K(ret), K(max_compress_size));
  } else if (FAILED(compressor_->compress(raw_data.data(),
                                          raw_data.size(),
                                          compress_buf_.data(),
                                          compress_buf_.capacity(),
                                          compressed_size))) {
    if (Status::kNotCompress == ret) {
      not_compress = true;
      //override ret
      ret = Status::kOk;
    } else {
      SE_LOG(WARN, "fail to compress data", K(ret));
    }
  } else if (compressed_size >= static_cast<int64_t>(raw_data.size())) {
    not_compress = true;
  } else {
    compressed_data.assign(compress_buf_.data(), compressed_size);
    actual_compress_type = compress_type_;
  }

  if (SUCCED(ret) && not_compress) {
    // TODO (Zhao Dongsheng) : Add some statistics for actually not compress.
    compressed_data.assign(raw_data.data(), raw_data.size());
    actual_compress_type = kNoCompression;
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

  if (UNLIKELY(compresed_data.empty()) || UNLIKELY(raw_data_size < static_cast<int64_t>(compresed_data.size()))) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(compresed_data), K(raw_data_size));
  } else {
    if (kNoCompression == compress_type) {
      if (UNLIKELY(raw_data_size != static_cast<int64_t>(compresed_data.size()))) {
        ret = Status::kCorruption;
        SE_LOG(WARN, "the data is corrupted or compress type is wrong", K(ret),
            K(raw_data_size), KE(compress_type), K(compresed_data));
      } else {
        raw_data.assign(compresed_data.data(), compresed_data.size());
      }
    } else {
      if (FAILED(actual_uncompress(compresed_data,
                                   compress_type,
                                   mod_id,
                                   raw_data_size,
                                   raw_data))) {
        SE_LOG(WARN, "fail to internal uncompress data", K(ret));
      }
    }
  }

  return ret;
}

int UncompressHelper::actual_uncompress(const Slice &compressed_data,
                                        const CompressionType compress_type,
                                        const int64_t mod_id,
                                        const int64_t raw_data_size,
                                        Slice &raw_data)
{
  int ret = Status::kOk;
  char *raw_buf = nullptr;
  int64_t raw_size = 0;
  Compressor *compressor = nullptr;

  if (UNLIKELY(compressed_data.empty()) || UNLIKELY(raw_data_size < static_cast<int64_t>(compressed_data.size()))) {
    ret = Status::kInvalidArgument;
    SE_LOG(WARN, "invalid argument", K(ret), K(compressed_data), K(raw_data_size));
  } else if (FAILED(CompressorFactory::get_instance().alloc_compressor(compress_type, compressor))) {
    SE_LOG(WARN, "fail to allocate compressor", K(ret), KE(compress_type));
  } else if (IS_NULL(raw_buf = reinterpret_cast<char *>(base_malloc(raw_data_size, mod_id)))) {
    ret = Status::kMemoryLimit;
    SE_LOG(WARN, "fail to allocate memory for uncompress buf", K(ret), K(raw_data_size));
  } else if (FAILED(compressor->uncompress(compressed_data.data(),
                                           compressed_data.size(),
                                           raw_buf,
                                           raw_data_size,
                                           raw_size))) {
    SE_LOG(WARN, "Failed to uncompress data", K(ret));
  } else if (raw_data_size != raw_size) {
    ret = Status::kCorruption;
    SE_LOG(WARN, "The data maybe corrupted", K(ret), K(raw_data_size), K(raw_size));
  } else {
    raw_data.assign(raw_buf, raw_size);
  }

  // release resource
  if (IS_NOTNULL(compressor)) {
    CompressorFactory::get_instance().free_compressor(compressor);
  }

  if (FAILED(ret)) {
    if (IS_NOTNULL(raw_buf)) {
      base_free(raw_buf);
      raw_buf = nullptr;
    }
  }

  return ret;
}

} // namespace util
} // namespace smartengine
