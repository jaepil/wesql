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

#pragma once

#include "options/options.h"
#include "util/data_buffer.h"
#include <cstdint>

namespace smartengine
{
namespace util
{

class CompressHelper
{
public:
  CompressHelper(int64_t mod_id) : compress_buf_(mod_id) {}
  ~CompressHelper() {}

  int compress(const common::Slice &raw_data,
               common::Slice &compressed_data,
               common::CompressionType &compress_type);
  
private:
  int compress_internal(const char *raw_data,
                        const int64_t raw_data_size,
                        char *&compressed_data,
                        int64_t &compressed_data_size,
                        common::CompressionType &compress_type);

private:
  util::AutoBufferWriter compress_buf_;
};

class UncompressHelper
{
public:
  static int uncompress(const common::Slice &compresed_data,
                        const common::CompressionType compress_type,
                        const int64_t mod_id,
                        const int64_t raw_data_size,
                        common::Slice &raw_data);

private:
  static int uncompress_internal(const char *compressed_data,
                                 const int64_t compressed_data_size,
                                 const common::CompressionType compress_type,
                                 const int64_t mod_id,
                                 const int64_t raw_data_size,
                                 char *&raw_data);
};

} // namespace util
} // namespace smartengine