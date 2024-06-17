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
class Compressor;

class CompressHelper
{
public:
  CompressHelper();
  ~CompressHelper();

  int init(common::CompressionType compress_type);
  void destroy();
  int compress(const common::Slice &raw_data,
               common::Slice &compressed_data,
               common::CompressionType &actual_compress_type);
  
private:
  int actual_compress(const common::Slice &raw_data, common::Slice &compressed_data, common::CompressionType &actual_compress_type);

private:
  bool is_inited_;
  common::CompressionType compress_type_;
  util::Compressor *compressor_;
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
  static int actual_uncompress(const common::Slice &compressed_data,
                               const common::CompressionType compress_type,
                               const int64_t mod_id,
                               const int64_t raw_data_size,
                               common::Slice &raw_data);
};

} // namespace util
} // namespace smartengine