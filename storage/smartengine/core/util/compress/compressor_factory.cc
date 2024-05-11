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

#include "util/compress/compressor_factory.h"
#include "options/advanced_options.h"
#include "util/compress/compressor.h"
#include "util/status.h"

#include "logger/log_module.h"

namespace smartengine {
using namespace common;
using namespace memory;

namespace util {

CompressorFactory &CompressorFactory::get_instance()
{
  static CompressorFactory factory;
  return factory;
}

int CompressorFactory::get_compressor(const CompressionType compress_type, Compressor *&compressor)
{
  int ret = Status::kOk;
  compressor = nullptr;

  switch (compress_type) {
#ifdef HAVE_LZ4
    case common::kLZ4Compression: {
      compressor = &lz4_compressor_;
      break;
    }
#endif // HAVE_LZ4
#ifdef HAVE_ZLIB
    case common::kZlibCompression: {
      compressor = &zlib_compressor_;
      break;
    }
#endif // HAVE_ZLIB
#ifdef HAVE_ZSTD
    case common::kZSTD: {
      compressor = &zstd_compressor_;
      break;
    }
#endif // HAVE_ZSTD
    default:
      ret = Status::kNotSupported;
      SE_LOG(WARN, "the compression is not linked", KE(compress_type));
      break;
  }

  return ret;
}

} // namespace util
} // namespace smartengine
