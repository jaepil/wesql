/*
 * Portions Copyright (c) 2024, ApeCloud Inc Holding Limited
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

#ifndef BINLOG_ARCHIVE_COMMAND_INCLUDED
#define BINLOG_ARCHIVE_COMMAND_INCLUDED

#include "sql/udf_service_impl.h"
#include "sql/udf_service_util.h"

class Binlog_archive_command : public Udf_service_impl {
 public:
  Binlog_archive_command() = default;
  ~Binlog_archive_command() override = default;
  bool init() override;
  bool deinit();

 private:
  Udf_charset_service m_charset_service;
  static constexpr const char *m_udf_name = "binlog_persistent_purge";
  bool m_initialized{false};
  static char *binlog_purge(UDF_INIT *, UDF_ARGS *args, char *result,
                    unsigned long *length, unsigned char *,
                    unsigned char *error);
  static bool binlog_purge_init(UDF_INIT *init_id, UDF_ARGS *args, char *message);
  static void binlog_purge_deinit(UDF_INIT *);
};
#endif
