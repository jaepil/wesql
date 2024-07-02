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

#include "sql/binlog_archive_command.h"

#include "m_string.h"
#include "my_dbug.h"
#include "mysqld_error.h"
#include "sql/binlog_archive.h"
#include "sql/sql_class.h"

bool Binlog_archive_command::init() {
  DBUG_TRACE;

  Udf_data udf(m_udf_name, STRING_RESULT, Binlog_archive_command::binlog_purge,
               Binlog_archive_command::binlog_purge_init,
               Binlog_archive_command::binlog_purge_deinit);

  m_initialized = !register_udf(udf);
  return !m_initialized;
}

bool Binlog_archive_command::deinit() {
  DBUG_TRACE;

  if (m_initialized && !unregister_udf(m_udf_name)) {
    m_initialized = false;
  }

  return m_initialized;
}

char *Binlog_archive_command::binlog_purge(UDF_INIT *, UDF_ARGS *args,
                                           char *result, unsigned long *length,
                                           unsigned char *,
                                           unsigned char *error) {
  DBUG_TRACE;
  Binlog_archive *binlog_archive = Binlog_archive::get_instance();
  *error = 0;
  auto err_val{0};
  std::string err_msg{};                                  // error message
  std::string log_file(args->args[0], args->lengths[0]);  // binlog file name

  if (!binlog_archive) {
    *error = 1;
    err_msg.assign("binlog archive  is not started.");
    goto err;
  }

  std::tie(err_val, err_msg) =
      binlog_archive->purge_logs(log_file.c_str(), nullptr);
  if (err_val) {
    *error = err_val;
    my_error(ER_UDF_ERROR, MYF(0), m_udf_name, err_msg.c_str());
  } else {
    err_msg.assign("Purge binlog persistent files successfully");
  }

err:
  strcpy(result, err_msg.c_str());
  *length = err_msg.length();
  return result;
}

bool Binlog_archive_command::binlog_purge_init(UDF_INIT *init_id,
                                               UDF_ARGS *args, char *message) {
  DBUG_TRACE;
  if (args->arg_count != 1) {
    my_stpcpy(message,
              "Wrong arguments: You need to specify correct arguments.");
    return true;
  }
  if (args->arg_type[0] != STRING_RESULT) {
    my_stpcpy(message,
              "Wrong arguments: You need to specify binlog file name.");
    return true;
  }

  if (Udf_charset_service::set_return_value_charset(init_id) ||
      Udf_charset_service::set_args_charset(args)) {
    return true;
  }

  init_id->maybe_null = false;
  return false;
}

void Binlog_archive_command::binlog_purge_deinit(UDF_INIT *) { DBUG_TRACE; }
