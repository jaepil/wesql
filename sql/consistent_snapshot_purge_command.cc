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

#include "sql/consistent_snapshot_purge_command.h"

#include "m_string.h"
#include "my_dbug.h"
#include "mysqld_error.h"
#include "sql/consistent_archive.h"
#include "sql/sql_class.h"

bool Consistent_archive_purge_command::init() {
  DBUG_TRACE;

  Udf_data udf(m_udf_name, STRING_RESULT,
               Consistent_archive_purge_command::consistent_archive_purge,
               Consistent_archive_purge_command::consistent_archive_purge_init,
               Consistent_archive_purge_command::consistent_archive_purge_deinit);

  m_initialized = !register_udf(udf);
  return !m_initialized;
}

bool Consistent_archive_purge_command::deinit() {
  DBUG_TRACE;

  if (m_initialized && !unregister_udf(m_udf_name)) {
    m_initialized = false;
  }

  return m_initialized;
}

char *Consistent_archive_purge_command::consistent_archive_purge(
    UDF_INIT *, UDF_ARGS *args, char *result, unsigned long *length,
    unsigned char *, unsigned char *error) {
  DBUG_TRACE;
  Consistent_archive *consistent_archive = Consistent_archive::get_instance();
  *error = 0;
  auto err_val{0};
  std::string err_msg{};  // error message

  if (!consistent_archive) {
    *error = 1;
    err_msg.assign("consistent snapshot archive  is not started.");
    goto err;
  }

  std::tie(err_val, err_msg) =
      consistent_archive->purge_consistent_snapshot(args->args[0], args->lengths[0], false);
  if (err_val) {
    *error = err_val;
    my_error(ER_UDF_ERROR, MYF(0), m_udf_name, err_msg.c_str());
  }

err:
  strcpy(result, err_msg.c_str());
  *length = err_msg.length();
  return result;
}

bool Consistent_archive_purge_command::consistent_archive_purge_init(
    UDF_INIT *init_id, UDF_ARGS *args, char *message) {
  DBUG_TRACE;
  if (args->arg_count != 1) {
    my_stpcpy(message,
              "Wrong arguments: You need to specify correct arguments.");
    return true;
  }
  if (args->arg_type[0] != STRING_RESULT) {
    my_stpcpy(
        message,
        "Wrong arguments: You need to specify snapshot created timestamp.");
    return true;
  }

  if (Udf_charset_service::set_return_value_charset(init_id) ||
      Udf_charset_service::set_args_charset(args)) {
    return true;
  }

  init_id->maybe_null = false;
  return false;
}

void Consistent_archive_purge_command::consistent_archive_purge_deinit(UDF_INIT *) {
  DBUG_TRACE;
}
