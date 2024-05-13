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

#ifndef CONSISTENT_RECOVERY_INCLUDED
#define CONSISTENT_RECOVERY_INCLUDED
#include "my_dbug.h"
#include "my_sys.h"
#include "my_thread.h"
#include "mysql/components/services/log_builtins.h"  // LogErr
#include "mysql/psi/mysql_file.h"
#include "mysqld_error.h"
#include "objstore.h"

// smartengine, innodb, binlog no recovery order dependency.
#define CONSISTENT_RECOVERY_NO 0
#define CONSISTENT_RECOVERY_INNODB 1
#define CONSISTENT_RECOVERY_SMARTENGINE 2
#define CONSISTENT_RECOVERY_BINLOG 4

/**
 * @brief The consistent recovery class.
 * Build recovery from consistent snapshot.
 */
class Consistent_recovery {
 public:
  Consistent_recovery();
  ~Consistent_recovery() {}
  // recovery consistent snapshot
  int recovery_consistent_snapshot(int flags);
  bool recovery_consistent_snapshot_finish();
  // read consistent snapshot file
  bool read_consistent_snapshot_file();
  // recovery mysql innodb
  bool recovery_mysql_innodb();
  // recovery smartengine
  bool recovery_smartengine();
  // recovery binlog
  bool recovery_binlog(const char *index_file_name_arg, const char *log_name);
  bool binlog_recovery_path_is_validation();
  bool need_recovery_binlog();

 private:
  int create_binlog_index_file(const char *index_file_name);
  int add_line_to_index_file(uchar *log_name, size_t log_name_len);
  int close_binlog_index_file();

  enum Consistent_recovery_state {
    CONSISTENT_RECOVERY_STATE_NONE = 0,
    CONSISTENT_RECOVERY_STATE_SNAPSHOT_FILE = 1,
    CONSISTENT_RECOVERY_STATE_MYSQL_INNODB = 2,
    CONSISTENT_RECOVERY_STATE_SE = 3,
    CONSISTENT_RECOVERY_STATE_BINLOG = 4,
    CONSISTENT_RECOVERY_STATE_END
  };
  Consistent_recovery_state m_state;
  objstore::ObjectStore *recovery_objstore;
  uint64_t m_binlog_pos{};
  char m_binlog_file[FN_REFLEN + 1];
  char m_binlog_relative_file[FN_REFLEN + 1];
  // binlog recoverd from consistent snapshot.
  bool is_recovery_binlog;

  char m_mysql_archive_recovery_dir[FN_REFLEN + 1];
  char m_mysql_archive_recovery_data_dir[FN_REFLEN + 1];
  char m_mysql_archive_recovery_binlog_dir[FN_REFLEN + 1];
  char m_mysql_innodb_clone_dir[FN_REFLEN + 1];

  char m_mysql_clone_name[FN_REFLEN + 1];
  char m_mysql_clone_index_file_name[FN_REFLEN + 1];
  IO_CACHE m_mysql_clone_index_file;

  char m_se_backup_index_file_name[FN_REFLEN + 1];
  IO_CACHE m_se_backup_index_file;
  uint m_se_backup_index;
  char m_se_backup_dir[FN_REFLEN + 1];
  char m_se_backup_name[FN_REFLEN + 1];
  IO_CACHE binlog_index_file;
  char binlog_index_file_name[FN_REFLEN + 1];
};

extern Consistent_recovery consistent_recovery;
#endif
