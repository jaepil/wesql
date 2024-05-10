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

#ifndef CONSISTENT_ARCHIVE_INCLUDED
#define CONSISTENT_ARCHIVE_INCLUDED

#include "objstore.h"
#include "sql/binlog.h"
#include "sql/clone_handler.h"
#include "sql/rpl_io_monitor.h"
#include "sql/sql_plugin_ref.h"

#define CONSISTENT_SNAPSHOT_FILE "#consistent_snapshot"
#define CONSISTENT_ARCHIVE_SUBDIR "data"
#define CONSISTENT_ARCHIVE_SUBDIR_LEN 4
#define CONSISTENT_TAR_SUFFIX ".tar.gz"
#define CONSISTENT_TAR_SUFFIX_LEN 7
#define CONSISTENT_SNAPSHOT_INDEX_FILE "consistent_snapshot.index"
#define CONSISTENT_SNAPSHOT_INDEX_FILE_LEN 25
#define CONSISTENT_INNODB_ARCHIVE_INDEX_FILE "innodb_archive.index"
#define CONSISTENT_INNODB_ARCHIVE_INDEX_FILE_LEN 20
#define CONSISTENT_INNODB_ARCHIVE_BASENAME "innodb_archive_"
#define CONSISTENT_INNODB_ARCHIVE_NAME_LEN 21
#define CONSISTENT_SE_ARCHIVE_INDEX_FILE "se_archive.index"
#define CONSISTENT_SE_ARCHIVE_INDEX_FILE_LEN 16
#define CONSISTENT_SE_ARCHIVE_BASENAME "se_archive_"
#define CONSISTENT_SE_ARCHIVE_NAME_LEN 17
#define MYSQL_SE_DATA_FILE_SUFFIX ".sst"
#define MYSQL_SE_DATA_FILE_SUFFIX_LEN 4
#define MYSQL_SE_WAL_FILE_SUFFIX ".wal"
#define MYSQL_SE_WAL_FILE_SUFFIX_LEN 4
#define MYSQL_SE_TMP_BACKUP_DIR "hotbackup_tmp"
#define MYSQL_SE_TMP_BACKUP_DIR_LEN 13
#define CONSISTENT_ARCHIVE_NAME_WITH_NUM_LEN 6

class THD;

typedef struct Consistent_snapshot {
  char mysql_clone_name[FN_REFLEN + 1];
  char se_backup_name[FN_REFLEN + 1];
  char binlog_name[FN_REFLEN + 1];
  uint64_t binlog_pos;
} Consistent_snapshot;

class Consistent_archive {
 public:
  Consistent_archive();
  ~Consistent_archive();
  void run();
  int terminate_consistent_archive_thread();
  void thread_set_created() { m_thd_state.set_created(); }
  bool is_thread_alive_not_running() const {
    return m_thd_state.is_alive_not_running();
  }
  bool is_thread_dead() const { return m_thd_state.is_thread_dead(); }
  bool is_thread_alive() const { return m_thd_state.is_thread_alive(); }
  static Consistent_archive *get_instance();
  void lock_mysql_clone_index() {
    mysql_mutex_lock(&m_mysql_innodb_clone_index_lock);
  }
  void unlock_mysql_clone_index() {
    mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
  }
  inline IO_CACHE *get_mysql_clone_index_file() {
    return &m_mysql_clone_index_file;
  }

  void lock_se_backup_index() { mysql_mutex_lock(&m_se_backup_index_lock); }
  void unlock_se_backup_index() { mysql_mutex_unlock(&m_se_backup_index_lock); }
  inline IO_CACHE *get_se_backup_index_file() {
    return &m_se_backup_index_file;
  }

  void lock_consistent_snapshot_index() {
    mysql_mutex_lock(&m_consistent_index_lock);
  }
  void unlock_consistent_snapshot_index() {
    mysql_mutex_unlock(&m_consistent_index_lock);
  }
  inline IO_CACHE *get_consistent_snapshot_index_file() {
    return &m_consistent_snapshot_index_file;
  }
  bool read_consistent_snapshot_file(Consistent_snapshot *cs);
  std::tuple<int, std::string> purge_consistent_snapshot();
  int show_innodb_persistent_files(std::vector<objstore::ObjectMeta> &objects);
  int show_se_persistent_files(std::vector<objstore::ObjectMeta> &objects);

 private:
  pthread_t m_thread;
  /** the THD handle. */
  THD *m_thd;
  /** Clone handle in server */
  Clone_handler *m_clone;
  /** Loaded clone plugin reference */
  plugin_ref m_plugin;
  Diagnostics_area m_diag_area;
  /* thread state */
  thread_state m_thd_state;
  void init();
  void cleanup();
  void set_thread_context();

  enum Archive_type {
    ARCHIVE_NONE = 0,
    ARCHIVE_MYSQL_INNODB = 1,
    ARCHIVE_SE = 2,
    ARCHICE_SNAPSHOT_FILE = 3,
    NUM_ARCHIVES
  };

  char m_mysql_archive_dir[FN_REFLEN + 1];
  char m_mysql_archive_data_dir[FN_REFLEN + 1];
  objstore::ObjectStore *snapshot_objstore;
  bool archive_consistent_snapshot();
  int wait_for_consistent_archive(const std::chrono::seconds &timeout);
  void signal_consistent_archive();
  // index file for every archive type.
  bool open_index_file(const char *index_file_name_arg, const char *log_name,
                       Archive_type arch_type);
  void close_index_file(Archive_type arch_type);
  int find_line_from_index(LOG_INFO *linfo, const char *match_name,
                           Archive_type arch_type);
  int find_next_line_from_index(LOG_INFO *linfo, Archive_type arch_type);
  int add_line_to_index(uchar *log_name, size_t log_name_len,
                        Archive_type arch_type);
  int open_crash_safe_index_file(Archive_type arch_type);
  int close_crash_safe_index_file(Archive_type arch_type);
  int move_crash_safe_index_file_to_index_file(Archive_type arch_type);
  int set_crash_safe_index_file_name(const char *base_file_name,
                                     Archive_type arch_type);
  int remove_line_from_index(LOG_INFO *log_info, Archive_type arch_type);

  // Archive mysql innodb
  bool achive_mysql_innodb();
  void read_mysql_innodb_clone_status();
  mysql_mutex_t m_mysql_innodb_clone_index_lock;
  char m_mysql_innodb_clone_dir[FN_REFLEN + 1];
  char m_mysql_clone_name[FN_REFLEN + 1];
  char m_mysql_clone_index_file_name[FN_REFLEN + 1];
  IO_CACHE m_mysql_clone_index_file;
  IO_CACHE m_crash_safe_mysql_clone_index_file;
  char m_crash_safe_mysql_clone_index_file_name[FN_REFLEN];
  uint m_mysql_clone_index_number;
  /** Clone States. */
  enum Clone_state : uint32_t {
    STATE_NONE = 0,
    STATE_STARTED,
    STATE_SUCCESS,
    STATE_FAILED,
    NUM_STATES
  };
  /** Length of variable length character columns. */
  static const size_t S_VAR_COL_LENGTH = 512;
  /** Current State. */
  Clone_state m_state{STATE_NONE};
  /** Clone error number. */
  uint32_t m_error_number{};
  /** Unique identifier in current instance. */
  uint32_t m_id{};
  /** Process List ID. */
  uint32_t m_pid{};
  /** Clone start time. */
  uint64_t m_start_time{};
  /** Clone end time. */
  uint64_t m_end_time{};
  /* Source binary log position. */
  uint64_t m_binlog_pos{};
  /** Clone source. */
  char m_source[S_VAR_COL_LENGTH]{};
  /** Clone destination. */
  char m_destination[S_VAR_COL_LENGTH]{};
  /** Clone error message. */
  char m_error_mesg[S_VAR_COL_LENGTH]{};
  /** Source binary log file name. */
  char m_binlog_file[S_VAR_COL_LENGTH]{};
  /** Clone GTID set */
  std::string m_gtid_string;

  // Archive smartengine.
  bool archive_smartengine();
  bool acquire_se_snapshot();
  bool copy_smartengine_wals_and_metas();
  bool release_se_snapshot();
  /**smartengine tmp backup directory, which stores hard link for smartengine
   files, protect them from be deleted before copy completely.*/
  mysql_mutex_t m_se_backup_index_lock;
  char tmp_se_backup_dir_[FN_REFLEN + 1];
  char m_se_backup_index_file_name[FN_REFLEN + 1];
  IO_CACHE m_se_backup_index_file;
  IO_CACHE m_crash_safe_se_backup_index_file;
  char m_crash_safe_se_backup_index_file_name[FN_REFLEN];
  uint m_se_backup_index_number;
  char m_se_backup_dir[FN_REFLEN + 1];
  char m_se_bakcup_name[FN_REFLEN + 1];

  // Archive consistent snapshot file
  bool write_consistent_snapshot_file();
  mysql_mutex_t m_consistent_index_lock;
  char m_consistent_snapshot_index_file_name[FN_REFLEN + 1];
  IO_CACHE m_consistent_snapshot_index_file;
  char m_crash_safe_consistent_snapshot_index_file_name[FN_REFLEN];
  IO_CACHE m_crash_safe_consistent_snapshot_index_file;

  IO_CACHE m_purge_index_file;
  char m_purge_index_file_name[FN_REFLEN];
  int purge_archive(const char *match_name, Archive_type arch_type);
  int set_purge_index_file_name(const char *base_file_name);
  int open_purge_index_file(bool destroy);
  bool purge_index_file_is_inited();
  int close_purge_index_file();
  int sync_purge_index_file();
  int register_purge_index_entry(const char *entry);
  int register_create_index_entry(const char *entry);
  int purge_index_entry(ulonglong *decrease_log_space);
};

extern int start_consistent_archive();
extern void stop_consistent_archive();
extern char *opt_mysql_archive_dir;

#endif
