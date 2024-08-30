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

#include "mysql/components/services/log_builtins.h"  // LogErr
#include "objstore.h"
#include "sql/binlog.h"
#include "sql/clone_handler.h"
#include "sql/rpl_io_monitor.h"
#include "sql/sql_plugin_ref.h"

#define CONSISTENT_ARCHIVE_SUBDIR "data"
#define CONSISTENT_ARCHIVE_SUBDIR_LEN 4
#define CONSISTENT_TAR_SUFFIX ".tar"
#define CONSISTENT_TAR_SUFFIX_LEN 4
#define CONSISTENT_TAR_GZ_SUFFIX ".tar.gz"
#define CONSISTENT_TAR_GZ_SUFFIX_LEN 7
#define CONSISTENT_SNAPSHOT_INDEX_FILE "consistent_snapshot.index"
#define CONSISTENT_SNAPSHOT_INDEX_FILE_LEN 25
#define CONSISTENT_INNODB_ARCHIVE_INDEX_FILE "innodb_archive.index"
#define CONSISTENT_INNODB_ARCHIVE_INDEX_FILE_LEN 20
#define CONSISTENT_INNODB_ARCHIVE_BASENAME "innodb_archive_"
#define CONSISTENT_SE_ARCHIVE_INDEX_FILE "se_archive.index"
#define CONSISTENT_SE_ARCHIVE_INDEX_FILE_LEN 16
#define CONSISTENT_SE_ARCHIVE_BASENAME "se_archive_"
#define MYSQL_SE_DATA_FILE_SUFFIX ".sst"
#define MYSQL_SE_DATA_FILE_SUFFIX_LEN 4
#define MYSQL_SE_WAL_FILE_SUFFIX ".wal"
#define MYSQL_SE_WAL_FILE_SUFFIX_LEN 4
#define MYSQL_SE_TMP_BACKUP_DIR "hotbackup_tmp"
#define MYSQL_SE_TMP_BACKUP_DIR_LEN 13

class THD;

typedef struct Consistent_snapshot {
  char m_consistent_snapshot_local_time[iso8601_size];
  char mysql_clone_name[FN_REFLEN + 1];
  char se_backup_name[FN_REFLEN + 1];
  char binlog_name[FN_REFLEN + 1];
  uint64_t binlog_pos;
  uint64_t consensus_index;
  uint64_t se_snapshot_id;
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
  bool is_thread_running() const { return m_thd_state.is_running(); }
  bool is_thread_dead() const { return m_thd_state.is_thread_dead(); }
  bool is_thread_alive() const { return m_thd_state.is_thread_alive(); }
  static Consistent_archive *get_instance();
  void init_pthread_object();
  void deinit_pthread_object();
  mysql_mutex_t *get_consistent_archive_lock();
  void lock_mysql_clone_index() {
    mysql_mutex_lock(&m_mysql_innodb_clone_index_lock);
  }
  void unlock_mysql_clone_index() {
    mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
  }
  IO_CACHE *get_mysql_clone_index_file();

  void lock_se_backup_index() { mysql_mutex_lock(&m_se_backup_index_lock); }
  void unlock_se_backup_index() { mysql_mutex_unlock(&m_se_backup_index_lock); }
  IO_CACHE *get_se_backup_index_file();

  void lock_consistent_snapshot_index() {
    mysql_mutex_lock(&m_consistent_index_lock);
  }
  void unlock_consistent_snapshot_index() {
    mysql_mutex_unlock(&m_consistent_index_lock);
  }
  IO_CACHE *get_consistent_snapshot_index_file();
  std::tuple<int, std::string> purge_consistent_snapshot(
      const char *to_created_ts, size_t len, bool auto_purge);
  int show_innodb_persistent_files(std::vector<objstore::ObjectMeta> &objects);
  int show_se_persistent_files(std::vector<objstore::ObjectMeta> &objects);
  int show_se_backup_snapshot(THD *thd, std::vector<uint64_t> &backup_ids);
  inline void set_objstore(objstore::ObjectStore *objstore) {
    snapshot_objstore = objstore;
  }
  inline objstore::ObjectStore *get_objstore() {
    return snapshot_objstore;
  }

 private:
  pthread_t m_thread;
  /** the THD handle. */
  THD *m_thd;
  Diagnostics_area m_diag_area;
  /* thread state */
  thread_state m_thd_state;
  void set_thread_context();

  enum Archive_type {
    ARCHIVE_NONE = 0,
    ARCHIVE_MYSQL_INNODB = 1,
    ARCHIVE_SE = 2,
    ARCHIVE_SNAPSHOT_FILE = 3,
    NUM_ARCHIVES
  };

  char m_mysql_archive_dir[FN_REFLEN + 1];
  char m_mysql_archive_data_dir[FN_REFLEN + 1];
  objstore::ObjectStore *snapshot_objstore;
  uint64_t m_consensus_term;
  bool archive_consistent_snapshot();
  int archive_consistent_snapshot_data();
  int archive_consistent_snapshot_binlog();
  int archive_consistent_snapshot_cleanup(bool failed);
  int wait_for_consistent_archive(const std::chrono::seconds &timeout, bool &abort);
  void signal_consistent_archive();
  // index file for every archive type.
  bool open_index_file(const char *index_file_name_arg, const char *log_name,
                       Archive_type arch_type, bool need_lock=false);
  void close_index_file(Archive_type arch_type);
  int find_line_from_index(LOG_INFO *linfo, const char *match_name,
                           Archive_type arch_type);
  int find_next_line_from_index(LOG_INFO *linfo, Archive_type arch_type);
  int add_line_to_index(const char *log_name, Archive_type arch_type);
  int open_crash_safe_index_file(Archive_type arch_type);
  int close_crash_safe_index_file(Archive_type arch_type);
  int move_crash_safe_index_file_to_index_file(Archive_type arch_type);
  int set_crash_safe_index_file_name(const char *base_file_name,
                                     Archive_type arch_type);
  int remove_line_from_index(LOG_INFO *log_info, Archive_type arch_type);

  // Archive mysql innodb
  int generate_innodb_new_name();
  bool achive_mysql_innodb();
  void read_mysql_innodb_clone_status();
  mysql_mutex_t m_mysql_innodb_clone_index_lock;
  char m_mysql_innodb_clone_dir[FN_REFLEN + 1];
  char m_mysql_clone_name[FN_REFLEN + 1];
  char m_mysql_clone_index_file_name[FN_REFLEN + 1];
  IO_CACHE m_mysql_clone_index_file;
  IO_CACHE m_crash_safe_mysql_clone_index_file;
  char m_crash_safe_mysql_clone_index_file_name[FN_REFLEN];
  uint64_t m_mysql_clone_next_index_number;

  // Archive smartengine.
  int generate_se_new_name();
  bool archive_smartengine();
  bool copy_smartengine_wals_and_metas();
  bool release_se_snapshot(uint64_t backup_snapshot_id);
  uint64_t m_mysql_binlog_pos_previous_snapshot;
  uint64_t m_mysql_binlog_pos;
  uint64_t m_consensus_index;
  char m_mysql_binlog_file_previous_snapshot[FN_REFLEN + 1]; // mysql binlog index entry name.
  char m_mysql_binlog_file[FN_REFLEN + 1]; // mysql binlog index entry name.
  char m_binlog_file[FN_REFLEN + 1];
  uint64_t m_se_snapshot_id;
  /**smartengine tmp backup directory, which stores hard link for smartengine
   files, protect them from be deleted before copy completely.*/
  mysql_mutex_t m_se_backup_index_lock;
  char m_se_temp_backup_dir[FN_REFLEN + 1];
  char m_se_backup_index_file_name[FN_REFLEN + 1];
  IO_CACHE m_se_backup_index_file;
  IO_CACHE m_crash_safe_se_backup_index_file;
  char m_crash_safe_se_backup_index_file_name[FN_REFLEN];
  uint64_t m_se_backup_next_index_number;
  char m_se_snapshot_dir[FN_REFLEN + 1];
  char m_se_backup_name[FN_REFLEN + 1];

  // Archive consistent snapshot file
  bool write_consistent_snapshot_file();
  mysql_mutex_t m_consistent_index_lock;
  char m_consistent_snapshot_local_time[iso8601_size];
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
extern char *opt_consistent_snapshot_archive_dir;

#endif
