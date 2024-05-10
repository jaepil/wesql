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

#ifndef BINLOG_ARCHIVE_INCLUDED
#define BINLOG_ARCHIVE_INCLUDED

#include "objstore.h"
#include "sql/binlog.h"
#include "sql/binlog_reader.h"
#include "sql/rpl_io_monitor.h"

#define BINLOG_ARCHIVE_SUBDIR "binlog"
#define BINLOG_ARCHIVE_SUBDIR_LEN 6
#define BINLOG_ARCHIVE_INDEX_FILE "binlog.index"
#define BINLOG_ARCHIVE_INDEX_FILE_LEN 12

class THD;

/**
 * @class Binlog_archive
 * @brief The Binlog_archive class is responsible for archiving binary log
 * files.
 */
class Binlog_archive {
 public:
  /**
   * @brief Constructs a Binlog_archive object.
   * @param thd A pointer to the THD object.
   */
  Binlog_archive();

  /**
   * @brief Destructs the Binlog_archive object.
   */
  ~Binlog_archive();

  /**
   * @brief Runs the binlog archiving process.
   */
  void run();
  void thread_set_created() { m_thd_state.set_created(); }
  bool is_thread_alive_not_running() const {
    return m_thd_state.is_alive_not_running();
  }
  bool is_thread_dead() const { return m_thd_state.is_thread_dead(); }
  bool is_thread_alive() const { return m_thd_state.is_thread_alive(); }
  bool is_thread_running() const { return m_thd_state.is_running(); }
  static Binlog_archive *get_instance();
  int archive_event(THD *thd, ushort flags, String *packet,
                    const char *log_file, my_off_t log_pos);
  int flush_events();
  bool stop_waiting_for_update(char *log_file_name, my_off_t log_pos);
  int wait_for_update();
  void signal_update();
  int terminate_binlog_archive_thread();
  void lock_binlog_index() { mysql_mutex_lock(&m_index_lock); }
  void unlock_binlog_index() { mysql_mutex_unlock(&m_index_lock); }
  inline IO_CACHE *get_index_file() { return &m_index_file; }
  std::tuple<int, std::string> purge_logs(const char *to_log,
                                          ulonglong *decrease_log_space);
  int show_binlog_persistent_files(std::vector<objstore::ObjectMeta> &objects);

 private:
  mysql_mutex_t m_index_lock;
  // the binlog archive THD handle.
  THD *m_thd;
  /* thread state */
  thread_state m_thd_state;

  /**
   * @brief Initializes the binlog archive.
   */
  void init();
  /**
   * @brief Cleans up any resources used by the binlog archive.
   */
  void cleanup();
  void set_thread_context();
  int archive_binlog_with_gtid(THD *thd);
  bool is_binlog_archived(const char *log_file_name_arg);

  /* current archive binlog file name, copy from mysql binlog file name
    e.g.
        --log-bin=mysql-bin/binlog
      m_binlog_archive_file_name = mysql-bin/binlog.000001
        --log-bin=/u01/mysql-bin/binlog
      m_binlog_archive_file_name = /u01/mysql-bin/binlog.000001
  */
  // current archive binlog file HANDLE
  FILE *m_binlog_archive_file;
  // current archive binlog file name, copy from mysql binlog file name
  char m_binlog_archive_file_name[FN_REFLEN + 1];
  char m_mysql_archive_dir[FN_REFLEN + 1];
  char m_binlog_archive_dir[FN_REFLEN + 1];
  char m_next_binlog_archive_file_name[FN_REFLEN + 1];
  Format_description_event description_event;
  objstore::ObjectStore *binlog_objstore;

  IO_CACHE m_index_file;
  char m_index_file_name[FN_REFLEN];
  bool open_index_file();
  void close_index_file();
  int add_log_to_index(uchar *log_name, size_t log_name_len);
  int find_log_pos(LOG_INFO *linfo, const char *log_name);
  int find_next_log(LOG_INFO *linfo);
  int remove_logs_from_index(LOG_INFO *linfo);
  /*
    m_crash_safe_index_file is temp file used for guaranteeing
    index file crash safe when master server restarts.
  */
  IO_CACHE m_crash_safe_index_file;
  char m_crash_safe_index_file_name[FN_REFLEN];
  enum enum_log_state { LOG_INDEX_OPENED, LOG_INDEX_CLOSED };
  std::atomic<enum_log_state> atomic_log_index_state{LOG_INDEX_CLOSED};
  int open_crash_safe_index_file();
  int close_crash_safe_index_file();
  int move_crash_safe_index_file_to_index_file();
  int set_crash_safe_index_file_name();
  /*
    purge_file is a temp file used in purge_logs so that the index file
    can be updated before deleting files from disk, yielding better crash
    recovery. It is created on demand the first time purge_logs is called
    and then reused for subsequent calls. It is cleaned up in cleanup().
  */
  IO_CACHE m_purge_index_file;
  char m_purge_index_file_name[FN_REFLEN];
  int set_purge_index_file_name();
  int open_purge_index_file(bool destroy);
  bool is_inited_purge_index_file();
  int close_purge_index_file();
  int sync_purge_index_file();
  int register_purge_index_entry(const char *entry);
  int register_create_index_entry(const char *entry);
  int purge_index_entry(ulonglong *decrease_log_space);
};

extern int start_binlog_archive();
extern void stop_binlog_archive();
extern int binlog_archive_wait_for_update(THD *thd, char *log_file_name,
                                          my_off_t log_pos);
#endif
