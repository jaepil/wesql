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

#include "sql/binlog_archive.h"

#include <fstream>
#include <sstream>
#include <string>

#include "libbinlogevents/include/binlog_event.h"  // binary_log::max_log_event_size
#include "mf_wcomp.h"                              // wild_one, wild_many
#include "my_dbug.h"
#include "my_thread.h"
#include "mysql/components/services/log_builtins.h"  // LogErr
#include "mysql/components/services/log_shared.h"
#include "mysql/psi/mysql_file.h"
#include "mysql/psi/mysql_thread.h"
#include "objstore.h"
#include "sql/basic_istream.h"
#include "sql/basic_ostream.h"
#include "sql/binlog.h"
#include "sql/binlog_istream.h"
#include "sql/binlog_ostream.h"
#include "sql/binlog_reader.h"
#include "sql/consistent_archive.h"
#include "sql/debug_sync.h"
#include "sql/derror.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/log.h"
#include "sql/mdl.h"
#include "sql/mysqld.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/protocol_classic.h"
#include "sql/rpl_io_monitor.h"
#include "sql/rpl_source.h"
#include "sql/sql_parse.h"
#include "unsafe_string_append.h"

const float Binlog_archive::PACKET_GROW_FACTOR = 2.0;
const float Binlog_archive::PACKET_SHRINK_FACTOR = 0.5;

// Global binlog archive thread object
static Binlog_archive *mysql_binlog_archive = nullptr;
static my_thread_handle mysql_binlog_archive_pthd;
static bool abort_binlog_archive = false;

static mysql_mutex_t m_run_lock;
static mysql_cond_t m_run_cond;

#ifdef HAVE_PSI_INTERFACE
static PSI_thread_key THR_KEY_binlog_archive;
static PSI_mutex_key PSI_binlog_archive_lock_key;
static PSI_cond_key PSI_binlog_archive_cond_key;
static PSI_mutex_key PSI_binlog_index_lock_key;
static PSI_mutex_key PSI_binlog_rotate_lock_key;

/** The instrumentation key to use for opening the log file. */
static PSI_file_key PSI_binlog_purge_archive_log_file_key;
/** The instrumentation key to use for opening the log file. */
static PSI_file_key PSI_binlog_archive_log_file_slice_key;
/** The instrumentation key to use for opening the log index file. */
static PSI_file_key PSI_binlog_archive_log_index_key;
/** The instrumentation key to use for opening a log index cache file. */
static PSI_file_key PSI_binlog_archive_log_index_cache_key;

static PSI_thread_info all_binlog_archive_threads[] = {
    {&THR_KEY_binlog_archive, "archive", "bin_arch", PSI_FLAG_AUTO_SEQNUM, 0,
     PSI_DOCUMENT_ME}};

static PSI_mutex_info all_binlog_archive_mutex_info[] = {
    {&PSI_binlog_archive_lock_key, "binlog_archive::mutex", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_index_lock_key, "binlog_archive_index::mutex", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_rotate_lock_key, "binlog_archive_rotate::mutex", 0, 0,
     PSI_DOCUMENT_ME}};

static PSI_cond_info all_binlog_archive_cond_info[] = {
    {&PSI_binlog_archive_cond_key, "binlog_archive::condition", 0, 0,
     PSI_DOCUMENT_ME}};

static PSI_file_info all_binlog_archive_files[] = {
    {&PSI_binlog_purge_archive_log_file_key, "binlog_archive_file", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_log_file_slice_key, "binlog_archive_slice", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_log_index_key, "binlog_archive_index", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_log_index_cache_key, "binlog_archive_index_cache", 0,
     0, PSI_DOCUMENT_ME}};
#endif

static void *mysql_binlog_archive_action(void *arg);
static int compare_log_name(const char *log_1, const char *log_2);
static bool remove_file(const std::string &file);
static void root_directory(std::string in, std::string *out, std::string *left);
static bool recursive_create_dir(const std::string &dir,
                                 const std::string &root);
static void exec_binlog_error_action_abort(const char *err_string);
static time_t calculate_auto_purge_lower_time_bound();

/**
 * @brief Creates a binlog archive object and starts the binlog archive thread.
 */
int start_binlog_archive() {
  DBUG_TRACE;

  // Check if the binlog is enabled.
  if (!opt_bin_log) {
    LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "The binlog archive start need binlog mode.");
    return 0;
  }
  // Check if the binlog archive is enabled.
  if (!opt_binlog_archive || !opt_serverless) {
    LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "The binlog persistent not enabled.");
    return 0;
  }
  // Check if the mysql archive path is set.
  if (opt_consistent_snapshot_archive_dir == nullptr) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "Mysql archive path no set, please set --consistent_snapshot_archive_dir.");
    return 1;
  }

  MY_STAT d_stat;
  // Check if opt_consistent_snapshot_archive_dir exists.
  if (!my_stat(opt_consistent_snapshot_archive_dir, &d_stat, MYF(0)) ||
      !MY_S_ISDIR(d_stat.st_mode) ||
      my_access(opt_consistent_snapshot_archive_dir, (F_OK | W_OK))) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "Mysql archive path not exists.");
    return 1;
  }

  // data home can not include archive directory.
  if (test_if_data_home_dir(opt_consistent_snapshot_archive_dir)) {
    std::string err_msg;
    err_msg.assign("mysql archive path is within the current data directory: ");
    err_msg.append(opt_consistent_snapshot_archive_dir);
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    return 1;
  }

  // Check if archive binlog dir exists. If not exists, create it.
  char tmp_archive_binlog_dir[FN_REFLEN + 1] = {0};
  strmake(tmp_archive_binlog_dir, opt_consistent_snapshot_archive_dir,
          sizeof(tmp_archive_binlog_dir) - BINLOG_ARCHIVE_SUBDIR_LEN - 1);
  strmake(
      convert_dirname(tmp_archive_binlog_dir, tmp_archive_binlog_dir, NullS),
      STRING_WITH_LEN(BINLOG_ARCHIVE_SUBDIR));
  convert_dirname(tmp_archive_binlog_dir, tmp_archive_binlog_dir, NullS);

  // If binlog persist to object store,
  // when startup, remove local binlog archive dir and recreate it.
  if (opt_persistent_on_objstore) {
    remove_file(tmp_archive_binlog_dir);
  }

  // Check if the binlog archive dir exists. If not exists, create it.
  if (!my_stat(tmp_archive_binlog_dir, &d_stat, MYF(0))) {
    if (my_mkdir(tmp_archive_binlog_dir, 0777, MYF(0))) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             "Failed to create binlog archive dir.");
      return 1;
    }
  } else {
    if (!MY_S_ISDIR(d_stat.st_mode) ||
        my_access(tmp_archive_binlog_dir, (F_OK | W_OK))) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             "Mysql binlog archive path not access.");
      return 1;
    }
  }

  LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, "start binlog archive");

#ifdef HAVE_PSI_INTERFACE
  const char *category = "archive";
  int count_thread =
      static_cast<int>(array_elements(all_binlog_archive_threads));
  mysql_thread_register(category, all_binlog_archive_threads, count_thread);

  int count_mutex =
      static_cast<int>(array_elements(all_binlog_archive_mutex_info));
  mysql_mutex_register(category, all_binlog_archive_mutex_info, count_mutex);

  int count_cond =
      static_cast<int>(array_elements(all_binlog_archive_cond_info));
  mysql_cond_register(category, all_binlog_archive_cond_info, count_cond);

  int count_file = static_cast<int>(array_elements(all_binlog_archive_files));
  mysql_file_register(category, all_binlog_archive_files, count_file);

#endif

  mysql_mutex_init(PSI_binlog_archive_lock_key, &m_run_lock,
                   MY_MUTEX_INIT_FAST);
  mysql_cond_init(PSI_binlog_archive_cond_key, &m_run_cond);

  mysql_binlog_archive = new Binlog_archive();

  mysql_mutex_lock(&m_run_lock);
  abort_binlog_archive = false;
  if (mysql_thread_create(THR_KEY_binlog_archive, &mysql_binlog_archive_pthd,
                          &connection_attrib, mysql_binlog_archive_action,
                          (void *)mysql_binlog_archive)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "Failed to create binlog archive thread");
    mysql_mutex_unlock(&m_run_lock);
    return 1;
  }

  mysql_binlog_archive->thread_set_created();

  while (mysql_binlog_archive->is_thread_alive_not_running()) {
    DBUG_PRINT("sleep", ("Waiting for binlog archive thread to start"));
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_run_cond, &m_run_lock, &abstime);
  }
  mysql_mutex_unlock(&m_run_lock);
  return 0;
}

/**
 * @brief stop the binlog archive thread.
 * @note must stop consistent snapshot thread, before stop the binlog archive
 * thread. Consistent snapshot thread depends on the binlog archive thread.
 */
void stop_binlog_archive() {
  DBUG_TRACE;

  if (!mysql_binlog_archive) return;

  LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, "stop binlog archive");

  mysql_mutex_lock(&m_run_lock);
  if (mysql_binlog_archive->is_thread_dead()) {
    mysql_mutex_unlock(&m_run_lock);
    goto end;
  }
  mysql_mutex_unlock(&m_run_lock);
  mysql_binlog_archive->terminate_binlog_archive_thread();
  /* Wait until the thread is terminated. */
  my_thread_join(&mysql_binlog_archive_pthd, nullptr);
end:
  mysql_mutex_lock(&m_run_lock);
  delete mysql_binlog_archive;
  mysql_binlog_archive = nullptr;
  mysql_mutex_unlock(&m_run_lock);

  mysql_mutex_destroy(&m_run_lock);
  mysql_cond_destroy(&m_run_cond);
}

/**
 * @brief Archives the MySQL binlog.
 * @param arg The binlog archive object.
 */
static void *mysql_binlog_archive_action(void *arg) {
  Binlog_archive *archive = (Binlog_archive *)arg;
  archive->run();
  return nullptr;
}

class Binlog_archive::Event_allocator {
 public:
  enum { DELEGATE_MEMORY_TO_EVENT_OBJECT = false };

  void set_archiver(Binlog_archive *archiver) { m_archiver = archiver; }
  unsigned char *allocate(size_t size) {
    my_off_t event_offset = m_archiver->m_packet.length();
    if (m_archiver->grow_packet(size)) return nullptr;

    m_archiver->m_packet.length(event_offset + size);
    return pointer_cast<unsigned char *>(m_archiver->m_packet.ptr() +
                                         event_offset);
  }

  void deallocate(unsigned char *ptr [[maybe_unused]]) {}

 private:
  Binlog_archive *m_archiver = nullptr;
};

/**
 * @brief Constructs a Binlog_archive object.
 */
Binlog_archive::Binlog_archive()
    : m_thd(nullptr),
      m_thd_state(),
      m_description_event(BINLOG_VERSION, ::server_version),
      binlog_objstore(nullptr),
      m_diag_area(false),
      m_half_buffer_size_req_counter(0),
      m_new_shrink_size(PACKET_MIN_SIZE),
      m_index_file(),
      m_crash_safe_index_file(),
      m_purge_index_file() {
  m_binlog_archive_first_slice = false;
  m_binlog_archive_last_event_pos = 0;
  m_slice_bytes_written = 0;
  m_binlog_last_event_pos = 0;
  m_slice_pipeline_head = nullptr;
  m_next_binlog_archive_file_name[0] = '\0';
  m_mysql_archive_dir[0] = '\0';
  m_binlog_archive_dir[0] = '\0';
  m_binlog_archive_file_name[0] = '\0';
  m_binlog_archive_relative_file_name[0] = '\0';
  m_index_file_name[0] = '\0';
  m_crash_safe_index_file_name[0] = '\0';
  m_purge_index_file_name[0] = '\0';
  init();
}

/**
 * @brief Destructs the Binlog_archive object.
 */
Binlog_archive::~Binlog_archive() {
  DBUG_TRACE;
  cleanup();
}

/**
 * @brief Initializes the binlog archive object.
 */
void Binlog_archive::init() {
  DBUG_TRACE;
  mysql_mutex_init(PSI_binlog_index_lock_key, &m_index_lock,
                   MY_MUTEX_INIT_FAST);
  mysql_mutex_init(PSI_binlog_rotate_lock_key, &m_rotate_lock,
                   MY_MUTEX_INIT_FAST);
}

/**
 * @brief Cleans up any resources used by the binlog archive.
 */
void Binlog_archive::cleanup() {
  DBUG_TRACE;
  mysql_mutex_destroy(&m_index_lock);
  mysql_mutex_destroy(&m_rotate_lock);
}

/**
 * @brief Returns the binlog archive instance.
 * @return Binlog_archive*
 */
Binlog_archive *Binlog_archive::get_instance() { return mysql_binlog_archive; }

/**
 * @brief Terminate the binlog archive thread.
 *
 * @return int
 */
int Binlog_archive::terminate_binlog_archive_thread() {
  DBUG_TRACE;
  mysql_mutex_lock(&m_run_lock);
  abort_binlog_archive = true;
  while (m_thd_state.is_thread_alive()) {
    DBUG_PRINT("sleep", ("Waiting for binlog archive thread to stop"));
    if (m_thd_state.is_initialized()) {
      mysql_mutex_lock(&m_thd->LOCK_thd_data);
      m_thd->awake(THD::KILL_CONNECTION);
      mysql_mutex_unlock(&m_thd->LOCK_thd_data);
    }
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_run_cond, &m_run_lock, &abstime);
  }
  assert(m_thd_state.is_thread_dead());
  mysql_mutex_unlock(&m_run_lock);
  return 0;
}

/**
 * @brief Initializes the THD context.
 *
 */
void Binlog_archive::set_thread_context() {
  THD *thd = new THD;
  my_thread_init();
  thd->set_new_thread_id();
  thd->thread_stack = (char *)&thd;
  thd->store_globals();
  thd->get_protocol_classic()->init_net(nullptr);
  thd->thread_stack = (char *)&thd;
  thd->system_thread = SYSTEM_THREAD_BACKGROUND;
  /* No privilege check needed */
  thd->security_context()->skip_grants();
  // Global_THD_manager::get_instance()->add_thd(thd);
  m_thd = thd;
}

static bool is_number(const char *str, ulonglong *res, bool allow_wildcards) {
  int flag;
  const char *start;
  DBUG_TRACE;

  flag = 0;
  start = str;
  while (*str++ == ' ')
    ;
  if (*--str == '-' || *str == '+') str++;
  while (my_isdigit(files_charset_info, *str) ||
         (allow_wildcards && (*str == wild_many || *str == wild_one))) {
    flag = 1;
    str++;
  }
  if (*str == '.') {
    for (str++; my_isdigit(files_charset_info, *str) ||
                (allow_wildcards && (*str == wild_many || *str == wild_one));
         str++, flag = 1)
      ;
  }
  if (*str != 0 || flag == 0) return false;
  if (res) *res = atol(start);
  return true; /* Number ok */
} /* is_number */

/**
 * @brief Runs the binlog archiving process.
 */
void Binlog_archive::run() {
  DBUG_TRACE;
  std::string err_msg;
  LOG_INFO log_info;
  int error = 1;
  set_thread_context();

  mysql_mutex_lock(&m_run_lock);
  m_thd_state.set_initialized();
  mysql_cond_broadcast(&m_run_cond);
  mysql_mutex_unlock(&m_run_lock);

  // Set @source_binlog_checksum = 'NONE';
  LEX_CSTRING var_name = {STRING_WITH_LEN("source_binlog_checksum")};
  Item *item_str = new Item_string(STRING_WITH_LEN("NONE"), &my_charset_latin1);
  Item_func_set_user_var *suv = new Item_func_set_user_var(var_name, item_str);
  if (suv->fix_fields(m_thd, nullptr) || suv->check(false) || suv->update()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "Failed to set source_binlog_checksum to NONE");
    goto end;
  }

  // persist the binlog to the object store.
  if (opt_persistent_on_objstore) {
    std::string_view endpoint(
        opt_objstore_endpoint ? std::string_view(opt_objstore_endpoint) : "");
    binlog_objstore = objstore::create_object_store(
        std::string_view(opt_objstore_provider),
        std::string_view(opt_objstore_region),
        opt_objstore_endpoint ? &endpoint : nullptr, opt_objstore_use_https);
    if (!binlog_objstore) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             "Failed to create object store instance");
      goto end;
    }
  }

  strmake(m_mysql_archive_dir, opt_consistent_snapshot_archive_dir,
          sizeof(m_mysql_archive_dir) - 1);
  convert_dirname(m_mysql_archive_dir, m_mysql_archive_dir, NullS);
  strmake(strmake(m_binlog_archive_dir, m_mysql_archive_dir,
                  sizeof(m_binlog_archive_dir) - BINLOG_ARCHIVE_SUBDIR_LEN - 1),
          STRING_WITH_LEN(BINLOG_ARCHIVE_SUBDIR));
  // if m_binlog_archive_dir dir not exists, start_binlog_archive() will create
  // it.
  convert_dirname(m_binlog_archive_dir, m_binlog_archive_dir, NullS);

  // create archive binlog sub dir, if not exists.
  {
    // Read the first binlog file from mysql binlog.index file.
    error = mysql_bin_log.find_log_pos(&log_info, NullS, true);
    if (error != 0) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             "Failed to find binlog from mysql binlog");
      goto end;
    }
    // m_binlog_archive_file_name could be a absolute path.
    // get relative path. Check if the binlog file name's first char
    // is "/"
    char temp_binlog_dir[FN_REFLEN + 1] = {0};
    char temp_binlog_file[FN_REFLEN + 1] = {0};
    size_t temp_binlog_dir_len = 0;
    if (log_info.log_file_name[0] == FN_LIBCHAR) {
      strmake(temp_binlog_file, log_info.log_file_name + 1,
              sizeof(temp_binlog_file) - 1);
    } else if (log_info.log_file_name[0] == FN_CURLIB &&
               log_info.log_file_name[1] == FN_LIBCHAR) {
      strmake(temp_binlog_file, log_info.log_file_name + 2,
              sizeof(temp_binlog_file) - 1);
    } else {
      strmake(temp_binlog_file, log_info.log_file_name,
              sizeof(temp_binlog_file) - 1);
    }

    dirname_part(temp_binlog_dir, temp_binlog_file, &temp_binlog_dir_len);
    if (temp_binlog_dir_len > 0) {
      MY_STAT d_stat;
      char temp_archive_binlog_dir[FN_REFLEN + 1] = {0};
      strmake(
          strmake(temp_archive_binlog_dir, m_binlog_archive_dir,
                  sizeof(temp_archive_binlog_dir) - temp_binlog_dir_len - 1),
          temp_binlog_dir, temp_binlog_dir_len);
      if (!my_stat(temp_archive_binlog_dir, &d_stat, MYF(0))) {
        // recursive create subdir.
        if (recursive_create_dir(m_binlog_archive_relative_file_name,
                                 m_binlog_archive_dir)) {
          error = 1;
          LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
                 "Failed to create binlog archive sub dir.");
          goto end;
        }
      }
    }
  }

  mysql_mutex_lock(&m_run_lock);
  m_thd_state.set_running();
  mysql_cond_broadcast(&m_run_cond);
  mysql_mutex_unlock(&m_run_lock);

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, "Binlog archive thread running");

  for (;;) {
    char binlog_file_name[FN_REFLEN + 1] = {0};
    mysql_mutex_lock(&m_run_lock);
    if (abort_binlog_archive) {
      mysql_mutex_unlock(&m_run_lock);
      break;
    }
    mysql_mutex_unlock(&m_run_lock);
    if (m_thd == nullptr || m_thd->killed) break;
    m_thd->clear_error();

    mysql_mutex_lock(&m_index_lock);
    if (open_index_file()) {
      err_msg.assign("Failed to open archive binlog index file: ");
      err_msg.append(BINLOG_ARCHIVE_INDEX_FILE);
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      mysql_mutex_unlock(&m_index_lock);
      break;
    }
    // Find the last archived binlog file.
    error = find_log_pos(&log_info, NullS);
    if (error == 0) {
      do {
        strmake(binlog_file_name, log_info.log_file_name,
                sizeof(binlog_file_name) - 1);
      } while (!(error = find_next_log(&log_info)));
      if (error != LOG_INFO_EOF) {
        close_index_file();
        mysql_mutex_unlock(&m_index_lock);
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
               "Failed to find next log from archive binlog index file.");
        break;
      }
    } else if (error == LOG_INFO_EOF) {
      // log index file is empty.
      binlog_file_name[0] = '\0';
    } else {
      close_index_file();
      mysql_mutex_unlock(&m_index_lock);
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             "Failed IO to find log from archive binlog index file.");
      break;
    }
    mysql_mutex_unlock(&m_index_lock);
    // Verify if the binlog_file_name is valid. Find the position
    // from mysql binlog.index file.
    if (binlog_file_name[0] != '\0') {
      error = mysql_bin_log.find_log_pos(&log_info, binlog_file_name, true);
      // Find the next binlog file, start archive from the next binlog file.
      if (error == LOG_INFO_EOF) {
        // If not found, identify binlog path change. Start archive from the
        // first binlog file.
        binlog_file_name[0] = '\0';
      } else if (error != 0) {
        err_msg.assign("Failed to find binlog from mysql binlog: ");
        err_msg.append(binlog_file_name);
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
        close_index_file();
        break;
      }
    }
    m_binlog_last_event_pos = BIN_LOG_HEADER_SIZE;
    m_binlog_archive_last_event_pos = BIN_LOG_HEADER_SIZE;
    // Get archive start binlog file name.
    strmake(m_binlog_archive_file_name, binlog_file_name,
            sizeof(m_binlog_archive_file_name) - 1);

    // Get last archive file start position.
    if (binlog_file_name[0] != '\0' && binlog_objstore) {
      std::vector<objstore::ObjectMeta> objects;
      char binlog_relative_file[FN_REFLEN + 1] = {0};
      // Generate binlog keyid.
      // If binlog file is absolute path, change absolute path to relative path,
      // remove the root path.
      // If binlog file is relative path like ./binlog.000001, change it to
      // binlog.000001.
      if (binlog_file_name[0] == FN_LIBCHAR) {
        strmake(binlog_relative_file, binlog_file_name + 1,
                sizeof(binlog_relative_file) - 1);
      } else if (binlog_file_name[0] == FN_CURLIB &&
                 binlog_file_name[1] == FN_LIBCHAR) {
        strmake(binlog_relative_file, binlog_file_name + 2,
                sizeof(binlog_relative_file) - 1);
      } else {
        strmake(binlog_relative_file, binlog_file_name,
                sizeof(binlog_relative_file) - 1);
      }
      std::string binlog_keyid{};
      binlog_keyid.assign(BINLOG_ARCHIVE_SUBDIR);
      binlog_keyid.append(FN_DIRSEP);
      binlog_keyid.append(binlog_relative_file);
      binlog_keyid.append(".");

      objstore::Status ss = binlog_objstore->list_object(
          std::string_view(opt_objstore_bucket), binlog_keyid, objects);
      if (!ss.is_succ() &&
          ss.error_code() != objstore::Errors::SE_NO_SUCH_KEY) {
        err_msg.assign("Failed to binlog files: ");
        err_msg.append(BINLOG_ARCHIVE_SUBDIR);
        err_msg.append(" error=");
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
        break;
      }
      my_off_t max_found = 0, number = 0;
      for (const auto &object : objects) {
        if (is_number(object.key.c_str() + binlog_keyid.length(), &number,
                      false)) {
          max_found = std::max(max_found, number);
        }
      }
      m_binlog_last_event_pos = max_found;
      m_binlog_archive_last_event_pos = m_binlog_last_event_pos;
    }

    archive_binlog();
    // If previous archive binlog slice is not closed, close it.
    if (m_slice_pipeline_head) {
      m_slice_pipeline_head->close();
      m_slice_pipeline_head = nullptr;
    }

    struct timespec abstime;
    set_timespec(&abstime, 3); 
    mysql_mutex_lock(&m_run_lock);
    error = mysql_cond_timedwait(&m_run_cond, &m_run_lock, &abstime);
    mysql_mutex_unlock(&m_run_lock);

    mysql_mutex_lock(&m_index_lock);
    close_index_file();
    mysql_mutex_unlock(&m_index_lock);
  }

end:
  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, "Binlog archive thread end");
  if (binlog_objstore) {
    objstore::destroy_object_store(binlog_objstore);
    binlog_objstore = nullptr;
  }
  // Free new Item_***.
  m_thd->free_items();
  m_thd->release_resources();
  mysql_mutex_lock(&m_run_lock);
  delete m_thd;
  m_thd = nullptr;
  abort_binlog_archive = true;
  m_thd_state.set_terminated();
  mysql_cond_broadcast(&m_run_cond);
  mysql_mutex_unlock(&m_run_lock);
  my_thread_end();
  my_thread_exit(nullptr);
}

int Binlog_archive::archive_init() {
  DBUG_TRACE;
  THD *thd = m_thd;
  char index_entry_name[FN_REFLEN];
  char *name_ptr = nullptr;

  m_diag_area.reset_diagnostics_area();
  m_diag_area.reset_condition_info(thd);
  thd->push_diagnostics_area(&m_diag_area);
  m_mysql_linfo.thread_id = thd->thread_id();
  mysql_bin_log.register_log_info(&m_mysql_linfo);

  /* Initialize the buffer only once. */
  m_packet.mem_realloc(PACKET_MIN_SIZE);  // size of the buffer
  m_new_shrink_size = PACKET_MIN_SIZE;

  if (!mysql_bin_log.is_open()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "Binary log is not open");
    return 1;
  }

  // check start file
  if (m_binlog_archive_file_name[0] != '\0') {
    mysql_bin_log.make_log_name(index_entry_name, m_binlog_archive_file_name);
    name_ptr = index_entry_name;
  }
  if (mysql_bin_log.find_log_pos(&m_mysql_linfo, name_ptr, true)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "Could not find first log file name in binary log "
           "index file");
    return 1;
  }

  if (m_binlog_last_event_pos < BIN_LOG_HEADER_SIZE) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "Client requested source to start replication "
           "from position < 4");
    return 1;
  }

  Binlog_read_error binlog_read_error;
  Binlog_ifile binlog_ifile(&binlog_read_error);
  if (binlog_ifile.open(m_mysql_linfo.log_file_name)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, binlog_read_error.get_str());
    return 1;
  }
  // If the start position is smaller than the file size,
  // it indicates that the file has been truncated by Consensus.
  // If a binlog has been truncated, it indicates that 
  // there are incomplete transaction log in the binlog persisted to S3. 
  // Therefore, we also need to truncate the binlog on S3.
  if (m_binlog_last_event_pos > binlog_ifile.length()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "Client requested source to start replication from "
           "position > file size");
    return 1;
  }
  return 0;
}

int Binlog_archive::read_format_description_event(File_reader &reader) {
  uchar *event_ptr = nullptr;
  uint32 event_len = 0;

  if (reader.read_event_data(&event_ptr, &event_len)) {
    if (reader.get_error_type() == Binlog_read_error::READ_EOF) {
      event_ptr = nullptr;
      event_len = 0;
    } else {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             Binlog_read_error(reader.get_error_type()).get_str());
      return 1;
    }
  }

  if (event_ptr == nullptr ||
      event_ptr[EVENT_TYPE_OFFSET] != binary_log::FORMAT_DESCRIPTION_EVENT) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "Could not find format_description_event in binlog file");
    return 1;
  }

  Log_event *ev = nullptr;
  Binlog_read_error binlog_read_error = binlog_event_deserialize(
      event_ptr, event_len, &reader.format_description_event(), false, &ev);
  if (binlog_read_error.has_error()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, binlog_read_error.get_str());
    return 1;
  }
  reader.set_format_description_event(
      dynamic_cast<Format_description_log_event &>(*ev));
  delete ev;
  return 0;
}
int Binlog_archive::archive_binlog_file(File_reader &reader,
                                        my_off_t start_pos) {
  if (unlikely(read_format_description_event(reader))) return 1;

  /*
    Maybe requesting a position which is in the middle of a file,
    so seek to the correct position.
  */
  if (reader.position() != start_pos && reader.seek(start_pos)) return 1;

  while (!m_thd->killed) {
    auto [end_pos, code] = get_binlog_end_pos(reader);

    if (code) return 1;
    if (archive_events(reader, end_pos)) return 1;

    if (end_pos == 0) return 0;

    m_thd->killed.store(DBUG_EVALUATE_IF(
        "simulate_kill_dump", THD::KILL_CONNECTION, m_thd->killed.load()));
  }
  return 1;
}

int Binlog_archive::archive_binlog() {
  DBUG_TRACE;

  if (archive_init()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "Failed to initialize archive");
    mysql_bin_log.unregister_log_info(&m_mysql_linfo);
    m_thd->clear_error();
    m_thd->pop_diagnostics_area();
    return 1;
  }
  /* Binary event can be vary large. So set it to max allowed packet. */
  unsigned int max_event_size = binary_log::max_log_event_size;
  File_reader reader(opt_source_verify_checksum, max_event_size);
  my_off_t start_pos = m_binlog_last_event_pos;
  const char *log_file = m_mysql_linfo.log_file_name;
  bool is_index_file_reopened_on_binlog_disable = false;

  reader.allocator()->set_archiver(this);
  while (!m_thd->killed) {
    if (reader.open(log_file)) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "Binlog read error");
      break;
    }

    if (archive_binlog_file(reader, start_pos)) break;

    // After completing the archiving of each binlog file, 
    // perform a purge binlog operation.
    auto_purge_logs();

    mysql_bin_log.lock_index();
    if (!mysql_bin_log.is_open()) {
      if (mysql_bin_log.open_index_file(mysql_bin_log.get_index_fname(),
                                        log_file, false)) {
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
               "Binary log is not open and failed to open index file "
               "to retrieve next file.");
        mysql_bin_log.unlock_index();
        break;
      }
      is_index_file_reopened_on_binlog_disable = true;
    }

    int error = mysql_bin_log.find_next_log(&m_mysql_linfo, false);
    mysql_bin_log.unlock_index();
    if (unlikely(error)) {
      if (is_index_file_reopened_on_binlog_disable)
        mysql_bin_log.close(LOG_CLOSE_INDEX, true /*need_lock_log=true*/,
                            true /*need_lock_index=true*/);
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "could not find next log");
      break;
    }

    start_pos = BIN_LOG_HEADER_SIZE;
    reader.close();
  }

  if (reader.is_open()) {
    reader.close();
  }

  mysql_bin_log.unregister_log_info(&m_mysql_linfo);
  m_thd->clear_error();
  m_thd->pop_diagnostics_area();

  return 0;
}

int Binlog_archive::archive_events(File_reader &reader, my_off_t end_pos) {
  DBUG_TRACE;

  THD *thd = m_thd;
  const char *log_file = m_mysql_linfo.log_file_name;
  my_off_t log_pos = reader.position();

  while (likely(log_pos < end_pos) || end_pos == 0) {
    uchar *event_ptr = nullptr;
    uint32 event_len = 0;

    if (unlikely(thd->killed)) return 1;

    if (reset_transmit_packet(0)) return 1;
    size_t event_offset;
    event_offset = m_packet.length();

    if (reader.read_event_data(&event_ptr, &event_len)) {
      if (reader.get_error_type() == Binlog_read_error::READ_EOF) {
        event_ptr = nullptr;
        event_len = 0;
      } else {
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
               Binlog_read_error(reader.get_error_type()).get_str());
        return 1;
      }
    }

    if (event_ptr == nullptr) {
      if (end_pos == 0) return 0;  // Arrive the end of inactive file

      /*
        It is reading events before end_pos of active binlog file. In theory,
        it should never return nullptr. But RESET MASTER doesn't check if there
        is any dump thread working. So it is possible that the active binlog
        file is reopened and truncated to 0 after RESET MASTER.
      */
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "SYSTEM IO error");
      return 1;
    }

    assert(reinterpret_cast<char *>(event_ptr) ==
           (m_packet.ptr() + event_offset));

    DBUG_PRINT("info", ("Read event %s", Log_event::get_type_str(Log_event_type(
                                             (event_ptr)[EVENT_TYPE_OFFSET]))));

    log_pos = reader.position();
    Log_event_type event_type = (Log_event_type)event_ptr[EVENT_TYPE_OFFSET];
    DBUG_EXECUTE_IF("dump_thread_wait_before_send_xid", {
      if (event_type == binary_log::XID_EVENT) {
        thd->get_protocol()->flush();
        const char act[] =
            "now "
            "wait_for signal.continue";
        assert(opt_debug_sync_timeout > 0);
        assert(!debug_sync_set_action(thd, STRING_WITH_LEN(act)));
      }
    });

    if (archive_event(event_ptr, event_len, log_file, log_pos)) return 1;
  }
  return 0;
}

/**
 * @brief Write an binlog event from log_file with log_pos to the archive binlog
 * cache.
 *
 * @param thd
 * @param flags
 * @param packet
 * @param log_file
 * @param log_pos
 * @return int
 */
int Binlog_archive::archive_event(uchar *event_ptr, uint32 event_len,
                                  const char *log_file, my_off_t log_pos) {
  DBUG_TRACE;
  int error = 0;
  assert(log_file != nullptr);
  assert(log_pos >= BIN_LOG_HEADER_SIZE);

  Log_event_type type = (Log_event_type)event_ptr[EVENT_TYPE_OFFSET];

  DBUG_PRINT("info",
             ("Archiving event of type %s", Log_event::get_type_str(type)));

  if (type == binary_log::FORMAT_DESCRIPTION_EVENT) {
    mysql_mutex_lock(&m_rotate_lock);
    // Persist no archived binlog slice.
    if (m_slice_pipeline_head &&
        m_binlog_last_event_pos > m_binlog_archive_last_event_pos) {
      m_slice_pipeline_head->close();
      m_slice_pipeline_head = nullptr;
      // Upload the local archived binlog last slice to the object store.
      if (binlog_objstore) {
        std::string archived_binlog_keyid{};
        char binlog_slice_ext[11] = {0};
        archived_binlog_keyid.assign(BINLOG_ARCHIVE_SUBDIR);
        archived_binlog_keyid.append(FN_DIRSEP);
        archived_binlog_keyid.append(m_binlog_archive_relative_file_name);
        archived_binlog_keyid.append(".");
        sprintf(binlog_slice_ext, "%010llu", m_binlog_last_event_pos);
        // Binlog max size is 1G (1,073,741,824), so the binlog slice file name
        // extension is 10 bytes.
        archived_binlog_keyid.append(binlog_slice_ext);
        objstore::Status ss = binlog_objstore->put_object_from_file(
            std::string_view(opt_objstore_bucket), archived_binlog_keyid,
            m_binlog_archive_slice_name);
        if (!ss.is_succ()) {
          std::string err_msg;
          err_msg.assign("Failed to upload binlog file to object store: ");
          err_msg.append(archived_binlog_keyid);
          err_msg.append(" error=");
          err_msg.append(ss.error_message());
          LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
          error = 1;
          mysql_mutex_unlock(&m_rotate_lock);
          goto end;
        }
        // remove the local archived binlog slice, after upload to the object
        // store.
        remove_file(m_binlog_archive_slice_name);
      }

      if (m_binlog_archive_first_slice) {
        // When we are done with the file, we need to record it in the index.
        // record binlog real file name to the index file.
        mysql_mutex_lock(&m_index_lock);
        if (add_log_to_index((uchar *)m_binlog_archive_file_name,
                             strlen(m_binlog_archive_file_name))) {
          error = 1;
          mysql_mutex_unlock(&m_index_lock);
          mysql_mutex_unlock(&m_rotate_lock);
          goto end;
        }
        mysql_mutex_unlock(&m_index_lock);
        m_binlog_archive_first_slice = false;
      }
    }

    // create archive binlog first slice.
    if (new_binlog_slice(log_file, log_pos)) {
      error = 1;
      mysql_mutex_unlock(&m_rotate_lock);
      goto end;
    }
    m_binlog_archive_first_slice = true;
    // Write BINLOG_MAGIC to the binlog first slice.
    if (m_slice_pipeline_head->write((const uchar *)BINLOG_MAGIC,
                                     BIN_LOG_HEADER_SIZE)) {
      error = 1;
      goto end;
    }
    m_slice_bytes_written = BIN_LOG_HEADER_SIZE;
    m_binlog_last_event_pos = BIN_LOG_HEADER_SIZE;
    m_binlog_archive_last_event_pos = BIN_LOG_HEADER_SIZE;
    // Write Format_description_event to the binlog first slice.
    if (m_slice_pipeline_head->write(event_ptr, event_len)) {
      error = 1;
      mysql_mutex_unlock(&m_rotate_lock);
      goto end;
    }
    m_slice_bytes_written += event_len;
    // Update the last archived event end of position.
    m_binlog_last_event_pos = uint4korr(event_ptr + LOG_POS_OFFSET);
    m_slice_pipeline_head->flush();
    mysql_mutex_unlock(&m_rotate_lock);
    goto suc;
  }

  mysql_mutex_lock(&m_rotate_lock);

  // If slice not exists, create a new slice.
  if (!m_slice_pipeline_head) {
    assert(log_pos != BIN_LOG_HEADER_SIZE);
    // create archive binlog first slice.
    if (new_binlog_slice(log_file, log_pos)) {
      error = 1;
      mysql_mutex_unlock(&m_rotate_lock);
      goto end;
    }
  }

  assert(m_slice_pipeline_head != nullptr);
  if (m_slice_pipeline_head->write(event_ptr, event_len)) {
    error = 1;
    mysql_mutex_unlock(&m_rotate_lock);
    goto end;
  }
  m_slice_bytes_written += event_len;
  // Record the last archived event end of position.
  m_binlog_last_event_pos = uint4korr(event_ptr + LOG_POS_OFFSET);

  // rotate binlog slice, if the binlog slice size is too large.
  if (m_slice_bytes_written >= opt_binlog_archive_slice_max_size) {
    if (rotate_binlog_slice(m_binlog_last_event_pos, false)) {
      error = 1;
      mysql_mutex_unlock(&m_rotate_lock);
      goto end;
    }
  }
  m_slice_pipeline_head->flush();
  mysql_mutex_unlock(&m_rotate_lock);

suc:
  // Signal update after every event.
  // Actually, it's enough to send a signal only when rotating the binlog
  // file.
  signal_update();

end:
  return error;
}

bool Binlog_archive::stop_waiting_for_mysql_binlog_update(
    my_off_t log_pos) const {
  if (mysql_bin_log.get_binlog_end_pos() > log_pos ||
      !mysql_bin_log.is_active(m_mysql_linfo.log_file_name) || m_thd->killed) {
    return true;
  }
  return false;
}

int Binlog_archive::wait_new_mysql_binlog_events(my_off_t log_pos) {
  int ret = 0;

  /*
    MYSQL_BIN_LOG::binlog_end_pos is atomic. We should only acquire the
    LOCK_binlog_end_pos if we reached the end of the hot log and are going
    to wait for updates on the binary log (Binlog_sender::wait_new_event()).
  */
  if (stop_waiting_for_mysql_binlog_update(log_pos)) {
    return 0;
  }

  mysql_bin_log.lock_binlog_end_pos();
  m_thd->ENTER_COND(mysql_bin_log.get_log_cond(),
                    mysql_bin_log.get_binlog_end_pos_lock(), nullptr, nullptr);
  while (!stop_waiting_for_mysql_binlog_update(log_pos)) {
    ret = mysql_bin_log.wait_for_update();
  }
  mysql_bin_log.unlock_binlog_end_pos();
  m_thd->EXIT_COND(nullptr);

  return ret;
}

std::pair<my_off_t, int> Binlog_archive::get_binlog_end_pos(
    File_reader &reader) {
  DBUG_TRACE;
  my_off_t read_pos = reader.position();

  std::pair<my_off_t, int> result = std::make_pair(read_pos, 1);

  if (unlikely(wait_new_mysql_binlog_events(read_pos))) return result;

  result.first = mysql_bin_log.get_binlog_end_pos();

  DBUG_PRINT("info", ("Reading file %s, seek pos %llu, end_pos is %llu",
                      m_mysql_linfo.log_file_name, read_pos, result.first));
  DBUG_PRINT("info", ("Active file is %s", mysql_bin_log.get_log_fname()));

  /* If this is a cold binlog file, we are done getting the end pos */
  if (unlikely(!mysql_bin_log.is_active(m_mysql_linfo.log_file_name))) {
    return std::make_pair(0, 0);
  }
  if (read_pos < result.first) {
    result.second = 0;
    return result;
  }
  return result;
}

/**
 * @brief Create a new binlog slice local file.
 *
 * @param log_file
 * @param log_pos
 * @return int
 * @note Must be called with m_rotate_lock held.
 */
int Binlog_archive::new_binlog_slice(const char *log_file,
                                     my_off_t log_pos [[maybe_unused]]) {
  int error = 0;
  // rotate new archive binlog first slice.
  assert(log_file != nullptr);
  // Get the next archive binlog file name.
  strmake(m_binlog_archive_file_name, log_file,
          sizeof(m_binlog_archive_file_name) - 1);

  // m_binlog_archive_file_name could be a absolute path.
  // get relative path. Check if the binlog file name's first char
  // is "/"
  if (m_binlog_archive_file_name[0] == FN_LIBCHAR) {
    strmake(m_binlog_archive_relative_file_name, m_binlog_archive_file_name + 1,
            sizeof(m_binlog_archive_relative_file_name) - 1);
  } else if (m_binlog_archive_file_name[0] == FN_CURLIB &&
             m_binlog_archive_file_name[1] == FN_LIBCHAR) {
    strmake(m_binlog_archive_relative_file_name, m_binlog_archive_file_name + 2,
            sizeof(m_binlog_archive_relative_file_name) - 1);
  } else {
    strmake(m_binlog_archive_relative_file_name, m_binlog_archive_file_name,
            sizeof(m_binlog_archive_relative_file_name) - 1);
  }

  assert(m_binlog_archive_dir[0] != '\0');
  m_binlog_archive_slice_name.assign(m_binlog_archive_dir);
  m_binlog_archive_slice_name.append(m_binlog_archive_relative_file_name);
  // split binlog to slices, if the binlog file size is too large.
  // create IO_CACHE for binlog slice.
  // Open the first binlog slice file.
  m_binlog_archive_slice_name.append(".slice");
  remove_file(m_binlog_archive_slice_name);
  std::unique_ptr<IO_CACHE_ostream> file_ostream(new IO_CACHE_ostream);
  if (file_ostream->open(PSI_binlog_archive_log_file_slice_key,
                         m_binlog_archive_slice_name.c_str(), MYF(0))) {
    error = 1;
    goto end;
  }
  m_slice_pipeline_head = std::move(file_ostream);

end:
  return error;
}
int Binlog_archive::rotate_binlog_slice(my_off_t log_pos, bool need_lock) {
  int error = 0;
  if (need_lock) {
    mysql_mutex_lock(&m_rotate_lock);
  }

  // log_pos already archived.
  if (m_binlog_archive_last_event_pos >= log_pos) {
    goto end;
  }

  if (m_binlog_last_event_pos < log_pos) {
    error = 1;
    goto end;
  }

  if (m_slice_pipeline_head) {
    m_slice_pipeline_head->close();
    m_slice_pipeline_head = nullptr;
    // Upload the local archived binlog slice to the object store.
    if (binlog_objstore) {
      size_t len;
      std::string archived_binlog_keyid{};
      char binlog_slice_ext[11] = {0};
      archived_binlog_keyid.assign(BINLOG_ARCHIVE_SUBDIR);
      archived_binlog_keyid.append(FN_DIRSEP);
      archived_binlog_keyid.append(m_binlog_archive_relative_file_name);
      archived_binlog_keyid.append(".");
      len = snprintf(binlog_slice_ext, sizeof(binlog_slice_ext), "%010llu",
                     m_binlog_last_event_pos);
      assert(len > 0);
      archived_binlog_keyid.append(binlog_slice_ext);
      objstore::Status ss = binlog_objstore->put_object_from_file(
          std::string_view(opt_objstore_bucket), archived_binlog_keyid,
          m_binlog_archive_slice_name);
      if (!ss.is_succ()) {
        std::string err_msg;
        err_msg.assign("Failed to upload binlog slice to object store: ");
        err_msg.append(archived_binlog_keyid);
        err_msg.append(" error=");
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
        error = 1;
        goto end;
      }
      // remove the local archived binlog slice, after upload to the object
      // store.
      remove_file(m_binlog_archive_slice_name);
    }
    // When we are uploaded the binlog first slice to object store,
    // we need to refresh the binlog.index.
    if (m_binlog_archive_first_slice) {
      // When we are done with the file, we need to record it in the index.
      // record binlog real file name to the index file.
      mysql_mutex_lock(&m_index_lock);
      if (add_log_to_index((uchar *)m_binlog_archive_file_name,
                           strlen(m_binlog_archive_file_name))) {
        error = 1;
        mysql_mutex_unlock(&m_index_lock);
        goto end;
      }
      mysql_mutex_unlock(&m_index_lock);
    }
    m_binlog_archive_first_slice = false;
    m_binlog_archive_last_event_pos = m_binlog_last_event_pos;

    std::unique_ptr<IO_CACHE_ostream> file_ostream(new IO_CACHE_ostream);
    if (file_ostream->open(PSI_binlog_archive_log_file_slice_key,
                           m_binlog_archive_slice_name.c_str(), MYF(0))) {
      error = 1;
      goto end;
    }
    m_slice_pipeline_head = std::move(file_ostream);
    m_slice_bytes_written = 0;
    m_slice_pipeline_head->flush();
  }
  assert(m_binlog_archive_last_event_pos >= log_pos);

end:
  if (need_lock) {
    mysql_mutex_unlock(&m_rotate_lock);
  }
  return error;
}

/**
 * @brief Flush the archive binlog file.
 *
 * @return int
 */
int Binlog_archive::flush_events() {
  DBUG_TRACE;
  mysql_mutex_lock(&m_rotate_lock);
  if (m_slice_pipeline_head) {
    m_slice_pipeline_head->flush();
    ;
  }
  mysql_mutex_unlock(&m_rotate_lock);
  return 0;
}

/**
  Copy content of 'from' file from offset to 'to' file.

  - We do the copy outside of the IO_CACHE as the cache
  buffers would just make things slower and more complicated.
  In most cases the copy loop should only do one read.

  @param from          File to copy.
  @param to            File to copy to.
  @param offset        Offset in 'from' file.


  @retval
    0    ok
  @retval
    -1    error
*/
static bool copy_file(IO_CACHE *from, IO_CACHE *to, my_off_t offset) {
  int bytes_read = 0;
  uchar io_buf[IO_SIZE * 2] = {0};
  DBUG_TRACE;

  mysql_file_seek(from->file, offset, MY_SEEK_SET, MYF(0));
  while (true) {
    if ((bytes_read = (int)mysql_file_read(from->file, io_buf, sizeof(io_buf),
                                           MYF(MY_WME))) < 0)
      goto err;
    if (!bytes_read) break;  // end of file
    if (mysql_file_write(to->file, io_buf, bytes_read, MYF(MY_WME | MY_NABP)))
      goto err;
  }

  return false;

err:
  return true;
}

/**
  Open a (new) crash safe index file.

  @note
    The crash safe index file is a special file
    used for guaranteeing index file crash safe.
  @retval
    0   ok
  @retval
    1   error
*/
int Binlog_archive::open_crash_safe_index_file() {
  int error = 0;
  File file = -1;
  DBUG_TRACE;

  if (!my_b_inited(&m_crash_safe_index_file)) {
    myf flags = MY_WME | MY_NABP | MY_WAIT_IF_FULL;

    if ((file = my_open(m_crash_safe_index_file_name, O_RDWR | O_CREAT,
                        MYF(MY_WME))) < 0 ||
        init_io_cache(&m_crash_safe_index_file, file, IO_SIZE, WRITE_CACHE, 0,
                      false, flags)) {
      error = 1;
    }
  }
  return error;
}

/**
  Set the name of crash safe index file.

  @retval
    0   ok
  @retval
    1   error
*/
int Binlog_archive::set_crash_safe_index_file_name() {
  int error = 0;
  DBUG_TRACE;
  if (fn_format(m_crash_safe_index_file_name, BINLOG_ARCHIVE_INDEX_FILE,
                m_binlog_archive_dir, ".index_crash_safe",
                MYF(MY_UNPACK_FILENAME | MY_SAFE_PATH | MY_REPLACE_EXT |
                    MY_REPLACE_DIR)) == nullptr) {
    error = 1;
  }
  return error;
}

/**
  Close the crash safe index file.

  @note
    The crash safe file is just closed, is not deleted.
    Because it is moved to index file later on.
  @retval
    0   ok
  @retval
    1   error
*/
int Binlog_archive::close_crash_safe_index_file() {
  int error = 0;
  DBUG_TRACE;

  if (my_b_inited(&m_crash_safe_index_file)) {
    end_io_cache(&m_crash_safe_index_file);
    error = my_close(m_crash_safe_index_file.file, MYF(0));
  }
  m_crash_safe_index_file = IO_CACHE();

  return error;
}

/**
  Move crash safe index file to index file.

  @param need_lock_index If true, m_index_lock will be acquired;
  otherwise it should already be held.

  @retval 0 ok
  @retval -1 error
*/
int Binlog_archive::move_crash_safe_index_file_to_index_file() {
  int error = 0;
  File fd = -1;
  DBUG_TRACE;
  int failure_trials = MYSQL_BIN_LOG::MAX_RETRIES_FOR_DELETE_RENAME_FAILURE;
  bool file_rename_status = false, file_delete_status = false;
  THD *thd = m_thd;

  if (my_b_inited(&m_index_file)) {
    end_io_cache(&m_index_file);
    if (mysql_file_close(m_index_file.file, MYF(0)) < 0) {
      error = -1;
      /*
        Delete Crash safe index file here and recover the binlog.index
        state(m_index_file io_cache) from old binlog.index content.
       */
      mysql_file_delete(key_file_binlog_index, m_crash_safe_index_file_name,
                        MYF(0));

      goto recoverable_err;
    }

    while ((file_delete_status == false) && (failure_trials > 0)) {
      file_delete_status = !(mysql_file_delete(key_file_binlog_index,
                                               m_index_file_name, MYF(MY_WME)));
      --failure_trials;
      if (!file_delete_status) {
        my_sleep(1000);
        /* Clear the error before retrying. */
        if (failure_trials > 0) thd->clear_error();
      }
    }

    if (!file_delete_status) {
      error = -1;
      /*
        Delete Crash safe file index file here and recover the binlog.index
        state(m_index_file io_cache) from old binlog.index content.
       */
      mysql_file_delete(key_file_binlog_index, m_crash_safe_index_file_name,
                        MYF(0));

      goto recoverable_err;
    }
  }

  failure_trials = MYSQL_BIN_LOG::MAX_RETRIES_FOR_DELETE_RENAME_FAILURE;
  while ((file_rename_status == false) && (failure_trials > 0)) {
    file_rename_status = !(my_rename(m_crash_safe_index_file_name,
                                     m_index_file_name, MYF(MY_WME)));
    --failure_trials;
    if (!file_rename_status) {
      my_sleep(1000);
      /* Clear the error before retrying. */
      if (failure_trials > 0) thd->clear_error();
    }
  }
  if (!file_rename_status) {
    error = -1;
    goto fatal_err;
  }

recoverable_err:
  if ((fd = mysql_file_open(key_file_binlog_index, m_index_file_name,
                            O_RDWR | O_CREAT, MYF(MY_WME))) < 0 ||
      mysql_file_sync(fd, MYF(MY_WME)) ||
      init_io_cache_ext(&m_index_file, fd, IO_SIZE, READ_CACHE,
                        mysql_file_seek(fd, 0L, MY_SEEK_END, MYF(0)), false,
                        MYF(MY_WME | MY_WAIT_IF_FULL),
                        key_file_binlog_index_cache)) {
    goto fatal_err;
  }

  // if (need_lock_index) mysql_mutex_unlock(&m_index_lock);
  return error;

fatal_err:
  /*
    This situation is very very rare to happen (unless there is some serious
    memory related issues like OOM) and should be treated as fatal error.
    Hence it is better to bring down the server without respecting
    'binlog_error_action' value here.
  */
  if (binlog_error_action == ABORT_SERVER) {
    exec_binlog_error_action_abort(
        "MySQL server failed to update the "
        "archive binlog.index file's content properly. "
        "It might not be in sync with available "
        "binlogs and the binlog.index file state is in "
        "unrecoverable state. Aborting the server.");
  } else {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "MySQL server failed to update the archive binlog.index file's "
           "content properly. It might not be in sync with available binlogs "
           "and the binlog.index file state is in unrecoverable state.");
  }
  return error;
}

/**
  @brief Append log file name to index file.

  - To make crash safe, we copy all the content of index file
  to crash safe index file firstly and then append the log
  file name to the crash safe index file. Then upload index file to object
  store. Finally move the crash safe index file to index file.
  If failed, we will recover the index file from object store.
  @note Must m_index_lock held.
  @retval
    0   ok
  @retval
    -1   error
*/
int Binlog_archive::add_log_to_index(uchar *log_name, size_t log_name_len) {
  DBUG_TRACE;

  if (!my_b_inited(&m_index_file)) {
    return LOG_INFO_IO;
  }

  if (open_crash_safe_index_file()) {
    goto err;
  }

  if (copy_file(&m_index_file, &m_crash_safe_index_file, 0)) {
    goto err;
  }

  if (my_b_write(&m_crash_safe_index_file, log_name, log_name_len) ||
      my_b_write(&m_crash_safe_index_file, pointer_cast<const uchar *>("\n"),
                 1) ||
      flush_io_cache(&m_crash_safe_index_file) ||
      mysql_file_sync(m_crash_safe_index_file.file, MYF(MY_WME))) {
    goto err;
  }

  if (close_crash_safe_index_file()) {
    goto err;
  }

  if (binlog_objstore != nullptr) {
    // refresh 'mysql-bin.index' to object store
    std::string index_keyid{};
    index_keyid.assign(BINLOG_ARCHIVE_SUBDIR);
    index_keyid.append(FN_DIRSEP);
    index_keyid.append(BINLOG_ARCHIVE_INDEX_FILE);
    objstore::Status ss = binlog_objstore->put_object_from_file(
        std::string_view(opt_objstore_bucket), index_keyid,
        m_crash_safe_index_file_name);
    if (!ss.is_succ()) {
      std::string err_msg;
      err_msg.assign("Failed to upload clone index file to object store: ");
      err_msg.append(index_keyid);
      err_msg.append(" error =");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      goto err;
    }
  }

  // If failed, restart the binlog archiving from run()->while{}.
  // Will recover the index file from object store.
  if (move_crash_safe_index_file_to_index_file()) {
    goto err;
  }

  return 0;

err:
  return -1;
}

/**
 * @brief  Open the binlog archive index file.
 *
 * @note  Mutex be called with m_index_lock held.
 * @return true, if error
 * @return false, if success
 */
bool Binlog_archive::open_index_file() {
  bool error = false;
  File index_file_nr = -1;

  /*
    First open of this class instance
    Create an index file that will hold all file names uses for logging.
    Add new entries to the end of it.
  */
  myf opt = MY_UNPACK_FILENAME | MY_REPLACE_DIR;

  if (my_b_inited(&m_index_file)) goto end;

  fn_format(m_index_file_name, BINLOG_ARCHIVE_INDEX_FILE, m_binlog_archive_dir,
            ".index", opt);

  if (set_crash_safe_index_file_name()) {
    error = true;
    goto end;
  }

  /*
    We need move m_crash_safe_index_file to m_index_file if the m_index_file
    does not exist and m_crash_safe_index_file exists when mysqld server
    restarts.
  */
  if (my_access(m_index_file_name, F_OK) &&
      !my_access(m_crash_safe_index_file_name, F_OK) &&
      my_rename(m_crash_safe_index_file_name, m_index_file_name, MYF(MY_WME))) {
    error = true;
    goto end;
  }

  // Check if the index file exists in s3, if so, download it to local.
  // And rename it to m_index_file_name.
  if (binlog_objstore != nullptr) {
    std::string index_file_name_str{};
    index_file_name_str.assign(m_crash_safe_index_file_name);
    std::string index_keyid{};
    index_keyid.assign(BINLOG_ARCHIVE_SUBDIR);
    index_keyid.append(FN_DIRSEP);
    index_keyid.append(BINLOG_ARCHIVE_INDEX_FILE);
    auto status = binlog_objstore->get_object_to_file(
        std::string_view(opt_objstore_bucket), index_keyid,
        index_file_name_str);
    if (status.is_succ()) {
      if (my_rename(m_crash_safe_index_file_name, m_index_file_name,
                    MYF(MY_WME))) {
        error = true;
        goto end;
      }
      LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             "download binlog index file from object store.");
    }
    // TODO: should check error code, if failed because of NOT_FOUND, should
    // continue to create a new index file. Otherwise, should report error.
  }

  if ((index_file_nr =
           mysql_file_open(PSI_binlog_archive_log_index_key, m_index_file_name,
                           O_RDWR | O_CREAT, MYF(MY_WME))) < 0 ||
      mysql_file_sync(index_file_nr, MYF(MY_WME)) ||
      init_io_cache_ext(&m_index_file, index_file_nr, IO_SIZE, READ_CACHE,
                        mysql_file_seek(index_file_nr, 0L, MY_SEEK_END, MYF(0)),
                        false, MYF(MY_WME | MY_WAIT_IF_FULL),
                        PSI_binlog_archive_log_index_cache_key)) {
    /*
      TODO: all operations creating/deleting the index file or a log, should
      call my_sync_dir() or my_sync_dir_by_file() to be durable.
      TODO: file creation should be done with mysql_file_create()
      not mysql_file_open().
    */
    if (index_file_nr >= 0) {
      mysql_file_close(index_file_nr, MYF(0));
    }
    error = true;
    goto end;
  }

  /*
    Sync the index by purging any binary log file that is not registered.
    In other words, either purge binary log files that were removed from
    the index but not purged from the file system due to a crash or purge
    any binary log file that was created but not register in the index
    due to a crash.
  */
  if (set_purge_index_file_name() || open_purge_index_file(false) ||
      purge_index_entry(nullptr) || close_purge_index_file()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "Failed to set purge index file.");
    error = true;
    goto end;
  }
  atomic_log_index_state = LOG_INDEX_OPENED;

end:
  // if (need_lock_index) mysql_mutex_unlock(&m_index_lock);
  return error;
}

/**
 * @brief Closes the binlog archive index file.
 * @note
 *  Mutex needed because we need to make sure the file pointer does not move
    from under our feet
 * @return int
 */
void Binlog_archive::close_index_file() {
  DBUG_TRACE;

  atomic_log_index_state = LOG_INDEX_CLOSED;
  if (my_b_inited(&m_index_file)) {
    end_io_cache(&m_index_file);
    if (mysql_file_close(m_index_file.file, MYF(0)))
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             "Failed to close binlog index file");
  }
}

static int compare_log_name(const char *log_1, const char *log_2) {
  const char *log_1_basename = log_1 + dirname_length(log_1);
  const char *log_2_basename = log_2 + dirname_length(log_2);

  return strcmp(log_1_basename, log_2_basename);
}

/**
  Find the position in the log-index-file for the given log name.

  @param[out] linfo The found log file name will be stored here, along
  with the byte offset of the next log file name in the index file.
  @param log_name Filename to find in the index file, or NULL if we
  want to read the first entry.
  @param need_lock_index If false, this function acquires m_index_lock;
  otherwise the lock should already be held by the caller.

  @note
    On systems without the truncate function the file will end with one or
    more empty lines.  These will be ignored when reading the file.

  @retval
    0			ok
  @retval
    LOG_INFO_EOF	        End of log-index-file found
  @retval
    LOG_INFO_IO		Got IO error while reading file
*/
int Binlog_archive::find_log_pos_common(IO_CACHE *index_file, LOG_INFO *linfo,
                                        const char *log_name) {
  DBUG_TRACE;
  int error = 0;
  char *full_fname = linfo->log_file_name;
  char full_log_name[FN_REFLEN] = {0}, fname[FN_REFLEN] = {0};

  // extend relative paths for log_name to be searched
  if (log_name) {
    if (normalize_binlog_name(full_log_name, log_name, false)) {
      error = LOG_INFO_EOF;
      goto end;
    }
  }

  DBUG_PRINT("enter", ("log_name: %s, full_log_name: %s",
                       log_name ? log_name : "NULL", full_log_name));

  /* As the file is flushed, we can't get an error here */
  my_b_seek(index_file, (my_off_t)0);

  for (;;) {
    size_t length = 0;
    my_off_t offset = my_b_tell(index_file);

    /* If we get 0 or 1 characters, this is the end of the file */
    if ((length = my_b_gets(index_file, fname, FN_REFLEN)) <= 1) {
      /* Did not find the given entry; Return not found or error */
      error = !index_file->error ? LOG_INFO_EOF : LOG_INFO_IO;
      break;
    }

    // extend relative paths and match against full path
    if (normalize_binlog_name(full_fname, fname, false)) {
      error = LOG_INFO_EOF;
      break;
    }
    // if the log entry matches, null string matching anything
    if (!log_name || !compare_log_name(full_fname, full_log_name)) {
      DBUG_PRINT("info", ("Found log file entry"));
      linfo->index_file_start_offset = offset;
      linfo->index_file_offset = my_b_tell(index_file);
      break;
    }
    linfo->entry_index++;
  }

end:
  return error;
}
int Binlog_archive::find_log_pos(LOG_INFO *linfo, const char *log_name) {
  DBUG_TRACE;

  if (!my_b_inited(&m_index_file)) {
    return LOG_INFO_IO;
  }
  return find_log_pos_common(&m_index_file, linfo, log_name);
}

/**
  Find the position in the log-index-file for the given log name.

  @param[out] linfo The filename will be stored here, along with the
  byte offset of the next filename in the index file.

  @param need_lock_index If true, m_index_lock will be acquired;
  otherwise it should already be held by the caller.

  @note
    - Before calling this function, one has to call find_log_pos()
    to set up 'linfo'
    - Mutex needed because we need to make sure the file pointer does not move
    from under our feet

  @retval 0 ok
  @retval LOG_INFO_EOF End of log-index-file found
  @retval LOG_INFO_IO Got IO error while reading file
*/
int Binlog_archive::find_next_log(LOG_INFO *linfo) {
  DBUG_TRACE;

  if (!my_b_inited(&m_index_file)) {
    return LOG_INFO_IO;
  }
  return find_next_log_common(&m_index_file, linfo);
}

int Binlog_archive::find_next_log_common(IO_CACHE *index_file,
                                         LOG_INFO *linfo) {
  DBUG_TRACE;

  int error = 0;
  size_t length = 0;
  char fname[FN_REFLEN] = {0};
  char *full_fname = linfo->log_file_name;

  if (!my_b_inited(index_file)) {
    error = LOG_INFO_IO;
    goto err;
  }
  /* As the file is flushed, we can't get an error here */
  my_b_seek(index_file, linfo->index_file_offset);

  linfo->index_file_start_offset = linfo->index_file_offset;
  if ((length = my_b_gets(index_file, fname, FN_REFLEN)) <= 1) {
    error = !index_file->error ? LOG_INFO_EOF : LOG_INFO_IO;
    goto err;
  }

  if (fname[0] != 0) {
    if (normalize_binlog_name(full_fname, fname, false)) {
      error = LOG_INFO_EOF;
      goto err;
    }
    length = strlen(full_fname);
  }

  linfo->index_file_offset = my_b_tell(index_file);

err:
  return error;
}

/**
 * @brief Check if the given log file is archived.
 * @note Must be called with m_index_lock held.
 */
bool Binlog_archive::binlog_is_archived(const char *log_file_name_arg,
                                        my_off_t log_pos) {
  DBUG_TRACE;
  bool ret = true;
  LOG_INFO log_info;
  // If the binlog file is the current binlog file.
  // rotate binlog slice by log_pos.
  mysql_mutex_lock(&m_rotate_lock);
  if (0 == compare_log_name(m_binlog_archive_file_name, log_file_name_arg)) {
    if (rotate_binlog_slice(log_pos, false)) {
      ret = false;
    }
    mysql_mutex_unlock(&m_rotate_lock);
    return ret;
  }
  mysql_mutex_lock(&m_index_lock);
  // If the log index file is not opened, return true.
  // Wait binlog archive thread to open the index file.
  if ((atomic_log_index_state != LOG_INDEX_OPENED) ||
      (!open_index_file() && find_log_pos(&log_info, log_file_name_arg))) {
    ret = false;
  }

  mysql_mutex_unlock(&m_index_lock);
  mysql_mutex_unlock(&m_rotate_lock);
  return ret;
}

/**
 * @brief Signal other threads that the binlog archive is updated.
 *
 */
void Binlog_archive::signal_update() { mysql_cond_broadcast(&m_run_cond); }

/**
 * @brief Wait for the binlog file and position is archived.
 * @param log_file_name
 * @param log_pos
 * @note Must be called with m_run_lock held.
 * @return true
 * @return false
 */
bool Binlog_archive::stop_waiting_for_update(char *log_file_name,
                                             my_off_t log_pos
                                             [[maybe_unused]]) {
  DBUG_TRACE;

  bool error = false;
  error = binlog_is_archived(log_file_name, log_pos);
  return error;
}

/**
 * @brief Wait for the binlog archive to finish.
 *
 * @return int
 */
int Binlog_archive::wait_for_update() {
  DBUG_TRACE;
  mysql_mutex_assert_owner(&m_run_lock);
  struct timespec abstime;
  set_timespec(&abstime, 1);
  return mysql_cond_timedwait(&m_run_cond, &m_run_lock, &abstime);
}

/**
 * @brief For consistent snapshot arhcive,
 *        consistent snapshot thread wait for the binlog archive to finish.
 * */
int binlog_archive_wait_for_update(THD *thd, char *log_file_name,
                                   my_off_t log_pos) {
  DBUG_TRACE;
  int ret = 0;
  if (!opt_binlog_archive) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "Binlog archive is not enabled");
    return 0;
  }
  if (!mysql_binlog_archive) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "Binlog archive object is not initialized");
    return 1;
  }
  mysql_mutex_lock(&m_run_lock);
  if (!mysql_binlog_archive->is_thread_running()) {
    mysql_mutex_unlock(&m_run_lock);
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "Binlog archive thread is not running");
    return 1;
  }
  thd->ENTER_COND(&m_run_cond, &m_run_lock, nullptr, nullptr);
  // Check if waited binlog file is archived.
  while (
      !mysql_binlog_archive->stop_waiting_for_update(log_file_name, log_pos)) {
    mysql_binlog_archive->wait_for_update();
    // binlog archive thread maybe terminated.
    if (!mysql_binlog_archive->is_thread_running()) {
      ret = 1;
      break;
    }
  }
  mysql_mutex_unlock(&m_run_lock);
  thd->EXIT_COND(nullptr);
  return ret;
}

/**
 * @brief Remove file or directory.
 * @param file file or directory name.
 * @return true, if error.
 * @return false, if success or file not exists.
 */
static bool remove_file(const std::string &file) {
  DBUG_TRACE;

  uint i = 0;
  MY_DIR *dir = nullptr;
  MY_STAT stat_area;

  if (!my_stat(file.c_str(), &stat_area, MYF(0))) return false;
  if (!MY_S_ISDIR(stat_area.st_mode)) {
    if (my_delete(file.c_str(), MYF(MY_WME))) return true;
    return false;
  }

  char dir_path[FN_REFLEN + 1] = {0};
  strmake(dir_path, file.c_str(), sizeof(dir_path) - 1);
  convert_dirname(dir_path, dir_path, NullS);

  // open directory.
  if (!(dir = my_dir(dir_path, MYF(MY_DONT_SORT | MY_WANT_STAT)))) {
    return true;
  }

  // Iterate through the source directory.
  for (i = 0; i < (uint)dir->number_off_files; i++) {
    // Skip . and ..
    if (strcmp(dir->dir_entry[i].name, ".") == 0 ||
        strcmp(dir->dir_entry[i].name, "..") == 0) {
      continue;
    }
    std::string tmp_sub;
    tmp_sub.assign(dir_path);
    tmp_sub.append(dir->dir_entry[i].name);
    if (remove_file(tmp_sub)) {
      return true;
    }
  }
  my_dirend(dir);

  // remove the directory.
  if (rmdir(dir_path)) {
    return true;
  }

  return false;
}

/**
 * @brief Get the root directory and left string.
 * @param in_str input string.
 * e.g "abc/def/ghi"
 * @param out_str output root directory from in_str. or empty string.
 * e.g "abc/"
 * @param left_str output left string from in_str. or in_str.
 * e.g "def/ghi"
 */
static void root_directory(std::string in_str, std::string *out_str,
                           std::string *left_str) {
  DBUG_TRACE;
  size_t idx = in_str.find(FN_DIRSEP);
  if (idx == std::string::npos) {
    out_str->assign("");
    left_str->assign(in_str);
  } else {
    // out string include the last FN_DIRSEP.
    out_str->assign(in_str.substr(0, idx + 1));
    // left string.
    left_str->assign(in_str.substr(idx + 1));
  }
}

/**
 * @brief Create directory recursively under the root directory.
 * e.g make -p "/u01/abc/def/ghi/"
 * @param dir e.g "adc/def/ghi/file.txt"
 * @param root e.g "/u01/"
 */
static bool recursive_create_dir(const std::string &dir,
                                 const std::string &root) {
  DBUG_TRACE;
  std::string out;
  std::string left;
  root_directory(dir, &out, &left);
  if (out.empty()) {
    return false;
  }
  std::string real_path;

  real_path.assign(root);
  real_path.append(out);
  MY_STAT stat_area;
  if (!my_stat(real_path.c_str(), &stat_area, MYF(0))) {
    if (my_mkdir(real_path.c_str(), 0777, MYF(0))) {
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             "Failed to create binlog archive dir.");
      return true;
    }
  }
  return recursive_create_dir(left, real_path);
}

int Binlog_archive::auto_purge_logs() {
  auto purge_time = calculate_auto_purge_lower_time_bound();
  char purge_to_log[FN_REFLEN + 1] = {0};
  bool found = false;
  std::vector<objstore::ObjectMeta> binlog_objects; 
  show_binlog_persistent_files(binlog_objects);
  for (const auto &object : binlog_objects) {
    if (purge_time < (object.last_modified / 1000)) {
      break;
    }
    strmake(purge_to_log, object.key.c_str(), sizeof(purge_to_log) - 1);
    found = true;
  }
  if (found) {
    std::string tmp_str;
    char dev[FN_REFLEN + 1];
    size_t dev_length;
    size_t length = dirname_part(dev, purge_to_log, &dev_length);
    tmp_str.assign(purge_to_log + length);
    tmp_str = tmp_str.substr(0, tmp_str.length() - strlen(".") - 10);
    strmake(purge_to_log, tmp_str.c_str(), sizeof(purge_to_log) - 1);
    purge_logs(purge_to_log, nullptr);
  }
  return 0; 
}

/**
 * @brief Purge the pesistent binlog archive file.
 * @param to_log
 * @param decrease_log_space
 * @return std::tuple<bool, std::string>
 */
std::tuple<int, std::string> Binlog_archive::purge_logs(
    const char *to_log, ulonglong *decrease_log_space) {
  DBUG_TRACE;
  int error = 0;
  std::string err_msg{};
  LOG_INFO log_info;
  char purge_log_name[FN_REFLEN + 1] = {0};
  DBUG_PRINT("info", ("to_log= %s", to_log));

  mysql_bin_log.make_log_name(purge_log_name, to_log);

  // Check consistent snapshot, only when start consistent snapshot.
  if (opt_consistent_snapshot_archive && opt_serverless) {
    IO_CACHE *index_file;
    char snapshot_info[4 * FN_REFLEN];
    size_t snapshot_info_len;
    Consistent_archive *consistent_snapshot_archive =
        Consistent_archive::get_instance();
    // If consistent snapshot thread is dead, can't purge binlog.
    if (consistent_snapshot_archive->is_thread_dead()) {
      return std::make_tuple(error, err_msg);
    }

    consistent_snapshot_archive->lock_consistent_snapshot_index();
    index_file =
        consistent_snapshot_archive->get_consistent_snapshot_index_file();
    if (!my_b_inited(index_file) ||
        reinit_io_cache(index_file, READ_CACHE, (my_off_t)0, false, false)) {
      consistent_snapshot_archive->unlock_consistent_snapshot_index();
      error = 1;
      err_msg.assign("consistent_snapshot.index is not opened.");
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      return std::make_tuple(error, err_msg);
    }

    snapshot_info_len =
        my_b_gets(index_file, snapshot_info, sizeof(snapshot_info));
    if (snapshot_info_len > 1) {
      snapshot_info[--snapshot_info_len] = '\0';  // remove the newline
      std::string in_str;
      in_str.assign(snapshot_info);
      size_t idx = in_str.find("|");
      std::string ts = in_str.substr(0, idx);
      std::string left_string = in_str.substr(idx + 1);

      idx = left_string.find("|");
      std::string innodb_name = left_string.substr(0, idx);
      left_string = left_string.substr(idx + 1);

      idx = left_string.find("|");
      std::string se_name = left_string.substr(0, idx);
      left_string = left_string.substr(idx + 1);

      idx = left_string.find("|");
      std::string binlog_name = left_string.substr(0, idx);
      left_string = left_string.substr(idx + 1);

      char binlog_name_tmp[FN_REFLEN + 1] = {0};
      mysql_bin_log.make_log_name(binlog_name_tmp, binlog_name.c_str());
      // Only purge binlog smaller than the oldest snapshot's binlog.
      if (compare_log_name(purge_log_name, binlog_name_tmp)) {
        strmake(purge_log_name, binlog_name_tmp, sizeof(purge_log_name) - 1);
        err_msg.assign("purge binlog smaller than the oldest snapshot's binlog: ");
        err_msg.append(binlog_name_tmp);
        LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str()); 
      }

      idx = left_string.find("|");
      std::string binlog_pos = left_string.substr(0, idx);
      std::string se_snapshot = left_string.substr(idx + 1);
    }
    consistent_snapshot_archive->unlock_consistent_snapshot_index();
    if (index_file->error == -1) {
      error = 1;
      err_msg.assign("Failed to read consistent_snapshot.index");
      return std::make_tuple(error, err_msg);
    }
  }

  mysql_mutex_lock(&m_index_lock);
  if (atomic_log_index_state != LOG_INDEX_OPENED) {
    error = 1;
    err_msg.assign("Binlog archive index file is not opened.");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    goto err;
  }

  if ((error = find_log_pos(&log_info, purge_log_name))) {
    err_msg.assign("Failed to find log pos: ");
    err_msg.append(purge_log_name);
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    goto err;
  }

  if ((error = open_purge_index_file(true))) {
    err_msg.assign("Failed to open purge index");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    goto err;
  }

  /*
    File name exists in index file; delete until we find this file
    or a file that is used.
  */
  if ((error = find_log_pos(&log_info, NullS))) {
    err_msg.assign("Failed to find log pos");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    goto err;
  }

  while (compare_log_name(purge_log_name, log_info.log_file_name)) {
    if ((error = register_purge_index_entry(log_info.log_file_name))) {
      err_msg.assign("Failed to register purge");
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      goto err;
    }

    if (find_next_log(&log_info)) break;
  }

  if ((error = sync_purge_index_file())) {
    err_msg.assign("Failed to sync purge index");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    goto err;
  }

  /* We know how many files to delete. Update index file. */
  if ((error = remove_logs_from_index(&log_info))) {
    err_msg.assign("Failed to remove logs from index");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    goto err;
  }

err:
  int error_index = 0, close_error_index = 0;
  /* Read each entry from m_purge_index_file and delete the file. */
  if (!error && is_inited_purge_index_file() &&
      (error_index = purge_index_entry(decrease_log_space))) {
    err_msg.assign("Failed to purge index entry");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
  }

  close_error_index = close_purge_index_file();
  mysql_mutex_unlock(&m_index_lock);
  /*
    Error codes from purge logs take precedence.
    Then error codes from purging the index entry.
    Finally, error codes from closing the purge index file.
  */
  error = error ? error : (error_index ? error_index : close_error_index);
  return std::make_tuple(error, err_msg);
}

int Binlog_archive::set_purge_index_file_name() {
  int error = 0;
  DBUG_TRACE;
  if (fn_format(m_purge_index_file_name, BINLOG_ARCHIVE_INDEX_FILE,
                m_binlog_archive_dir, ".~rec~",
                MYF(MY_UNPACK_FILENAME | MY_SAFE_PATH | MY_REPLACE_EXT)) ==
      nullptr) {
    error = 1;
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "failed to set purge index");
  }
  return error;
}

int Binlog_archive::open_purge_index_file(bool destroy) {
  DBUG_TRACE;

  int error = 0;
  File file = -1;

  if (destroy) close_purge_index_file();

  if (!my_b_inited(&m_purge_index_file)) {
    myf flags = MY_WME | MY_NABP | MY_WAIT_IF_FULL;
    if ((file = my_open(m_purge_index_file_name, O_RDWR | O_CREAT,
                        MYF(MY_WME))) < 0 ||
        init_io_cache(&m_purge_index_file, file, IO_SIZE,
                      (destroy ? WRITE_CACHE : READ_CACHE), 0, false, flags)) {
      error = 1;
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "failed to open purge index");
    }
  }
  return error;
}

int Binlog_archive::close_purge_index_file() {
  DBUG_TRACE;
  int error = 0;

  if (my_b_inited(&m_purge_index_file)) {
    end_io_cache(&m_purge_index_file);
    error = my_close(m_purge_index_file.file, MYF(0));
  }
  my_delete(m_purge_index_file_name, MYF(0));
  new (&m_purge_index_file) IO_CACHE();

  return error;
}

bool Binlog_archive::is_inited_purge_index_file() {
  DBUG_TRACE;
  return my_b_inited(&m_purge_index_file);
}

int Binlog_archive::sync_purge_index_file() {
  DBUG_TRACE;
  int error = 0;

  if (!my_b_inited(&m_purge_index_file)) {
    return LOG_INFO_IO;
  }
  if ((error = flush_io_cache(&m_purge_index_file)) ||
      (error = my_sync(m_purge_index_file.file, MYF(MY_WME))))
    return error;

  return error;
}

int Binlog_archive::register_purge_index_entry(const char *entry) {
  int error = 0;
  DBUG_TRACE;

  if (!my_b_inited(&m_purge_index_file)) {
    return LOG_INFO_IO;
  }
  if ((error = my_b_write(&m_purge_index_file, (const uchar *)entry,
                          strlen(entry))) ||
      (error = my_b_write(&m_purge_index_file, (const uchar *)"\n", 1)))
    return error;

  return error;
}

/**
 * @brief Delete the binlog file from the object store.
 *
 * @param decrease_log_space
 * @return int
 */
int Binlog_archive::purge_index_entry(ulonglong *decrease_log_space) {
  DBUG_TRACE;
  MY_STAT s;
  int error = 0;
  LOG_INFO log_info;
  LOG_INFO check_log_info;

  assert(my_b_inited(&m_purge_index_file));

  if ((error =
           reinit_io_cache(&m_purge_index_file, READ_CACHE, 0, false, false))) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, "failed to reinit_io_cache");
    goto err;
  }

  for (;;) {
    size_t length = 0;

    if ((length = my_b_gets(&m_purge_index_file, log_info.log_file_name,
                            FN_REFLEN)) <= 1) {
      if (m_purge_index_file.error) {
        error = m_purge_index_file.error;
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, error);
        goto err;
      }

      /* Reached EOF */
      break;
    }

    /* Get rid of the trailing '\n' */
    log_info.log_file_name[length - 1] = 0;

    // delete the archived binlog file from the object store.
    if (binlog_objstore) {
      char temp_binlog_relative_file_name[FN_REFLEN + 1] = {0};
      if (log_info.log_file_name[0] == FN_LIBCHAR) {
        strmake(temp_binlog_relative_file_name, log_info.log_file_name + 1,
                sizeof(temp_binlog_relative_file_name) - 1);
      } else if (log_info.log_file_name[0] == FN_CURLIB &&
                 log_info.log_file_name[1] == FN_LIBCHAR) {
        strmake(temp_binlog_relative_file_name, log_info.log_file_name + 2,
                sizeof(temp_binlog_relative_file_name) - 1);
      } else {
        strmake(temp_binlog_relative_file_name, log_info.log_file_name,
                sizeof(temp_binlog_relative_file_name) - 1);
      }

      // List all binlog slice.
      std::string binlog_keyid{};
      std::vector<objstore::ObjectMeta> objects;
      binlog_keyid.assign(BINLOG_ARCHIVE_SUBDIR);
      binlog_keyid.append(FN_DIRSEP);
      binlog_keyid.append(temp_binlog_relative_file_name);
      binlog_keyid.append(".");
      objstore::Status ss = binlog_objstore->list_object(
          std::string_view(opt_objstore_bucket), binlog_keyid, objects);
      if (!ss.is_succ() &&
          ss.error_code() != objstore::Errors::SE_NO_SUCH_KEY) {
        std::string err_msg;
        err_msg.assign("Failed to list binlog files: ");
        err_msg.append(BINLOG_ARCHIVE_SUBDIR);
        err_msg.append(" error=");
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
        error = 1;
        goto err;
      }

      // Delete all binlog slice.
      for (const auto &object : objects) {
        ss = binlog_objstore->delete_object(
            std::string_view(opt_objstore_bucket), object.key);
        if (!ss.is_succ()) {
          std::string err_msg;
          err_msg.assign("Failed to delet binlog file from object store: ");
          err_msg.append(object.key);
          err_msg.append(" error=");
          err_msg.append(ss.error_message());
          LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
          error = 1;
          goto err;
        }
        // TODO: should check error code, if failed because of NOT_FOUND,
        // should report WARNING and continue to delete next binlog file.
      }
    } else {
      // delete the archived binlog file from the local archive directory.
      if (!mysql_file_stat(PSI_binlog_purge_archive_log_file_key,
                           log_info.log_file_name, &s, MYF(0))) {
        if (my_errno() == ENOENT) {
          /*
            It's not fatal if we can't stat a log file that does not exist;
            If we could not stat, we won't delete.
          */
          LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG,
                 log_info.log_file_name);
          set_my_errno(0);
        } else {
          /*
            Other than ENOENT are fatal
          */
          LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG,
                 log_info.log_file_name);
          error = LOG_INFO_FATAL;
          goto err;
        }
      } else {
        DBUG_PRINT("info", ("purging %s", log_info.log_file_name));
        if (!mysql_file_delete(PSI_binlog_purge_archive_log_file_key,
                               log_info.log_file_name, MYF(0))) {
          if (decrease_log_space) *decrease_log_space -= s.st_size;
        } else {
          if (my_errno() == ENOENT) {
            LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG,
                   log_info.log_file_name);
            set_my_errno(0);
          } else {
            LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG,
                   log_info.log_file_name);

            if (my_errno() == EMFILE) {
              DBUG_PRINT("info", ("my_errno: %d, set ret = LOG_INFO_EMFILE",
                                  my_errno()));
              error = LOG_INFO_EMFILE;
              goto err;
            }
            error = LOG_INFO_FATAL;
            goto err;
          }
        }
      }
    }
  }

err:
  return error;
}

/**
 * @brief Remove logs entry from index file.
 *
 * @param log_info
 * @return int
 */
int Binlog_archive::remove_logs_from_index(LOG_INFO *log_info) {
  DBUG_TRACE;

  if (!my_b_inited(&m_index_file)) {
    goto err;
  }

  if (open_crash_safe_index_file()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "failed to open_crash_safe_index_file in remove_logs_from_index.");
    goto err;
  }

  if (copy_file(&m_index_file, &m_crash_safe_index_file,
                log_info->index_file_start_offset)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "failed to copy_file in remove_logs_from_index.");
    goto err;
  }

  if (close_crash_safe_index_file()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "failed to close_crash_safe_index_file.");
    goto err;
  }

  // First, update the local index file, then upload it to the object store.
  // If the local update fails, this purge of binlogs will also fail. If the
  // local update succeeds but the upload fails, it will result in
  // inconsistency between the local and object store indexes. Therefore,
  // forcibly close the index file to allow the archive thread to download the
  // index from the object store again and reopen it.
  if (move_crash_safe_index_file_to_index_file()) {
    // If move fails, attempt to recover the index file. If recovery fails, it
    // will result in a fatal error.
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "failed to move_crash_safe_index_file_to_index_file.");
    goto err;
  }

  if (binlog_objstore != nullptr) {
    // refresh 'mysql-bin.index' to object store
    std::string index_keyid{};
    index_keyid.assign(BINLOG_ARCHIVE_SUBDIR);
    index_keyid.append(FN_DIRSEP);
    index_keyid.append(BINLOG_ARCHIVE_INDEX_FILE);
    objstore::Status ss = binlog_objstore->put_object_from_file(
        std::string_view(opt_objstore_bucket), index_keyid, m_index_file_name);
    if (!ss.is_succ()) {
      std::string err_msg;
      err_msg.assign("Failed to upload clone index file to object store: ");
      err_msg.append(index_keyid);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      close_index_file();
      goto err;
    }
  }

  return 0;

err:
  return LOG_INFO_IO;
}

int Binlog_archive::show_binlog_persistent_files(
    std::vector<objstore::ObjectMeta> &objects) {
  DBUG_TRACE;
  int error = 0;
  if (binlog_objstore != nullptr) {
    objstore::Status ss = binlog_objstore->list_object(
        std::string_view(opt_objstore_bucket),
        std::string_view(BINLOG_ARCHIVE_SUBDIR), objects);
    if (!ss.is_succ() && ss.error_code() != objstore::Errors::SE_NO_SUCH_KEY) {
      std::string err_msg;
      error = 1;
      err_msg.assign("Failed to binlog files: ");
      err_msg.append(BINLOG_ARCHIVE_SUBDIR);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      return error;
    }

    objects.erase(
        std::remove_if(objects.begin(), objects.end(),
                       [](const objstore::ObjectMeta &obj) {
                         std::string index_keyid;
                         index_keyid.assign(BINLOG_ARCHIVE_SUBDIR);
                         index_keyid.append(FN_DIRSEP);
                         index_keyid.append(BINLOG_ARCHIVE_INDEX_FILE);
                         return obj.key.compare(index_keyid) == 0;
                       }),
        objects.end());
    // sort the binlog slice files by key.
    std::sort(objects.begin(), objects.end(),
              [](const objstore::ObjectMeta &a, const objstore::ObjectMeta &b) {
                return a.key < b.key;
              });
  }
  return error;
}

/**
  When a fatal error occurs due to which binary logging becomes impossible and
  the user specified binlog_error_action= ABORT_SERVER the following function
  is invoked. This function print error message to server error log and then
  aborts the server.

  @param err_string          Error string which specifies the exact error
                             message from the caller.
  @note binlog_archive is a background thread, so it does not use my_error()
  function to push error messages to client. Only use LogErr() function to
  print error messages to server error log.
*/
static void exec_binlog_error_action_abort(const char *err_string) {
  LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_string);
  flush_error_log_messages();
  my_abort();
}

inline int Binlog_archive::reset_transmit_packet(size_t event_len) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("event_len: %zu, m_packet->alloced_length: %zu",
                      event_len, m_packet.alloced_length()));
  assert(m_packet.alloced_length() >= PACKET_MIN_SIZE);

  m_packet.length(0);          // size of the content
  qs_append('\0', &m_packet);  // Set this as an OK packet

  /* Resizes the buffer if needed. */
  if (event_len > 0 && grow_packet(event_len)) return 1;

  DBUG_PRINT("info", ("m_packet.alloced_length: %zu (after potential "
                      "reallocation)",
                      m_packet.alloced_length()));

  return 0;
}

inline bool Binlog_archive::grow_packet(size_t extra_size) {
  DBUG_TRACE;
  size_t cur_buffer_size = m_packet.alloced_length();
  size_t cur_buffer_used = m_packet.length();
  size_t needed_buffer_size = cur_buffer_used + extra_size;

  if (extra_size > (PACKET_MAX_SIZE - cur_buffer_used))
    /*
       Not enough memory: requesting packet to be bigger than the max
       allowed - PACKET_MAX_SIZE.
    */
    return true;

  /* Grow the buffer if needed. */
  if (needed_buffer_size > cur_buffer_size) {
    size_t new_buffer_size;
    new_buffer_size =
        calc_grow_buffer_size(cur_buffer_size, needed_buffer_size);

    if (!new_buffer_size) return true;

    if (m_packet.mem_realloc(new_buffer_size)) return true;

    /*
     Calculates the new, smaller buffer, size to use the next time
     one wants to shrink the buffer.
    */
    calc_shrink_buffer_size(new_buffer_size);
  }

  return false;
}

inline bool Binlog_archive::shrink_packet() {
  DBUG_TRACE;
  bool res = false;
  size_t cur_buffer_size = m_packet.alloced_length();
  size_t buffer_used = m_packet.length();

  assert(!(cur_buffer_size < PACKET_MIN_SIZE));

  /*
     If the packet is already at the minimum size, just
     do nothing. Otherwise, check if we should shrink.
   */
  if (cur_buffer_size > PACKET_MIN_SIZE) {
    /* increment the counter if we used less than the new shrink size. */
    if (buffer_used < m_new_shrink_size) {
      m_half_buffer_size_req_counter++;

      /* Check if we should shrink the buffer. */
      if (m_half_buffer_size_req_counter == PACKET_SHRINK_COUNTER_THRESHOLD) {
        /*
         The last PACKET_SHRINK_COUNTER_THRESHOLD consecutive packets
         required less than half of the current buffer size. Lets shrink
         it to not hold more memory than we potentially need.
        */
        m_packet.shrink(m_new_shrink_size);

        /*
           Calculates the new, smaller buffer, size to use the next time
           one wants to shrink the buffer.
         */
        calc_shrink_buffer_size(m_new_shrink_size);

        /* Reset the counter. */
        m_half_buffer_size_req_counter = 0;
      }
    } else
      m_half_buffer_size_req_counter = 0;
  }
#ifndef NDEBUG
  if (res == false) {
    assert(m_new_shrink_size <= cur_buffer_size);
    assert(m_packet.alloced_length() >= PACKET_MIN_SIZE);
  }
#endif
  return res;
}

inline size_t Binlog_archive::calc_grow_buffer_size(size_t current_size,
                                                    size_t min_size) {
  /* Check that a sane minimum buffer size was requested.  */
  assert(min_size > PACKET_MIN_SIZE);
  if (min_size > PACKET_MAX_SIZE) return 0;

  /*
     Even if this overflows (PACKET_MAX_SIZE == UINT_MAX32) and
     new_size wraps around, the min_size will always be returned,
     i.e., it is a safety net.

     Also, cap new_size to PACKET_MAX_SIZE (in case
     PACKET_MAX_SIZE < UINT_MAX32).
   */
  size_t new_size = static_cast<size_t>(
      std::min(static_cast<double>(PACKET_MAX_SIZE),
               static_cast<double>(current_size * PACKET_GROW_FACTOR)));

  new_size = ALIGN_SIZE(std::max(new_size, min_size));

  return new_size;
}

void Binlog_archive::calc_shrink_buffer_size(size_t current_size) {
  size_t new_size = static_cast<size_t>(
      std::max(static_cast<double>(PACKET_MIN_SIZE),
               static_cast<double>(current_size * PACKET_SHRINK_FACTOR)));

  m_new_shrink_size = ALIGN_SIZE(new_size);
}

static time_t calculate_auto_purge_lower_time_bound() {
  if (DBUG_EVALUATE_IF("expire_logs_always", true, false)) return time(nullptr);

  int64 expiration_time = 0;
  int64 current_time = time(nullptr);

  if (opt_binlog_archive_expire_seconds > 0)
    expiration_time = current_time - opt_binlog_archive_expire_seconds;

  // check for possible overflow conditions (4 bytes time_t)
  if (expiration_time < std::numeric_limits<time_t>::min())
    expiration_time = std::numeric_limits<time_t>::min();

  return static_cast<time_t>(expiration_time);
}
