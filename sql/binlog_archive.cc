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

#include "my_dbug.h"
#include "my_thread.h"
#include "mysql/components/services/log_builtins.h"  // LogErr
#include "mysql/psi/mysql_file.h"
#include "mysql/psi/mysql_thread.h"
#include "objstore.h"
#include "sql/binlog.h"
#include "sql/binlog_reader.h"
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

/** The instrumentation key to use for opening the log file. */
static PSI_file_key PSI_binlog_archive_log_file_key;
/** The instrumentation key to use for opening the log index file. */
static PSI_file_key PSI_binlog_archive_log_index_key;
/** The instrumentation key to use for opening a log index cache file. */
static PSI_file_key PSI_binlog_archive_log_index_cache_key;

static PSI_thread_info all_binlog_archive_threads[] = {
    {&THR_KEY_binlog_archive, "archive", "bin_arch", PSI_FLAG_AUTO_SEQNUM, 0,
     PSI_DOCUMENT_ME}};

static PSI_mutex_info all_binlog_archive_mutex_info[] = {
    {&PSI_binlog_archive_lock_key, "mutex", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_binlog_index_lock_key, "mutex", 0, 0, PSI_DOCUMENT_ME}};

static PSI_cond_info all_binlog_archive_cond_info[] = {
    {&PSI_binlog_archive_cond_key, "condition", 0, 0, PSI_DOCUMENT_ME}};

static PSI_file_info all_binlog_archive_files[] = {
    {&PSI_binlog_archive_log_file_key, "log_file", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_log_index_key, "index", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_binlog_archive_log_index_cache_key, "index_cache", 0, 0,
     PSI_DOCUMENT_ME}};
#endif

static void *mysql_binlog_archive_action(void *arg);
static int compare_log_name(const char *log_1, const char *log_2);
static bool remove_file(const std::string &file);
static void root_directory(std::string in, std::string *out, std::string *left);
static bool recursive_create_dir(const std::string &dir,
                                 const std::string &root);
static void exec_binlog_error_action_abort(THD *thd, const char *err_string);
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
  if (!opt_binlog_archive) {
    LogErr(INFORMATION_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "The binlog archive not enabled --binlog-archive.");
    return 0;
  }
  // Check if the mysql archive path is set.
  if (opt_mysql_archive_dir == nullptr) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "Mysql archive path no set, please set --mysql-archive-dir.");
    return 1;
  }

  MY_STAT d_stat;
  // Check if opt_mysql_archive_dir exists.
  if (!my_stat(opt_mysql_archive_dir, &d_stat, MYF(0)) ||
      !MY_S_ISDIR(d_stat.st_mode) ||
      my_access(opt_mysql_archive_dir, (F_OK | W_OK))) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "Mysql archive path not exists.");
    return 1;
  }

  // data home can not include archive directory.
  if (test_if_data_home_dir(opt_mysql_archive_dir)) {
    std::string err_msg;
    err_msg.assign("mysql archive path is within the current data directory: ");
    err_msg.append(opt_mysql_archive_dir);
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    return 1;
  }

  // Check if archive binlog dir exists. If not exists, create it.
  char tmp_archive_binlog_dir[FN_REFLEN + 1] = {0};
  strmake(tmp_archive_binlog_dir, opt_mysql_archive_dir,
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

/**
 * @brief Constructs a Binlog_archive object.
 */
Binlog_archive::Binlog_archive()
    : m_thd(nullptr),
      m_thd_state(),
      m_binlog_archive_file(nullptr),
      description_event(BINLOG_VERSION, ::server_version),
      binlog_objstore(nullptr),
      m_index_file(),
      m_crash_safe_index_file(),
      m_purge_index_file() {
  m_next_binlog_archive_file_name[0] = '\0';
  m_mysql_archive_dir[0] = '\0';
  m_binlog_archive_dir[0] = '\0';
  m_binlog_archive_file_name[0] = '\0';
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
}

/**
 * @brief Cleans up any resources used by the binlog archive.
 */
void Binlog_archive::cleanup() {
  DBUG_TRACE;
  mysql_mutex_destroy(&m_index_lock);
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
  thd->set_binlog_archive(this);
  m_thd = thd;
}

/**
 * @brief Runs the binlog archiving process.
 */
void Binlog_archive::run() {
  DBUG_TRACE;
  std::string err_msg;
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

  strmake(m_mysql_archive_dir, opt_mysql_archive_dir,
          sizeof(m_mysql_archive_dir) - 1);
  convert_dirname(m_mysql_archive_dir, m_mysql_archive_dir, NullS);
  strmake(strmake(m_binlog_archive_dir, m_mysql_archive_dir,
                  sizeof(m_binlog_archive_dir) - BINLOG_ARCHIVE_SUBDIR_LEN - 1),
          STRING_WITH_LEN(BINLOG_ARCHIVE_SUBDIR));
  // if m_binlog_archive_dir dir not exists, start_binlog_archive() will create
  // it.
  convert_dirname(m_binlog_archive_dir, m_binlog_archive_dir, NullS);

  mysql_mutex_lock(&m_run_lock);
  m_thd_state.set_running();
  mysql_cond_broadcast(&m_run_cond);
  mysql_mutex_unlock(&m_run_lock);

  LogErr(SYSTEM_LEVEL, ER_BINLOG_ARCHIVE_LOG, "Binlog archive thread running");

  for (;;) {
    mysql_mutex_lock(&m_run_lock);
    if (abort_binlog_archive) {
      mysql_mutex_unlock(&m_run_lock);
      break;
    }
    mysql_mutex_unlock(&m_run_lock);
    if (m_thd == nullptr || m_thd->killed) break;
    m_thd->clear_error();
    m_thd->get_stmt_da()->reset_diagnostics_area();
    
    LOG_INFO log_info;
    int error = 1;
    // generate new archive binlog name.
    m_next_binlog_archive_file_name[0] = 0;
    mysql_mutex_lock(&m_index_lock);
    if (open_index_file()) {
      err_msg.assign("Failed to open archive binlog index file: ");
      err_msg.append(BINLOG_ARCHIVE_INDEX_FILE);
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
      mysql_mutex_unlock(&m_index_lock);
      break;
    }
    error = find_log_pos(&log_info, NullS);
    if (error == 0) {
      do {
        strmake(m_next_binlog_archive_file_name, log_info.log_file_name,
                sizeof(m_next_binlog_archive_file_name) - 1);
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
      m_next_binlog_archive_file_name[0] = 0;
    } else {
      close_index_file();
      mysql_mutex_unlock(&m_index_lock);
      LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
             "Failed IO to find log from archive binlog index file.");
      break;
    }
    // Verify if the m_next_binlog_archive_file_name is valid. Find the position
    // from mysql_bin_log index file.
    if (m_next_binlog_archive_file_name[0] != 0) {
      mysql_bin_log.lock_index();
      error = mysql_bin_log.find_log_pos(
          &log_info, m_next_binlog_archive_file_name, false);
      // Find the next binlog file, start archive from the next binlog file.
      if (error == 0) {
        if (!(error = mysql_bin_log.find_next_log(&log_info, false))) {
          strmake(m_next_binlog_archive_file_name, log_info.log_file_name,
                  sizeof(m_next_binlog_archive_file_name) - 1);
        }
        mysql_bin_log.unlock_index();
        // If not found the next mysql binlog file, report error.
        if (error != 0) {
          LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
                 "Failed to find next mysql binlog.");
          close_index_file();
          mysql_mutex_unlock(&m_index_lock);
          break;
        }
      } else if (error == LOG_INFO_EOF) {
        // If not found, identify binlog path change. Start archive from the
        // first binlog file.
        m_next_binlog_archive_file_name[0] = 0;
        mysql_bin_log.unlock_index();
      } else {
        mysql_bin_log.unlock_index();
        err_msg.assign("Failed to find binlog from mysql binlog: ");
        err_msg.append(m_next_binlog_archive_file_name);
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
        close_index_file();
        mysql_mutex_unlock(&m_index_lock);
        break;
      }
    }
    mysql_mutex_unlock(&m_index_lock);
    /*
     * Send the mysql binlog to archive path. Change send_packet() to
     * archive_events(), when mysql_binlog_send(). Contiue to wait for the next
     * binlog event to archive, until archive thd is killed or
     * abort_binlog_archive is true. If mysql_binlog_send abort, archive thread
     * will execute mysql_binlog_send repeatly.
     */
    archive_binlog_with_gtid(m_thd);
    // If previous archive binlog file is not closed, close it.
    if (m_binlog_archive_file) {
      my_fclose(m_binlog_archive_file, MYF(0));
      m_binlog_archive_file = nullptr;
    }
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

/**
 * @brief Send the mysql binlog to archive path. Change send_packet() to
 * archive_events(), when mysql_binlog_send(). Contiue to wait for the next
 * binlog event to archive, until archive thd is killed or abort_binlog_archive
 * is true.
 * If mysql_binlog_send abort, archive thread will execute mysql_binlog_send
 * repeatly.
 * @param thd
 * @return int
 * TODO: This function should be implemented by myself, not use
 * mysql_binlog_send. In this way, the binlog archiving process will no longer
 * rely on MySQL binlog rotation actions but can be defined by us to determine
 * the size of the archived binlog. However, it is necessary to retrieve the
 * GTID as the header each time.
 */
int Binlog_archive::archive_binlog_with_gtid(THD *thd) {
  ushort flags = 0;
  uint64 pos = 4;

  DBUG_PRINT("info", ("pos=%" PRIu64 " flags=%d server_id=%d", pos, flags,
                      thd->server_id));
  mysql_binlog_send(thd, m_next_binlog_archive_file_name, (my_off_t)pos,
                    nullptr, flags);
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
int Binlog_archive::archive_event(THD *thd, ushort flags, String *packet,
                                  const char *log_file, my_off_t log_pos) {
  DBUG_TRACE;
  int error = 0;
  assert(thd != nullptr);
  assert(flags == 0);
  assert(log_file != nullptr || log_file == nullptr);
  assert(log_pos > 0);

  /*
    In raw mode We only need the full event details if it is a
    ROTATE_EVENT or FORMAT_DESCRIPTION_EVENT
  */
  Log_event *ev = nullptr;
  const unsigned char *packet_ptr = NULL;
  unsigned char *event_buf = nullptr;
  ulong event_len = 0;

  packet_ptr = pointer_cast<const uchar *>(packet->ptr());
  Log_event_type type = (Log_event_type)packet_ptr[1 + EVENT_TYPE_OFFSET];

  DBUG_PRINT("info",
             ("Archiving event of type %s", Log_event::get_type_str(type)));

  event_len = packet->length() - 1;
  if (!(event_buf = (unsigned char *)my_malloc(key_memory_log_event,
                                               event_len + 1, MYF(0)))) {
    return 1;
  }
  memcpy(event_buf, packet_ptr + 1, event_len);

  Binlog_read_error read_error = binlog_event_deserialize(
      reinterpret_cast<unsigned char *>(event_buf), event_len,
      &description_event, opt_source_verify_checksum, &ev);

  if (read_error.has_error()) {
    my_free(event_buf);
    return 1;
  }
  ev->register_temp_buf((char *)event_buf);

  if (type == binary_log::ROTATE_EVENT) {
    Rotate_log_event *rev = (Rotate_log_event *)ev;
    // record the new log file name.
    // when the log file rotate, need to open the new log file.
    // rev->new_log_ident is only file name, not include the path.
    // my_stpcpy(m_next_binlog_archive_file_name, rev->new_log_ident);
    assert(log_file != nullptr);
    strmake(m_next_binlog_archive_file_name, log_file,
            sizeof(m_next_binlog_archive_file_name) - 1);

    // fake rotate event.
    // Binlog_sender::run will send fake rotate event, before send every binlog
    // file. So we need to ignore it.
    if (rev->common_header->when.tv_sec == 0) {
      error = 0;
      goto end;
    }
  } else if (type == binary_log::FORMAT_DESCRIPTION_EVENT) {
    /*
      This could be an fake Format_description_log_event that server
      (5.0+) automatically sends to a slave on connect, before sending
      a first event at the requested position.  If this is the case,
      don't increment old_off. Real Format_description_log_event always
      starts from BIN_LOG_HEADER_SIZE position.
    */
    if (m_binlog_archive_file) {
      // close the old archive binlog file and archive it to the object store.
      my_fclose(m_binlog_archive_file, MYF(0));
      m_binlog_archive_file = nullptr;

      char temp_binlog_relative_file_name[FN_REFLEN + 1] = {0};
      // m_binlog_archive_file_name could be a absolute path.
      // get relative path.
      if (m_binlog_archive_file_name[0] == FN_LIBCHAR) {
        strmake(temp_binlog_relative_file_name, m_binlog_archive_file_name + 1,
                sizeof(temp_binlog_relative_file_name) - 1);
      } else {
        strmake(temp_binlog_relative_file_name, m_binlog_archive_file_name,
                sizeof(temp_binlog_relative_file_name) - 1);
      }

      // Upload the local archived binlog file to the object store.
      std::string archived_binlog_name;
      archived_binlog_name.assign(m_binlog_archive_dir);
      archived_binlog_name.append(temp_binlog_relative_file_name);
      if (binlog_objstore) {
        std::string archived_binlog_keyid;
        archived_binlog_keyid.assign(BINLOG_ARCHIVE_SUBDIR);
        archived_binlog_keyid.append(FN_DIRSEP);
        archived_binlog_keyid.append(temp_binlog_relative_file_name);
        objstore::Status ss = binlog_objstore->put_object_from_file(
            std::string_view(opt_objstore_bucket), archived_binlog_keyid,
            archived_binlog_name);
        if (!ss.is_succ()) {
          std::string err_msg;
          err_msg.assign("Failed to upload binlog file to object store: ");
          err_msg.append(archived_binlog_keyid);
          err_msg.append(" error=");
          err_msg.append(ss.error_message());
          LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
          error = 1;
          goto end;
        }
        // remove the local archived binlog file, after upload to the object
        // store.
        remove_file(archived_binlog_name);
      }

      // When we are done with the file, we need to record it in the index.
      // record binlog real file name to the index file.
      mysql_mutex_lock(&m_index_lock);
      if (add_log_to_index((uchar *)m_binlog_archive_file_name,
                           strlen(m_binlog_archive_file_name))) {
        mysql_mutex_unlock(&m_index_lock);
        error = 1;
        goto end;
      }
      mysql_mutex_unlock(&m_index_lock);
    }
    // Maybe the binlog file is first mysql binlog.
    if (m_next_binlog_archive_file_name[0] == '\0') {
      error = 0;
      goto end;
    }

    char temp_binlog_relative_file_name[FN_REFLEN + 1] = {0};
    // m_next_binlog_archive_file_name could be a absolute path.
    // get relative path. Check if the binlog file name's first char
    // is "/"
    if (m_next_binlog_archive_file_name[0] == FN_LIBCHAR) {
      strmake(temp_binlog_relative_file_name,
              m_next_binlog_archive_file_name + 1,
              sizeof(temp_binlog_relative_file_name) - 1);
    } else {
      strmake(temp_binlog_relative_file_name, m_next_binlog_archive_file_name,
              sizeof(temp_binlog_relative_file_name) - 1);
    }
    char temp_binlog_dir[FN_REFLEN + 1] = {0};
    size_t temp_binlog_dir_len = 0;
    dirname_part(temp_binlog_dir, temp_binlog_relative_file_name,
                 &temp_binlog_dir_len);
    if (temp_binlog_dir_len > 0) {
      MY_STAT d_stat;
      char temp_archive_binlog_dir[FN_REFLEN + 1] = {0};
      strmake(
          strmake(temp_archive_binlog_dir, m_binlog_archive_dir,
                  sizeof(temp_archive_binlog_dir) - temp_binlog_dir_len - 1),
          temp_binlog_dir, temp_binlog_dir_len);
      // create archive binlog sub dir, if not exists.
      if (!my_stat(temp_archive_binlog_dir, &d_stat, MYF(0))) {
        // recursive create subdir.
        if (recursive_create_dir(temp_binlog_relative_file_name,
                                 m_binlog_archive_dir)) {
          error = 1;
          LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
                 "Failed to create binlog archive sub dir.");
          goto end;
        }
      }
    }

    std::string temp_archive_binlog_file_name;
    assert(m_binlog_archive_dir[0] != '\0');
    temp_archive_binlog_file_name.assign(m_binlog_archive_dir);
    temp_archive_binlog_file_name.append(temp_binlog_relative_file_name);

    if (!(m_binlog_archive_file =
              my_fopen(temp_archive_binlog_file_name.c_str(),
                       O_WRONLY | MY_FOPEN_BINARY, MYF(MY_WME)))) {
      error = 1;
      goto end;
    }
    strmake(m_binlog_archive_file_name, m_next_binlog_archive_file_name,
            sizeof(m_binlog_archive_file_name) - 1);

    if (my_fwrite(m_binlog_archive_file, (const uchar *)BINLOG_MAGIC,
                  BIN_LOG_HEADER_SIZE, MYF(MY_NABP))) {
      error = 1;
      goto end;
    }

    description_event = dynamic_cast<Format_description_event &>(*ev);
  }

  assert(m_binlog_archive_file != nullptr);
  if (my_fwrite(m_binlog_archive_file, (const uchar *)event_buf, event_len,
                MYF(MY_NABP))) {
    error = 1;
    goto end;
  }

  // Signal update after every event.
  // Actually, it's enough to send a signal only when rotating the binlog file.
  signal_update();

  /* Flush m_binlog_archive_file after every event */
  fflush(m_binlog_archive_file);

end:
  if (!ev) delete ev;
  return error;
}

/**
 * @brief Flush the archive binlog file.
 *
 * @return int
 */
int Binlog_archive::flush_events() {
  DBUG_TRACE;
  if (m_binlog_archive_file) {
    fflush(m_binlog_archive_file);
  }
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
        m_thd,
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
    std::string index_keyid;
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
    std::string index_file_name_str;
    index_file_name_str.assign(m_crash_safe_index_file_name);
    std::string index_keyid;
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
int Binlog_archive::find_log_pos(LOG_INFO *linfo, const char *log_name) {
  DBUG_TRACE;
  int error = 0;
  char *full_fname = linfo->log_file_name;
  char full_log_name[FN_REFLEN] = {0}, fname[FN_REFLEN] = {0};

  if (!my_b_inited(&m_index_file)) {
    error = LOG_INFO_IO;
    goto end;
  }

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
  my_b_seek(&m_index_file, (my_off_t)0);

  for (;;) {
    size_t length = 0;
    my_off_t offset = my_b_tell(&m_index_file);

    /* If we get 0 or 1 characters, this is the end of the file */
    if ((length = my_b_gets(&m_index_file, fname, FN_REFLEN)) <= 1) {
      /* Did not find the given entry; Return not found or error */
      error = !m_index_file.error ? LOG_INFO_EOF : LOG_INFO_IO;
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
      linfo->index_file_offset = my_b_tell(&m_index_file);
      break;
    }
    linfo->entry_index++;
  }

end:
  return error;
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

  int error = 0;
  size_t length = 0;
  char fname[FN_REFLEN] = {0};
  char *full_fname = linfo->log_file_name;

  if (!my_b_inited(&m_index_file)) {
    error = LOG_INFO_IO;
    goto err;
  }
  /* As the file is flushed, we can't get an error here */
  my_b_seek(&m_index_file, linfo->index_file_offset);

  linfo->index_file_start_offset = linfo->index_file_offset;
  if ((length = my_b_gets(&m_index_file, fname, FN_REFLEN)) <= 1) {
    error = !m_index_file.error ? LOG_INFO_EOF : LOG_INFO_IO;
    goto err;
  }

  if (fname[0] != 0) {
    if (normalize_binlog_name(full_fname, fname, false)) {
      error = LOG_INFO_EOF;
      goto err;
    }
    length = strlen(full_fname);
  }

  linfo->index_file_offset = my_b_tell(&m_index_file);

err:
  return error;
}

/**
 * @brief Check if the given log file is archived.
 * @note Must be called with m_index_lock held.
 */
bool Binlog_archive::is_binlog_archived(const char *log_file_name_arg) {
  DBUG_TRACE;

  // If the log index file is not opened, return true.
  // Wait binlog archive thread to open the index file.
  if (atomic_log_index_state != LOG_INDEX_OPENED) {
    return true;
  }

  if (!open_index_file()) {
    LOG_INFO log_info;
    int error = 1;
    if ((error = find_log_pos(&log_info, log_file_name_arg))) {
      return true;
    }
    assert(compare_log_name(log_info.log_file_name, log_file_name_arg) == 0);
    return false;
  }
  return true;
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
  lock_binlog_index();
  error = is_binlog_archived(log_file_name);
  unlock_binlog_index();
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
      mysql_binlog_archive->stop_waiting_for_update(log_file_name, log_pos)) {
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

/**
 * @brief Purge the pesistent binlog archive file.
 * @note Must be called with m_index_lock held.
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
  DBUG_PRINT("info", ("to_log= %s", to_log));

  if (atomic_log_index_state != LOG_INDEX_OPENED) {
    error = 1;
    err_msg.assign("Binlog archive index file is not opened.");
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
    goto err;
  }

  mysql_mutex_assert_owner(&m_index_lock);
  if ((error = find_log_pos(&log_info, to_log))) {
    err_msg.assign("Failed to find log pos: ");
    err_msg.append(to_log);
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

  while (compare_log_name(to_log, log_info.log_file_name)) {
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
      // m_binlog_archive_file_name could be a absolute path.
      // get relative path.
      if (log_info.log_file_name[0] == FN_LIBCHAR) {
        strmake(temp_binlog_relative_file_name, log_info.log_file_name + 1,
                sizeof(temp_binlog_relative_file_name) - 1);
      } else {
        strmake(temp_binlog_relative_file_name, log_info.log_file_name,
                sizeof(temp_binlog_relative_file_name) - 1);
      }
      std::string archived_binlog_keyid;
      archived_binlog_keyid.assign(BINLOG_ARCHIVE_SUBDIR);
      archived_binlog_keyid.append(FN_DIRSEP);
      archived_binlog_keyid.append(temp_binlog_relative_file_name);
      objstore::Status ss = binlog_objstore->delete_object(
          std::string_view(opt_objstore_bucket), archived_binlog_keyid);
      if (!ss.is_succ()) {
        std::string err_msg;
        err_msg.assign("Failed to delet binlog file from object store: ");
        err_msg.append(archived_binlog_keyid);
        err_msg.append(" error=");
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_msg.c_str());
        error = 1;
        goto err;
      }
      // TODO: should check error code, if failed because of NOT_FOUND, should
      // report WARNING and continue to delete next binlog file.
    } else {
      // delete the archived binlog file from the local archive directory.
      if (!mysql_file_stat(PSI_binlog_archive_log_file_key,
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
        if (!mysql_file_delete(PSI_binlog_archive_log_file_key,
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
    std::string index_keyid;
    index_keyid.assign(BINLOG_ARCHIVE_SUBDIR);
    index_keyid.append(FN_DIRSEP);
    index_keyid.append(BINLOG_ARCHIVE_INDEX_FILE);
    objstore::Status ss = binlog_objstore->put_object_from_file(
        std::string_view(opt_objstore_bucket), index_keyid,
        m_index_file_name);
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
  }
  return error;
}

/**
  When a fatal error occurs due to which binary logging becomes impossible and
  the user specified binlog_error_action= ABORT_SERVER the following function
  is invoked. This function pushes the appropriate error message to client and
  logs the same to server error log and then aborts the server.

  @param err_string          Error string which specifies the exact error
                             message from the caller.
*/
static void exec_binlog_error_action_abort(THD *thd, const char *err_string) {
  /*
    When the code enters here it means that there was an error at higher layer
    and my_error function could have been invoked to let the client know what
    went wrong during the execution.

    But these errors will not let the client know that the server is going to
    abort. Even if we add an additional my_error function call at this point
    client will be able to see only the first error message that was set
    during the very first invocation of my_error function call.

    The advantage of having multiple my_error function calls are visible when
    the server is up and running and user issues SHOW WARNINGS or SHOW ERROR
    calls. In this special scenario server will be immediately aborted and
    user will not be able execute the above SHOW commands.

    Hence we clear the previous errors and push one critical error message to
    clients.
   */
  if (thd) {
    if (thd->is_error()) thd->clear_error();
    /*
      Send error to both client and to the server error log.
    */
    my_error(ER_BINLOG_ARCHIVE_LOG, MYF(ME_FATALERROR), err_string);
  }

  LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG, err_string);
  flush_error_log_messages();

  my_abort();
}
