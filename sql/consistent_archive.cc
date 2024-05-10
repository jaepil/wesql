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

#include "sql/consistent_archive.h"

#include <fstream>
#include <sstream>
#include <string>
#include "my_io.h"

#include "libbinlogevents/include/binlog_event.h"
#include "my_dbug.h"
#include "my_dir.h"
#include "my_sys.h"
#include "my_thread.h"
#include "mysql/components/services/log_builtins.h"  // LogErr
#include "mysql/psi/mysql_file.h"
#include "mysql/psi/mysql_thread.h"
#include "mysqld_error.h"
#include "plugin/clone/include/clone.h"
#include "sql/binlog.h"
#include "sql/binlog_archive.h"
#include "sql/binlog_reader.h"
#include "sql/debug_sync.h"
#include "sql/derror.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/log.h"
#include "sql/mysqld.h"
#include "sql/mysqld_thd_manager.h"  // Global_THD_manager
#include "sql/rpl_io_monitor.h"
#include "sql/rpl_source.h"
#include "sql/sql_backup_lock.h"
#include "sql/sql_lex.h"
#include "sql/sql_parse.h"
#include "sql/sys_vars_shared.h"
#include "sql/transaction.h"

/** Mysql local clone directory ,refer to clone_status.cc */
#define MYSQL_LOCAL_CLONE_FILES_DIR "#clone" FN_DIRSEP
/** Mysql local clone view clone_status persister file */
#define MYSQL_LOCAL_CLONE_VIEW_STATUS_FILE \
  MYSQL_LOCAL_CLONE_FILES_DIR "#view_status"

// mysql consistent snapshot archive object
static Consistent_archive *mysql_consistent_archive = nullptr;
static my_thread_handle mysql_archiver_pthd;
static bool abort_archive = false;
static mysql_mutex_t m_run_lock;
static mysql_cond_t m_run_cond;

#ifdef HAVE_PSI_INTERFACE
static PSI_thread_key THR_KEY_consistent_archive;
static PSI_mutex_key PSI_consistent_archive_lock_key;
static PSI_cond_key PSI_consistent_archive_cond_key;
static PSI_mutex_key PSI_consistent_index_lock_key;
static PSI_mutex_key PSI_mysql_innodb_clone_index_lock_key;
static PSI_mutex_key PSI_se_backup_index_lock_key;

/** The instrumentation key to use for opening the index file. */
static PSI_file_key PSI_consistent_archive_mysql_log_index_key;
static PSI_file_key PSI_consistent_archive_se_log_index_key;
static PSI_file_key PSI_consistent_snapshot_file_key;
static PSI_file_key PSI_archive_file_key;
/** The instrumentation key to use for opening a index cache file. */
static PSI_file_key PSI_consistent_archive_mysql_log_index_cache_key;
static PSI_file_key PSI_consistent_archive_se_log_index_cache_key;
static PSI_file_key PSI_consistent_snapshot_file_cache_key;

static PSI_thread_info all_consistent_archive_threads[] = {
    {&THR_KEY_consistent_archive, "archive", "arch", PSI_FLAG_AUTO_SEQNUM, 0,
     PSI_DOCUMENT_ME}};

static PSI_mutex_info all_consistent_archive_mutex_info[] = {
    {&PSI_consistent_index_lock_key, "consistent_mutex", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_mysql_innodb_clone_index_lock_key, "innodb_mutex", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_se_backup_index_lock_key, "se_mutex", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_consistent_archive_lock_key, "archive_mutex", 0, 0, PSI_DOCUMENT_ME}};

static PSI_cond_info all_consistent_archive_cond_info[] = {
    {&PSI_consistent_archive_cond_key, "archive_condition", 0, 0,
     PSI_DOCUMENT_ME}};

static PSI_file_info all_consistent_archive_files[] = {
    {&PSI_consistent_archive_mysql_log_index_key, "innodb_index", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_consistent_archive_mysql_log_index_cache_key, "innodb_index_cache", 0,
     0, PSI_DOCUMENT_ME},
    {&PSI_consistent_archive_se_log_index_key, "se_index", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_consistent_archive_se_log_index_cache_key, "se_index_cache", 0, 0,
     PSI_DOCUMENT_ME},
    {&PSI_archive_file_key, "archive_file", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_consistent_snapshot_file_key, "snapshot_file", 0, 0, PSI_DOCUMENT_ME},
    {&PSI_consistent_snapshot_file_cache_key, "snapshot_file_cache", 0, 0,
     PSI_DOCUMENT_ME}};
#endif

static void *consistent_archive_action(void *arg);
static bool remove_file(const std::string &file);
static bool copy_file(IO_CACHE *from, IO_CACHE *to, my_off_t offset);
static bool file_has_suffix(const std::string &sfx, const std::string &path);
static int compare_log_name(const char *log_1, const char *log_2);
static void exec_binlog_error_action_abort(THD *thd, const char *err_string);
/**
 * @brief Creates a consistent archive object and starts the consistent snapshot
 * archive thread.
 * Called when mysqld main thread mysqld_main().
 */
int start_consistent_archive() {
  DBUG_TRACE;

  if (!opt_consistent_snapshot_archive) {
    LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "consistent snapshot archive not enabled "
           "--consistent-snapshot-archive.");
    return 0;
  }
  // Not set consistent archive dir
  if (opt_mysql_archive_dir == nullptr) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "Mysql archive path not set, please set --mysql-archive-dir.");
    return 1;
  }

  // Check if mysql consistent archive directory exists, and directory is
  // writable.
  MY_STAT stat;
  if (!my_stat(opt_mysql_archive_dir, &stat, MYF(0)) ||
      !MY_S_ISDIR(stat.st_mode) ||
      my_access(opt_mysql_archive_dir, (F_OK | W_OK))) {
    std::string err_msg;
    err_msg.assign("Mysql archive path not exist: ");
    err_msg.append(opt_mysql_archive_dir);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    return 1;
  }
  // data home can not contain archive directory.
  if (test_if_data_home_dir(opt_mysql_archive_dir)) {
    std::string err_msg;
    err_msg.assign("mysql archive path is within the current data directory: ");
    err_msg.append(opt_mysql_archive_dir);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    return 1;
  }

  // Check if archive binlog dir exists. If not exists, create it.
  char tmp_archive_data_dir[FN_REFLEN + 1] = {0};
  strmake(tmp_archive_data_dir, opt_mysql_archive_dir,
          sizeof(tmp_archive_data_dir) - CONSISTENT_ARCHIVE_SUBDIR_LEN - 1);
  strmake(convert_dirname(tmp_archive_data_dir, tmp_archive_data_dir, NullS),
          STRING_WITH_LEN(CONSISTENT_ARCHIVE_SUBDIR));
  convert_dirname(tmp_archive_data_dir, tmp_archive_data_dir, NullS);

  // If consistent snapshot persist to object store,
  // when startup, remove local archive data dir.
  if (opt_persistent_on_objstore) {
    remove_file(tmp_archive_data_dir);
  }
  // Check if the binlog archive dir exists. If not exists, create it.
  if (!my_stat(tmp_archive_data_dir, &stat, MYF(0))) {
    if (my_mkdir(tmp_archive_data_dir, 0777, MYF(0))) {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
             "Failed to create archive data dir.");
      return 1;
    }
  } else {
    if (!MY_S_ISDIR(stat.st_mode) ||
        my_access(tmp_archive_data_dir, (F_OK | W_OK))) {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
             "Mysql archive data path not access.");
      return 1;
    }
  }

  LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
         "start consistent archive");

#ifdef HAVE_PSI_INTERFACE
  const char *category = "consistent";
  int count_thread =
      static_cast<int>(array_elements(all_consistent_archive_threads));
  mysql_thread_register(category, all_consistent_archive_threads, count_thread);

  int count_mutex =
      static_cast<int>(array_elements(all_consistent_archive_mutex_info));
  mysql_mutex_register(category, all_consistent_archive_mutex_info,
                       count_mutex);

  int count_cond =
      static_cast<int>(array_elements(all_consistent_archive_cond_info));
  mysql_cond_register(category, all_consistent_archive_cond_info, count_cond);

  int count_file =
      static_cast<int>(array_elements(all_consistent_archive_files));
  mysql_file_register(category, all_consistent_archive_files, count_file);

#endif

  mysql_mutex_init(PSI_consistent_archive_lock_key, &m_run_lock,
                   MY_MUTEX_INIT_FAST);
  mysql_cond_init(PSI_consistent_archive_cond_key, &m_run_cond);

  mysql_consistent_archive = new Consistent_archive();

  mysql_mutex_lock(&m_run_lock);
  abort_archive = false;
  if (mysql_thread_create(THR_KEY_consistent_archive, &mysql_archiver_pthd,
                          &connection_attrib, consistent_archive_action,
                          (void *)mysql_consistent_archive)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "create consistent archive thread failed");
    mysql_mutex_unlock(&m_run_lock);
    return 1;
  }
  mysql_consistent_archive->thread_set_created();

  while (mysql_consistent_archive->is_thread_alive_not_running()) {
    DBUG_PRINT("sleep", ("Waiting for consisten archive thread to start"));
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_run_cond, &m_run_lock, &abstime);
  }
  mysql_mutex_unlock(&m_run_lock);
  return 0;
}

/**
 * @brief Stops the consistent snapshot archive thread.
 * Called when mysqld main thread clean_up().
 */
void stop_consistent_archive() {
  DBUG_TRACE;

  if (!mysql_consistent_archive) goto end;

  LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
         "stop consistent archive");

  mysql_mutex_lock(&m_run_lock);
  if (mysql_consistent_archive->is_thread_dead()) {
    mysql_mutex_unlock(&m_run_lock);
    goto end;
  }
  mysql_mutex_unlock(&m_run_lock);
  mysql_consistent_archive->terminate_consistent_archive_thread();
  /* Wait until the thread is terminated. */
  my_thread_join(&mysql_archiver_pthd, nullptr);
end:
  if (mysql_consistent_archive) {
    delete mysql_consistent_archive;
    mysql_consistent_archive = nullptr;
    mysql_mutex_destroy(&m_run_lock);
    mysql_cond_destroy(&m_run_cond);
  }
}

/**
 * @brief Consistent archive thread action.
 *
 * @param arg
 * @return void*
 */
static void *consistent_archive_action(void *arg [[maybe_unused]]) {
  Consistent_archive *archiver = (Consistent_archive *)arg;
  archiver->run();
  return nullptr;
}

/**
 * @brief Constructs a Consistent_archive object.
 */
Consistent_archive::Consistent_archive()
    : m_thread(),
      m_thd(nullptr),
      m_clone(nullptr),
      m_diag_area(false),
      m_thd_state(),
      snapshot_objstore(nullptr),
      m_mysql_clone_index_file(),
      m_crash_safe_mysql_clone_index_file(),
      m_se_backup_index_file(),
      m_crash_safe_se_backup_index_file(),
      m_consistent_snapshot_index_file(),
      m_crash_safe_consistent_snapshot_index_file(),
      m_purge_index_file() {
  m_mysql_archive_dir[0] = '\0';
  m_mysql_archive_data_dir[0] = '\0';

  m_mysql_clone_name[0] = '\0';
  m_mysql_innodb_clone_dir[0] = '\0';
  m_mysql_clone_index_file_name[0] = '\0';
  m_crash_safe_mysql_clone_index_file_name[0] = '\0';
  m_mysql_clone_index_number = 0;

  tmp_se_backup_dir_[0] = '\0';
  m_se_backup_index_file_name[0] = '\0';
  m_crash_safe_se_backup_index_file_name[0] = '\0';
  m_se_backup_dir[0] = '\0';
  m_se_bakcup_name[0] = '\0';
  m_se_backup_index_number = 0;

  m_consistent_snapshot_index_file_name[0] = '\0';
  m_crash_safe_consistent_snapshot_index_file_name[0] = '\0';

  m_purge_index_file_name[0] = '\0';

  init();
}

/**
 * @brief Initializes the Consistent_archive object.
 */
void Consistent_archive::init() {
  DBUG_TRACE;
  mysql_mutex_init(PSI_consistent_index_lock_key, &m_consistent_index_lock,
                   MY_MUTEX_INIT_FAST);
  mysql_mutex_init(PSI_mysql_innodb_clone_index_lock_key,
                   &m_mysql_innodb_clone_index_lock, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(PSI_se_backup_index_lock_key, &m_se_backup_index_lock,
                   MY_MUTEX_INIT_FAST);
}

/**
 * @brief Destructs the Consistent_archive object.
 */
Consistent_archive::~Consistent_archive() {
  DBUG_TRACE;
  cleanup();
}

/**
 * @brief Cleans up any resources used by the Consistent_archive.
 */
void Consistent_archive::cleanup() {
  DBUG_TRACE;
  mysql_mutex_destroy(&m_se_backup_index_lock);
  mysql_mutex_destroy(&m_mysql_innodb_clone_index_lock);
  mysql_mutex_destroy(&m_consistent_index_lock);
}

/**
 * @brief Returns the consistent snapshot archive instance.
 * @return Consistent_archive*
 */
Consistent_archive *Consistent_archive::get_instance() {
  return mysql_consistent_archive;
}

/**
 * @brief Terminate the binlog archive thread.
 *
 * @return int
 */
int Consistent_archive::terminate_consistent_archive_thread() {
  DBUG_TRACE;
  mysql_mutex_lock(&m_run_lock);
  abort_archive = true;
  while (m_thd_state.is_thread_alive()) {
    DBUG_PRINT("sleep", ("Waiting for consistent archive thread to stop"));
    // Can not awake(KILL_CONNECTION), otherise last consistent snapshot failed.
    /*
    if (m_thd_state.is_initialized()) {
      mysql_mutex_lock(&m_thd->LOCK_thd_data);
      m_thd->awake(THD::KILL_CONNECTION);
      mysql_mutex_unlock(&m_thd->LOCK_thd_data);
    }
    */
    struct timespec abstime;
    set_timespec(&abstime, 1);
    mysql_cond_timedwait(&m_run_cond, &m_run_lock, &abstime);
  }
  assert(m_thd_state.is_thread_dead());
  mysql_mutex_unlock(&m_run_lock);
  return 0;
}

void Consistent_archive::set_thread_context() {
  THD *thd = new THD;
  my_thread_init();
  thd->set_new_thread_id();
  thd->thread_stack = (char *)&thd;
  thd->store_globals();
  thd->thread_stack = (char *)&thd;
  thd->system_thread = SYSTEM_THREAD_BACKGROUND;
  /* No privilege check needed */
  thd->security_context()->skip_grants();
  // Global_THD_manager::get_instance()->add_thd(thd);
  m_thd = thd;
}

/**
 * @brief Consistent archive thread run function.
 *
 */
void Consistent_archive::run() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("Consistent_archive::run"));
  set_thread_context();

  mysql_mutex_lock(&m_run_lock);
  m_thd_state.set_initialized();
  mysql_cond_broadcast(&m_run_cond);
  mysql_mutex_unlock(&m_run_lock);

  // need to persist the snapshot to the object store.
  if (opt_persistent_on_objstore) {
    std::string_view endpoint(
        opt_objstore_endpoint ? std::string_view(opt_objstore_endpoint) : "");
    snapshot_objstore = objstore::create_object_store(
        std::string_view(opt_objstore_provider),
        std::string_view(opt_objstore_region),
        opt_objstore_endpoint ? &endpoint : nullptr, opt_objstore_use_https);
    if (!snapshot_objstore) {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
             "Failed to create object store instance");
      goto error;
    }
  }

  strmake(m_mysql_archive_dir, opt_mysql_archive_dir,
          sizeof(m_mysql_archive_dir) - 1);
  convert_dirname(m_mysql_archive_dir, m_mysql_archive_dir, NullS);
  strmake(strmake(m_mysql_archive_data_dir, m_mysql_archive_dir,
                  sizeof(m_mysql_archive_data_dir) -
                      CONSISTENT_ARCHIVE_SUBDIR_LEN - 1),
          STRING_WITH_LEN(CONSISTENT_ARCHIVE_SUBDIR));
  convert_dirname(m_mysql_archive_data_dir, m_mysql_archive_data_dir, NullS);
  // init new mysql archive dir name
  {
    LOG_INFO log_info;
    int error = 1;

    strmake(m_mysql_clone_index_file_name, CONSISTENT_INNODB_ARCHIVE_INDEX_FILE,
            sizeof(m_mysql_clone_index_file_name) - 1);

    mysql_mutex_lock(&m_mysql_innodb_clone_index_lock);
    if (open_index_file(CONSISTENT_INNODB_ARCHIVE_INDEX_FILE,
                        CONSISTENT_INNODB_ARCHIVE_INDEX_FILE,
                        ARCHIVE_MYSQL_INNODB)) {
      mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
      goto error;
    }
    // init new innodb index number from mysql clone index file.
    error = find_line_from_index(&log_info, NullS, ARCHIVE_MYSQL_INNODB);
    if (error == 0) {
      char tmp_buf[FN_REFLEN + 1] = {0};
      do {
        strmake(tmp_buf, log_info.log_file_name, sizeof(tmp_buf) - 1);
      } while (!(
          error = find_next_line_from_index(&log_info, ARCHIVE_MYSQL_INNODB)));
      if (error != LOG_INFO_EOF) {
        mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
        goto error;
      }
      // get tmp_buf last 6 char
      char *p = tmp_buf + strlen(tmp_buf) - 6;
      // convert to int
      m_mysql_clone_index_number = atoi(p);
      m_mysql_clone_index_number++;
    } else if (error == LOG_INFO_EOF) {
      // log index file is empty.
      m_mysql_clone_index_number = 1;
    } else {
      mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
      goto error;
    }
    mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
  }

  // init new smartengine archive dir name.
  {
    // select @@SMARTENGINE_DATADIR
    String se_data_dir_tmp, *se_data_dir = nullptr;
    System_variable_tracker se_datadir_var_tracker =
        System_variable_tracker::make_tracker("SMARTENGINE_DATADIR");
    if (se_datadir_var_tracker.access_system_variable(m_thd)) {
      LogErr(
          ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
          "failed to get system configuration parameter SMARTENGINE_DATADIR.");
      goto error;
    }
    Item_func_get_system_var *item =
        new Item_func_get_system_var(se_datadir_var_tracker, OPT_GLOBAL);
    item->fixed = true;
    se_data_dir = item->val_str(&se_data_dir_tmp);
    if (se_data_dir == nullptr) {
      LogErr(
          ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
          "failed to get system configuration parameter SMARTENGINE_DATADIR.");
      goto error;
    }

    strmake(tmp_se_backup_dir_, se_data_dir->c_ptr(),
            sizeof(tmp_se_backup_dir_) - MYSQL_SE_TMP_BACKUP_DIR_LEN - 1);
    strmake(convert_dirname(tmp_se_backup_dir_, tmp_se_backup_dir_, NullS),
            STRING_WITH_LEN(MYSQL_SE_TMP_BACKUP_DIR));
    convert_dirname(tmp_se_backup_dir_, tmp_se_backup_dir_, NullS);

    LOG_INFO log_info;
    int error = 1;

    strmake(m_se_backup_index_file_name, CONSISTENT_SE_ARCHIVE_INDEX_FILE,
            sizeof(m_se_backup_index_file_name) - 1);

    mysql_mutex_lock(&m_se_backup_index_lock);
    if (open_index_file(CONSISTENT_SE_ARCHIVE_INDEX_FILE,
                        CONSISTENT_SE_ARCHIVE_INDEX_FILE, ARCHIVE_SE)) {
      mysql_mutex_unlock(&m_se_backup_index_lock);
      goto error;
    }
    // init new smartengine index number from mysql clone index file.
    error = find_line_from_index(&log_info, NullS, ARCHIVE_SE);
    if (error == 0) {
      char tmp_buf[FN_REFLEN + 1] = {0};
      do {
        strmake(tmp_buf, log_info.log_file_name, sizeof(tmp_buf) - 1);
      } while (!(error = find_next_line_from_index(&log_info, ARCHIVE_SE)));
      if (error != LOG_INFO_EOF) {
        mysql_mutex_unlock(&m_se_backup_index_lock);
        goto error;
      }
      // get tmp_buf last 6 char
      char *p = tmp_buf + strlen(tmp_buf) - 6;
      // convert to int
      m_se_backup_index_number = atoi(p);
      m_se_backup_index_number++;
    } else if (error == LOG_INFO_EOF) {
      // log index file is empty.
      m_se_backup_index_number = 1;
    } else {
      mysql_mutex_unlock(&m_se_backup_index_lock);
      goto error;
    }
    mysql_mutex_unlock(&m_se_backup_index_lock);
  }

  mysql_mutex_lock(&m_run_lock);
  m_thd_state.set_running();
  mysql_cond_broadcast(&m_run_cond);
  mysql_mutex_unlock(&m_run_lock);

  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
         "Consistent snapshot archive thread running");

  for (;;) {
    int ret = 0;
    // Wait for the next archive period.
    std::chrono::seconds timeout =
        std::chrono::seconds{opt_consistent_archive_period};
    DBUG_EXECUTE_IF("check_mysql_consistent_archive_debug",
                    { timeout = std::chrono::seconds{10}; });
    if (m_thd == nullptr || m_thd->killed) break;
    m_thd->clear_error();
    m_thd->get_stmt_da()->reset_diagnostics_area();

    // generate next innodb clone dir name
    sprintf(m_mysql_clone_name, "%s%06u", CONSISTENT_INNODB_ARCHIVE_BASENAME,
            m_mysql_clone_index_number);
    strmake(strmake(m_mysql_innodb_clone_dir, m_mysql_archive_data_dir,
                    sizeof(m_mysql_innodb_clone_dir) -
                        CONSISTENT_INNODB_ARCHIVE_NAME_LEN - 1),
            m_mysql_clone_name, CONSISTENT_INNODB_ARCHIVE_NAME_LEN);

    // generate next smartengine backup dir name
    sprintf(m_se_bakcup_name, "%s%06u", CONSISTENT_SE_ARCHIVE_BASENAME,
            m_se_backup_index_number);
    strmake(
        strmake(m_se_backup_dir, m_mysql_archive_data_dir,
                sizeof(m_se_backup_dir) - CONSISTENT_SE_ARCHIVE_NAME_LEN - 1),
        m_se_bakcup_name, CONSISTENT_SE_ARCHIVE_NAME_LEN);

    ret = wait_for_consistent_archive(timeout);
    assert(ret == 0 || is_timeout(ret));
    // If the thread is killed, again archive last consistent snapshot, before
    // exit.
    mysql_mutex_lock(&m_run_lock);
    if (abort_archive) {
      mysql_mutex_unlock(&m_run_lock);
      std::string err_msg;
      if (archive_consistent_snapshot()) {
        err_msg.assign("Consistent snapshot last archive failed: ");
        err_msg.append(m_mysql_innodb_clone_dir);
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      } else {
        err_msg.assign("Consistent snapshot last archive: ");
        err_msg.append(m_mysql_innodb_clone_dir);
        LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      }
      break;
    }
    mysql_mutex_unlock(&m_run_lock);
    if (archive_consistent_snapshot()) {
      std::string err_msg;
      err_msg.assign("Consistent snapshot archive failed: ");
      err_msg.append(m_mysql_innodb_clone_dir);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    } else {
      std::string err_msg;
      err_msg.assign("Consistent snapshot archive: ");
      err_msg.append(m_mysql_innodb_clone_dir);
      LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    }
    // Regardless of whether the last archiving was successful or not,
    // m_mysql_clone_index_number is incremented.
    m_mysql_clone_index_number++;
    m_se_backup_index_number++;

    // Every time a consistent snapshot is archived, reopens the index file.
    // Ensure that the local index files are consistent with the object store.
    mysql_mutex_lock(&m_mysql_innodb_clone_index_lock);
    close_index_file(ARCHIVE_MYSQL_INNODB);
    if (open_index_file(CONSISTENT_INNODB_ARCHIVE_INDEX_FILE,
                        CONSISTENT_INNODB_ARCHIVE_INDEX_FILE,
                        ARCHIVE_MYSQL_INNODB)) {
      mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
      goto error;
    }
    mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);

    mysql_mutex_lock(&m_se_backup_index_lock);
    close_index_file(ARCHIVE_SE);
    if (open_index_file(CONSISTENT_SE_ARCHIVE_INDEX_FILE,
                        CONSISTENT_SE_ARCHIVE_INDEX_FILE, ARCHIVE_SE)) {
      mysql_mutex_unlock(&m_se_backup_index_lock);
      goto error;
    }
    mysql_mutex_unlock(&m_se_backup_index_lock);
    // Free new Item_***.
    m_thd->free_items();
  }

error:
  if (snapshot_objstore) {
    objstore::destroy_object_store(snapshot_objstore);
    snapshot_objstore = nullptr;
  }
  mysql_mutex_lock(&m_mysql_innodb_clone_index_lock);
  close_index_file(ARCHIVE_MYSQL_INNODB);
  mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
  mysql_mutex_lock(&m_se_backup_index_lock);
  close_index_file(ARCHIVE_SE);
  mysql_mutex_unlock(&m_se_backup_index_lock);

  LogErr(SYSTEM_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
         "Consistent snapshot archive thread end");
  // Free new Item_***.
  m_thd->free_items();
  m_thd->release_resources();
  my_thread_end();
  mysql_mutex_lock(&m_run_lock);
  delete m_thd;
  m_thd = nullptr;
  abort_archive = true;
  m_thd_state.set_terminated();
  mysql_cond_broadcast(&m_run_cond);
  mysql_mutex_unlock(&m_run_lock);
  my_thread_exit(nullptr);
}

/**
 * @brief Wait for the consistent archive thread to start archiving.
 *
 * @param timeout
 * @return int
 */
int Consistent_archive::wait_for_consistent_archive(
    const std::chrono::seconds &timeout) {
  DBUG_TRACE;
  int error = 0;
  struct timespec ts;
  set_timespec(&ts, timeout.count());
  mysql_mutex_lock(&m_run_lock);
  error = mysql_cond_timedwait(&m_run_cond, &m_run_lock, &ts);
  mysql_mutex_unlock(&m_run_lock);
  return error;
}

/**
 * @brief Signal the consistent archive thread to start archiving.
 */
void Consistent_archive::signal_consistent_archive() {
  DBUG_TRACE;
  mysql_mutex_lock(&m_run_lock);
  mysql_cond_broadcast(&m_run_cond);
  mysql_mutex_unlock(&m_run_lock);
  return;
}

/**
 * @brief Consistent archive mysql innodb, and smartengine metas and wals.
 * Refresh consistent_snapshot file.
 */
bool Consistent_archive::archive_consistent_snapshot() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("archive_consistent_snapshot"));
  THD *thd = m_thd;

  /*
    Acquire shared backup lock to block concurrent backup. Acquire exclusive
    backup lock to block any concurrent DDL.
  */
  if (acquire_exclusive_backup_lock(thd, thd->variables.lock_wait_timeout,
                                    false)) {
    // MDL subsystem has to set an error in Diagnostics Area
    assert(thd->is_error());
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           thd->get_stmt_da()->message_text());
    goto err;
  }

  // consistent archive mysql innodb
  if (achive_mysql_innodb()) {
    release_backup_lock(thd);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "archive mysql innodb failed");
    goto err;
  }

  // consisten archive smartengine wals and metas
  if (archive_smartengine()) {
    release_backup_lock(thd);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "archive smartengine failed");
    goto err;
  }
  // release ddl lock
  release_backup_lock(thd);

  // write consistent snapshot file.
  if (write_consistent_snapshot_file()) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "write consistent snapshot file failed");
    goto err;
  }

  MYSQL_END_STATEMENT(thd->m_statement_psi, thd->get_stmt_da());
  thd->m_statement_psi = nullptr;
  thd->m_digest = nullptr;
  thd->free_items();
  return false;

err:
  MYSQL_END_STATEMENT(thd->m_statement_psi, thd->get_stmt_da());
  thd->m_statement_psi = nullptr;
  thd->m_digest = nullptr;
  thd->free_items();
  return true;
}

/**
 * @brief Archive mysql innodb by local clone.
 * Acquired backup lock before archive innodb.
 */
bool Consistent_archive::achive_mysql_innodb() {
  DBUG_TRACE;
  List<set_var_base> tmp_var_list;
  THD *thd = m_thd;

  // set global clone_block_ddl = off, held backup lock already.
  System_variable_tracker block_ddl_var_tracker =
      System_variable_tracker::make_tracker("clone_block_ddl");
  // Free item by m_thd->free_items()
  Item *value = new (thd->mem_root)
      Item_string("off", strlen("off"), &my_charset_utf8mb4_bin);
  set_var *block_ddl_var =
      new (thd->mem_root) set_var(OPT_GLOBAL, block_ddl_var_tracker, value);
  tmp_var_list.push_back(block_ddl_var);

  // set global clone_autotune_concurrency=off
  System_variable_tracker autotune_var_tracker =
      System_variable_tracker::make_tracker("clone_autotune_concurrency");
  value = new (thd->mem_root)
      Item_string("off", strlen("off"), &my_charset_utf8mb4_bin);
  set_var *autotune_var =
      new (thd->mem_root) set_var(OPT_GLOBAL, autotune_var_tracker, value);
  tmp_var_list.push_back(autotune_var);

  // set global clone_max_concurrency=1
  System_variable_tracker concurrency_var_tracker =
      System_variable_tracker::make_tracker("clone_max_concurrency");
  value = new (thd->mem_root) Item_int(1);
  set_var *concurrency_var =
      new (thd->mem_root) set_var(OPT_GLOBAL, concurrency_var_tracker, value);
  tmp_var_list.push_back(concurrency_var);

  LEX *saved_lex = thd->lex, temp_lex;
  LEX *lex = &temp_lex;
  thd->lex = lex;
  if (lex_start(thd) || sql_set_variables(thd, &tmp_var_list, false)) {
    if (thd->is_error())
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
             thd->get_stmt_da()->message_text());
    lex->cleanup(true);
    lex_end(thd->lex);
    thd->lex = saved_lex;
    return true;
  }
  lex->cleanup(true);
  lex_end(thd->lex);
  thd->lex = saved_lex;

  assert(m_clone == nullptr);
  m_clone = clone_plugin_lock(thd, &m_plugin);
  if (m_clone == nullptr) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, "clone plugin load failed");
    return true;
  }
  // clone mysql innodb data to local dir.
  auto err = m_clone->clone_local(thd, m_mysql_innodb_clone_dir);
  clone_plugin_unlock(thd, m_plugin);
  m_clone = nullptr;
  if (err != 0) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "local clone mysql innodb failed");
    return true;
  }

  // get binlog file name and position from clone status file.
  read_mysql_innodb_clone_status();

  // tar mysql innod data dir and
  // upload innodb_archive_000001.tar.gz to s3.
  // If subsequent errors, recycle the archive file by consisent_archive_purge()
  if (snapshot_objstore != nullptr) {
    std::stringstream cmd;
    std::string clone_tar_name;
    clone_tar_name.assign(m_mysql_innodb_clone_dir);
    clone_tar_name.append(CONSISTENT_TAR_SUFFIX);
    cmd << "tar -czf " << clone_tar_name << " ";
    cmd << " --absolute-names --transform 's|^" << m_mysql_archive_data_dir
        << "||' ";
    cmd << m_mysql_innodb_clone_dir;
    if ((err = system(cmd.str().c_str())) != 0) {
      std::string err_msg;
      err_msg.assign("tar innodb clone dir failed: ");
      err_msg.append(cmd.str());
      err_msg.append(" error=");
      err_msg.append(std::to_string(err));
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      // remove local innodb_archive_000001/ dir
      remove_file(m_mysql_innodb_clone_dir);
      return true;
    }

    // upload innodb_archive_000001.tar.gz to object store.
    std::string clone_keyid;
    clone_keyid.assign(m_mysql_clone_name);
    clone_keyid.append(CONSISTENT_TAR_SUFFIX);
    objstore::Status ss = snapshot_objstore->put_object_from_file(
        std::string_view(opt_objstore_bucket), clone_keyid, clone_tar_name);

    // remove local innodb_archive_000001.tar.gz
    remove_file(clone_tar_name);
    // remove local innodb_archive_000001/ dir
    remove_file(m_mysql_innodb_clone_dir);

    if (!ss.is_succ()) {
      std::string err_msg;
      err_msg.assign("upload innodb clone to object store failed: ");
      err_msg.append(" keyid=");
      err_msg.append(clone_keyid);
      err_msg.append(" file=");
      err_msg.append(clone_tar_name);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      return true;
    }
  }

  // append new archive name to local index file 'innodb_archive.index'.
  // If subsequent errors, recover local index file by run()->while reopen
  // index.
  // If an error occurs during add_line_to_index, a garbage file
  // innodb_archive.000000.tar will exist on the object store.
  mysql_mutex_lock(&m_mysql_innodb_clone_index_lock);
  if (DBUG_EVALUATE_IF("fault_injection_persistent_innodb_archive_index_file",
                       1, 0) ||
      add_line_to_index((uchar *)m_mysql_clone_name, strlen(m_mysql_clone_name),
                        ARCHIVE_MYSQL_INNODB)) {
    mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "append to innodb_archive index file failed");
    return true;
  }
  mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);

  // force rotate binlog, wait for binlog rotate complete
  if (mysql_bin_log.is_open() && m_binlog_file[0] != '\0') {
    DBUG_EXECUTE_IF("fault_injection_persistent_innodb_archive_rotate_binlog",
                    { return true; });
    mysql_mutex_lock(mysql_bin_log.get_log_lock());
    bool check_purge = false;
    err = mysql_bin_log.rotate(true, &check_purge);
    mysql_mutex_unlock(mysql_bin_log.get_log_lock());
    if (err != 0) {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, "binlog rotate failed");
      return true;
    }
    // wait archive binlog complete.
    err = binlog_archive_wait_for_update(thd, m_binlog_file, m_binlog_pos);
    if (err != 0) {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
             "binlog archive wait for update failed");
      return true;
    }
  }

  return false;
}

/**
 * @brief Read mysql innodb clone status file.
 * Retrieve binlog file name and pos.
 */
void Consistent_archive::read_mysql_innodb_clone_status() {
  DBUG_TRACE;
  std::string file_name;
  char full_log_name[FN_REFLEN + 1] = {0};

  file_name.assign(m_mysql_innodb_clone_dir);
  file_name.append(FN_DIRSEP);
  file_name.append(MYSQL_LOCAL_CLONE_VIEW_STATUS_FILE);

  // Reset the status variables.
  m_binlog_file[0] = '\0';
  m_binlog_pos = 0;

  std::ifstream status_file;
  status_file.open(file_name, std::ifstream::in);
  if (!status_file.is_open()) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "open clone status file failed");
    return;
  }

  std::string file_line;
  int line_number = 0;
  uint32_t state = 0;
  /* loop through the lines and extract status information. */
  while (std::getline(status_file, file_line)) {
    ++line_number;
    std::stringstream file_data(file_line, std::ifstream::in);
    switch (line_number) {
      case 1:
        /* Read state columns. */
        file_data >> state >> m_id;
        m_state = STATE_NONE;
        if (state < static_cast<uint32_t>(NUM_STATES)) {
          m_state = static_cast<Clone_state>(state);
        }
        break;
      case 2:
        /* Read time columns. */
        file_data >> m_start_time >> m_end_time;
        break;
      case 3:
        /* read source string */
        strncpy(m_source, file_line.c_str(), sizeof(m_source) - 1);
        break;
      case 4:
        /* Read error number. */
        file_data >> m_error_number;
        break;
      case 5:
        /* read error string */
        strncpy(m_error_mesg, file_line.c_str(), sizeof(m_error_mesg) - 1);
        break;
      case 6:
        /* Read binary log file name. */
        strncpy(m_binlog_file, file_line.c_str(), sizeof(m_binlog_file) - 1);
        // extend relative paths for m_binlog_file to be consistent snapshot
        normalize_binlog_name(full_log_name, m_binlog_file, false);
        strmake(m_binlog_file, full_log_name, sizeof(m_binlog_file) - 1);
        break;
      case 7:
        /* Read binary log position. */
        file_data >> m_binlog_pos;
        break;
      case 8:
        /* Read GTID_EXECUTED. */
        m_gtid_string.assign(file_data.str());
        break;
      default:
        m_gtid_string.append("\n");
        m_gtid_string.append(file_data.str());
        break;
    }
  }
  status_file.close();
}

/**
 * @brief Archive smartengine wals and metas.
 * Acquired backup lock before archive smartengine.
 */
bool Consistent_archive::archive_smartengine() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("archive_smartengine"));
  bool error = false;
  THD *thd = m_thd;

  if (trans_begin(thd)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "Failed to archive smartengine begin transaction");
    return true;
  }

  if ((error = acquire_se_snapshot()) ||
      (error = copy_smartengine_wals_and_metas()) ||
      (error = release_se_snapshot())) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "Failed to archive smartengine, rollback.");
    trans_rollback_stmt(thd);
    trans_rollback(thd);
    return true;
  }

  if (trans_commit_stmt(thd) || trans_commit(thd)) {
    trans_rollback_stmt(thd);
    trans_rollback(thd);
    return true;
  }

  return false;
}

/**
 * @brief  smartengine backup consistened point,
 * that is, the instance should restore to this point.
 * SmartEngine's data include base part in sst file and incremental part in
 * memtable, so the backup consistened point include manifest file position and
 * wal file position.
 * @return true if success. or false if fail.
 */
bool Consistent_archive::acquire_se_snapshot() {
  DBUG_TRACE;
  bool res = false;
  THD *thd = m_thd;

  DBUG_EXECUTE_IF("fault_injection_persistent_smartengine_acquire_snapshot",
                  { return true; });

  List<set_var_base> tmp_var_list;
  System_variable_tracker se_hotbackup_var_tracker =
      System_variable_tracker::make_tracker("SMARTENGINE_HOTBACKUP");
  // SET GLOBAL SMARTENGINE_HOTBACKUP = 'checkpoint'
  Item *value = new (thd->mem_root)
      Item_string("checkpoint", strlen("checkpoint"), &my_charset_utf8mb4_bin);
  set_var *se_hotbackup_var =
      new (thd->mem_root) set_var(OPT_GLOBAL, se_hotbackup_var_tracker, value);
  tmp_var_list.push_back(se_hotbackup_var);

  // SET GLOBAL SMARTENGINE_HOTBACKUP = 'acquire'
  value = new (thd->mem_root)
      Item_string("acquire", strlen("acquire"), &my_charset_utf8mb4_bin);
  se_hotbackup_var =
      new (thd->mem_root) set_var(OPT_GLOBAL, se_hotbackup_var_tracker, value);
  tmp_var_list.push_back(se_hotbackup_var);

  // SET GLOBAL SMARTENGINE_HOTBACKUP = 'incremental'
  value = new (thd->mem_root) Item_string("incremental", strlen("incremental"),
                                          &my_charset_utf8mb4_bin);
  se_hotbackup_var =
      new (thd->mem_root) set_var(OPT_GLOBAL, se_hotbackup_var_tracker, value);
  tmp_var_list.push_back(se_hotbackup_var);

  LEX *saved_lex = thd->lex, temp_lex;
  LEX *lex = &temp_lex;
  thd->lex = lex;

  if (lex_start(thd) || sql_set_variables(thd, &tmp_var_list, false)) {
    if (thd->is_error()) {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
             thd->get_stmt_da()->message_text());
    }
    res = true;
  }
  lex->cleanup(true);
  lex_end(thd->lex);
  thd->lex = saved_lex;
  return res;
}

/**
 * @brief Release smartengine snapshot.
 * @return true if success. or false if fail.
 */
bool Consistent_archive::release_se_snapshot() {
  DBUG_TRACE;
  bool res = false;
  THD *thd = m_thd;

  DBUG_EXECUTE_IF("fault_injection_persistent_smartengine_release_snapshot",
                  { return true; });
  List<set_var_base> tmp_var_list;
  // set global SMARTENGINE_HOTBACKUP = 'release'
  System_variable_tracker se_hotbackup_var_tracker =
      System_variable_tracker::make_tracker("SMARTENGINE_HOTBACKUP");
  Item *value = new (thd->mem_root)
      Item_string("release", strlen("release"), &my_charset_utf8mb4_bin);
  set_var *se_hotbackup_var =
      new (thd->mem_root) set_var(OPT_GLOBAL, se_hotbackup_var_tracker, value);
  tmp_var_list.push_back(se_hotbackup_var);

  LEX *saved_lex = thd->lex, temp_lex;
  LEX *lex = &temp_lex;
  thd->lex = lex;

  if (lex_start(thd) || sql_set_variables(thd, &tmp_var_list, false)) {
    if (thd->is_error()) {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
             thd->get_stmt_da()->message_text());
    }
    res = true;
  }
  lex->cleanup(true);
  lex_end(thd->lex);
  thd->lex = saved_lex;
  return res;
}

/**
 * @brief Copy file from smartengine data directory to mysql archive directory.
 *
 */
bool Consistent_archive::copy_smartengine_wals_and_metas() {
  DBUG_TRACE;
  DBUG_PRINT("info", ("copy_smartengine_wals_and_metas"));

  DBUG_EXECUTE_IF("fault_injection_persistent_smartengine_copy_wals_and_metas",
                  { return true; });

  if (my_mkdir(m_se_backup_dir, 0777, MYF(0)) < 0) {
    std::string err_msg;
    err_msg.assign("Failed to create smartengine archive local directory: ");
    err_msg.append(m_se_backup_dir);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    return true;
  }

  MY_DIR *dir_info = my_dir(tmp_se_backup_dir_, MYF(0));
  // copy files in tmp_se_backup_dir_ to local m_se_backup_dir.
  for (uint i = 0; i < dir_info->number_off_files; i++) {
    FILEINFO *file = dir_info->dir_entry + i;
    char ds_source[FN_REFLEN + 1] = {0};
    char ds_destination[FN_REFLEN + 1] = {0};

    // skip . and .. directories
    if (0 == strcmp(file->name, ".") || 0 == strcmp(file->name, "..")) {
      /**ignore special directories*/
      continue;
    }
    // skip *.sst files
    if (file_has_suffix(MYSQL_SE_DATA_FILE_SUFFIX, file->name)) continue;

    strmake(ds_source, tmp_se_backup_dir_,
            sizeof(ds_source) - strlen(file->name) - 1);
    strmake(convert_dirname(ds_source, ds_source, NullS), file->name,
            strlen(file->name));

    strmake(ds_destination, m_se_backup_dir,
            sizeof(ds_destination) - strlen(file->name) - 1);
    strmake(convert_dirname(ds_destination, ds_destination, NullS), file->name,
            strlen(file->name));
    DBUG_PRINT("info", ("Copying file: %s to %s", ds_source, ds_destination));

    int ret = my_copy(ds_source, ds_destination, MYF(MY_HOLD_ORIGINAL_MODES));
    if (ret) {
      std::string err_msg;
      err_msg.assign("Failed to copy file: ");
      err_msg.append(ds_source);
      err_msg.append(" to ");
      err_msg.append(ds_destination);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      my_dirend(dir_info);
      // remove local garbage files
      remove_file(m_se_backup_dir);
      return true;
    }
  }
  my_dirend(dir_info);

  // upload smartengine archive to object store
  if (snapshot_objstore != nullptr) {
    std::stringstream cmd;
    std::string se_tar_name;
    se_tar_name.assign(m_se_backup_dir);
    se_tar_name.append(CONSISTENT_TAR_SUFFIX);
    cmd << "tar -czf " << se_tar_name << " ";
    cmd << " --absolute-names --transform 's|^" << m_mysql_archive_data_dir
        << "||' ";
    cmd << m_se_backup_dir;
    if (system(cmd.str().c_str())) {
      std::string err_msg;
      err_msg.assign("Failed to tar se_backup");
      err_msg.append(se_tar_name);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      return true;
    }

    // upload se_backup000000.tar to s3
    std::string se_keyid;
    se_keyid.assign(m_se_bakcup_name);
    se_keyid.append(CONSISTENT_TAR_SUFFIX);
    objstore::Status ss = snapshot_objstore->put_object_from_file(
        std::string_view(opt_objstore_bucket), se_keyid, se_tar_name);

    // remove local se_archive_000001.tar and se_archive_000001/
    remove_file(se_tar_name);
    remove_file(m_se_backup_dir);

    if (!ss.is_succ()) {
      std::string err_msg;
      err_msg.assign("Failed to upload se_backup to s3: ");
      err_msg.append("key=");
      err_msg.append(se_keyid);
      err_msg.append(" file=");
      err_msg.append(se_tar_name);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      return true;
    }
  }
  // add smartengine backup name to se_backup.index,
  // And upload se_backup.index to s3.  
  mysql_mutex_lock(&m_se_backup_index_lock);
  if (DBUG_EVALUATE_IF("fault_injection_persistent_smartengine_copy_index_file",
                       1, 0) ||
      add_line_to_index((uchar *)m_se_bakcup_name, strlen(m_se_bakcup_name),
                        ARCHIVE_SE)) {
    mysql_mutex_unlock(&m_se_backup_index_lock);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "Failed to add se_backup to se_backup.index");
    return true;
  }
  mysql_mutex_unlock(&m_se_backup_index_lock);
  return false;
}

/**
 * @brief Write consistent snapshot file, when archive finished.
 * Only a consistent snapshot file is written to the archive directory.
 * Instance can restore to this point. Or recycle mysql innodb clone and
 * smartengine backup before this point.
 */
bool Consistent_archive::write_consistent_snapshot_file() {
  DBUG_TRACE;
  std::string file_name;
  std::ofstream status_file;
  objstore::Status ss;

  DBUG_EXECUTE_IF("fault_injection_persistent_consistent_snapshot_file",
                  { return true; });
  /* Append data directory if cloning to different place. */
  file_name.assign(m_mysql_archive_dir);
  file_name.append(FN_DIRSEP);
  file_name.append(CONSISTENT_SNAPSHOT_FILE);

  mysql_mutex_lock(&m_consistent_index_lock);
  status_file.open(file_name, std::ofstream::out | std::ofstream::trunc);
  if (!status_file.is_open()) {
    std::string err_msg;
    err_msg.assign("Failed to open consistent snapshot file: ");
    err_msg.append(file_name);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    goto err;
  }

  /* Write mysql innodb clone name. */
  status_file << m_mysql_clone_name << std::endl;

  /* Write smartengine backup name. */
  status_file << m_se_bakcup_name << std::endl;

  /* Write binary log information. */
  status_file << m_binlog_file << std::endl;
  status_file << m_binlog_pos << std::endl;
  status_file.close();
  // upload to s3
  if (snapshot_objstore != nullptr) {
    ss = snapshot_objstore->put_object_from_file(
        std::string_view(opt_objstore_bucket),
        std::string_view(CONSISTENT_SNAPSHOT_FILE), file_name);
    if (!ss.is_succ()) {
      std::string err_msg;
      err_msg.assign("Failed to upload consistent snapshot file to s3: ");
      err_msg.append("key=");
      err_msg.append(CONSISTENT_SNAPSHOT_FILE);
      err_msg.append(" file=");
      err_msg.append(file_name);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      goto err;
    }
  }
  mysql_mutex_unlock(&m_consistent_index_lock);
  return false;
err:
  mysql_mutex_unlock(&m_consistent_index_lock);
  return true;
}

/**
 * @brief Read consistent snapshot file
 *
 * @param cs
 * @return true
 * @return false
 */
bool Consistent_archive::read_consistent_snapshot_file(
    Consistent_snapshot *cs) {
  DBUG_TRACE;
  mysql_mutex_assert_owner(&m_consistent_index_lock);
  std::string consistent_file_name;
  consistent_file_name.assign(m_mysql_archive_dir);
  consistent_file_name.append(FN_DIRSEP);
  consistent_file_name.append(CONSISTENT_SNAPSHOT_FILE);
  if (snapshot_objstore != nullptr) {
    // Delete the local consistent snapshot file if it exists.
    remove_file(consistent_file_name);
    objstore::Status ss = snapshot_objstore->get_object_to_file(
        std::string_view(opt_objstore_bucket),
        std::string_view(CONSISTENT_SNAPSHOT_FILE), consistent_file_name);
    if (!ss.is_succ()) {
      return true;
    }

    if (my_chmod(consistent_file_name.c_str(),
                 USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE |
                     OTHERS_WRITE | OTHERS_READ,
                 MYF(MY_WME))) {
      return true;
    }
  }

  std::ifstream consistent_file;
  consistent_file.open(consistent_file_name, std::ifstream::in);
  if (!consistent_file.is_open()) {
    return true;
  }

  std::string file_line;
  int line_number = 0;
  /* loop through the lines and extract consistent snapshot information. */
  while (std::getline(consistent_file, file_line)) {
    ++line_number;
    std::stringstream file_data(file_line, std::ifstream::in);
    switch (line_number) {
      case 1:
        strncpy(cs->mysql_clone_name, file_line.c_str(),
                sizeof(cs->mysql_clone_name) - 1);
        break;
      case 2:
        strncpy(cs->se_backup_name, file_line.c_str(),
                sizeof(cs->se_backup_name) - 1);
        break;
      case 3:
        strncpy(cs->binlog_name, file_line.c_str(),
                sizeof(cs->binlog_name) - 1);
        break;
      case 4:
        file_data >> cs->binlog_pos;
        break;
      default:
        break;
    }
  }
  consistent_file.close();
  return false;
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
int Consistent_archive::open_crash_safe_index_file(Archive_type arch_type) {
  DBUG_TRACE;
  int error = 0;
  File file = -1;
  IO_CACHE *crash_safe_index_file;
  char *crash_safe_index_file_name;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    crash_safe_index_file = &m_crash_safe_mysql_clone_index_file;
    crash_safe_index_file_name = m_crash_safe_mysql_clone_index_file_name;
  } else if (arch_type == ARCHIVE_SE) {
    crash_safe_index_file = &m_crash_safe_se_backup_index_file;
    crash_safe_index_file_name = m_crash_safe_se_backup_index_file_name;
  } else {
    crash_safe_index_file = &m_crash_safe_consistent_snapshot_index_file;
    crash_safe_index_file_name =
        m_crash_safe_consistent_snapshot_index_file_name;
  }

  if (!my_b_inited(crash_safe_index_file)) {
    myf flags = MY_WME | MY_NABP | MY_WAIT_IF_FULL;

    if ((file = my_open(crash_safe_index_file_name, O_RDWR | O_CREAT,
                        MYF(MY_WME))) < 0 ||
        init_io_cache(crash_safe_index_file, file, IO_SIZE, WRITE_CACHE, 0,
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
int Consistent_archive::set_crash_safe_index_file_name(
    const char *base_file_name, Archive_type arch_type) {
  DBUG_TRACE;
  int error = 0;
  char *crash_safe_index_file_name;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    crash_safe_index_file_name = m_crash_safe_mysql_clone_index_file_name;
  } else if (arch_type == ARCHIVE_SE) {
    crash_safe_index_file_name = m_crash_safe_se_backup_index_file_name;
  } else {
    crash_safe_index_file_name =
        m_crash_safe_consistent_snapshot_index_file_name;
  }
  if (fn_format(crash_safe_index_file_name, base_file_name,
                m_mysql_archive_data_dir, ".index_crash_safe",
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
int Consistent_archive::close_crash_safe_index_file(Archive_type arch_type) {
  DBUG_TRACE;
  int error = 0;
  IO_CACHE *crash_safe_index_file;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    crash_safe_index_file = &m_crash_safe_mysql_clone_index_file;
  } else if (arch_type == ARCHIVE_SE) {
    crash_safe_index_file = &m_crash_safe_se_backup_index_file;
  } else {
    crash_safe_index_file = &m_crash_safe_consistent_snapshot_index_file;
  }

  if (my_b_inited(crash_safe_index_file)) {
    end_io_cache(crash_safe_index_file);
    error = my_close(crash_safe_index_file->file, MYF(0));
  }
  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    m_crash_safe_mysql_clone_index_file = IO_CACHE();
  } else if (arch_type == ARCHIVE_SE) {
    m_crash_safe_se_backup_index_file = IO_CACHE();
  } else {
    m_crash_safe_consistent_snapshot_index_file = IO_CACHE();
  }

  return error;
}

/**
  Move crash safe index file to index file.

  @param need_lock_index If true, LOCK_index will be acquired;
  otherwise it should already be held.

  @retval 0 ok
  @retval -1 error
*/
int Consistent_archive::move_crash_safe_index_file_to_index_file(
    Archive_type arch_type) {
  DBUG_TRACE;
  int error = 0;
  File fd = -1;
  int failure_trials = MYSQL_BIN_LOG::MAX_RETRIES_FOR_DELETE_RENAME_FAILURE;
  bool file_rename_status = false, file_delete_status = false;
  THD *thd = m_thd;

  char *crash_safe_index_file_name;
  IO_CACHE *index_file;
  char *index_file_name;
  PSI_file_key *index_file_key;
  PSI_file_key *index_file_cache_key;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    crash_safe_index_file_name = m_crash_safe_mysql_clone_index_file_name;
    index_file = &m_mysql_clone_index_file;
    index_file_name = m_mysql_clone_index_file_name;
    index_file_key = &PSI_consistent_archive_mysql_log_index_key;
    index_file_cache_key = &PSI_consistent_archive_mysql_log_index_cache_key;
  } else if (arch_type == ARCHIVE_SE) {
    crash_safe_index_file_name = m_crash_safe_se_backup_index_file_name;
    index_file = &m_se_backup_index_file;
    index_file_name = m_se_backup_index_file_name;
    index_file_key = &PSI_consistent_archive_se_log_index_key;
    index_file_cache_key = &PSI_consistent_archive_se_log_index_cache_key;
  } else {
    crash_safe_index_file_name =
        m_crash_safe_consistent_snapshot_index_file_name;
    index_file = &m_consistent_snapshot_index_file;
    index_file_name = m_consistent_snapshot_index_file_name;
    index_file_key = &PSI_consistent_snapshot_file_key;
    index_file_cache_key = &PSI_consistent_snapshot_file_cache_key;
  }

  /*
  if (need_lock_index)
    mysql_mutex_lock(&LOCK_index);
  else
    mysql_mutex_assert_owner(&LOCK_index);
  */

  if (my_b_inited(index_file)) {
    end_io_cache(index_file);
    if (mysql_file_close(index_file->file, MYF(0)) < 0) {
      error = -1;
      /*
        Delete Crash safe index file here and recover the binlog.index
        state(index_file io_cache) from old binlog.index content.
       */
      mysql_file_delete(*index_file_key, crash_safe_index_file_name, MYF(0));

      goto recoverable_err;
    }

    /*
      Sometimes an outsider can lock index files for temporary viewing
      purpose. For eg: MEB locks binlog.index/relaylog.index to view
      the content of the file. During that small period of time, deletion
      of the file is not possible on some platforms(Eg: Windows)
      Server should retry the delete operation for few times instead of
      panicking immediately.
    */
    while ((file_delete_status == false) && (failure_trials > 0)) {
      file_delete_status =
          !(mysql_file_delete(*index_file_key, index_file_name, MYF(MY_WME)));
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
        state(m_mysql_clone_index_file io_cache) from old binlog.index content.
       */
      mysql_file_delete(*index_file_key, crash_safe_index_file_name, MYF(0));

      goto recoverable_err;
    }
  }

  /*
    Sometimes an outsider can lock index files for temporary viewing
    purpose. For eg: MEB locks binlog.index/relaylog.index to view
    the content of the file. During that small period of time, rename
    of the file is not possible on some platforms(Eg: Windows)
    Server should retry the rename operation for few times instead of panicking
    immediately.
  */
  failure_trials = MYSQL_BIN_LOG::MAX_RETRIES_FOR_DELETE_RENAME_FAILURE;
  while ((file_rename_status == false) && (failure_trials > 0)) {
    file_rename_status =
        !(my_rename(crash_safe_index_file_name, index_file_name, MYF(MY_WME)));
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
  if ((fd = mysql_file_open(*index_file_key, index_file_name, O_RDWR | O_CREAT,
                            MYF(MY_WME))) < 0 ||
      mysql_file_sync(fd, MYF(MY_WME)) ||
      init_io_cache_ext(index_file, fd, IO_SIZE, READ_CACHE,
                        mysql_file_seek(fd, 0L, MY_SEEK_END, MYF(0)), false,
                        MYF(MY_WME | MY_WAIT_IF_FULL), *index_file_cache_key)) {
    goto fatal_err;
  }

  // if (need_lock_index) mysql_mutex_unlock(&LOCK_index);
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
    LogErr(
        ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
        "MySQL server failed to update the archive binlog.index file's "
        "content properly. It might not be in sync with available binlogs and "
        "the binlog.index file state is in unrecoverable state.");
  }
  return error;
}

/**
  When a fatal error occurs due to which binary logging becomes impossible and
  the user specified binlog_error_action= ABORT_SERVER the following function is
  invoked. This function pushes the appropriate error message to client and logs
  the same to server error log and then aborts the server.

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
    my_error(ER_CONSISTENT_SNAPSHOT_LOG, MYF(ME_FATALERROR), err_string);
  }

  LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_string);
  flush_error_log_messages();

  my_abort();
}

/**
  Append log file name to index file.

  - To make crash safe, we copy all the content of index file
  to crash safe index file firstly and then append the log
  file name to the crash safe index file. Finally move the
  crash safe index file to index file.
  @note Must mutext held before called.
  @retval
    0   ok
  @retval
    -1   error
*/
int Consistent_archive::add_line_to_index(uchar *log_name, size_t log_name_len,
                                          Archive_type arch_type) {
  DBUG_TRACE;
  IO_CACHE *crash_safe_index_file;
  std::string crash_safe_index_file_name;
  IO_CACHE *index_file;
  std::string index_file_name;
  std::string index_keyid;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    crash_safe_index_file = &m_crash_safe_mysql_clone_index_file;
    crash_safe_index_file_name.assign(m_crash_safe_mysql_clone_index_file_name);
    index_file = &m_mysql_clone_index_file;
    index_file_name.assign(m_mysql_clone_index_file_name);
    index_keyid.assign(CONSISTENT_INNODB_ARCHIVE_INDEX_FILE);
  } else if (arch_type == ARCHIVE_SE) {
    crash_safe_index_file = &m_crash_safe_se_backup_index_file;
    crash_safe_index_file_name.assign(m_crash_safe_se_backup_index_file_name);
    index_file = &m_se_backup_index_file;
    index_file_name.assign(m_se_backup_index_file_name);
    index_keyid.assign(CONSISTENT_SE_ARCHIVE_INDEX_FILE);
  } else {
    crash_safe_index_file = &m_crash_safe_consistent_snapshot_index_file;
    crash_safe_index_file_name.assign(
        m_crash_safe_consistent_snapshot_index_file_name);
    index_file = &m_consistent_snapshot_index_file;
    index_file_name.assign(m_consistent_snapshot_index_file_name);
    index_keyid.assign(CONSISTENT_SNAPSHOT_INDEX_FILE);
  }

  if (!my_b_inited(index_file)) {
    return LOG_INFO_IO;
  }

  if (open_crash_safe_index_file(arch_type)) {
    goto err;
  }

  if (copy_file(index_file, crash_safe_index_file, 0)) {
    goto err;
  }

  if (my_b_write(crash_safe_index_file, log_name, log_name_len) ||
      my_b_write(crash_safe_index_file, pointer_cast<const uchar *>("\n"), 1) ||
      flush_io_cache(crash_safe_index_file) ||
      mysql_file_sync(crash_safe_index_file->file, MYF(MY_WME))) {
    goto err;
  }

  if (close_crash_safe_index_file(arch_type)) {
    goto err;
  }

  if (snapshot_objstore != nullptr) {
    // upload index file to s3
    objstore::Status ss = snapshot_objstore->put_object_from_file(
        std::string_view(opt_objstore_bucket), index_keyid,
        crash_safe_index_file_name);
    if (!ss.is_succ()) {
      std::string err_msg;
      err_msg.assign("Failed to upload index file to object store: ");
      err_msg.append("key=");
      err_msg.append(index_keyid);
      err_msg.append(" file=");
      err_msg.append(crash_safe_index_file_name);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      goto err;
    }
  }

  // If upload to obj failed, rerun the binlog archiving from run()->while{}.
  // Will recover the index file from object store, when open_index_file.
  if (move_crash_safe_index_file_to_index_file(arch_type)) {
    goto err;
  }

  return 0;

err:
  return -1;
}

bool Consistent_archive::open_index_file(const char *index_file_name_arg,
                                         const char *log_name,
                                         Archive_type arch_type) {
  DBUG_TRACE;
  bool error = false;
  File index_file_nr = -1;
  char *crash_safe_index_file_name;
  IO_CACHE *index_file;
  char *index_file_name;
  PSI_file_key *index_file_key;
  PSI_file_key *index_file_cache_key;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    crash_safe_index_file_name = m_crash_safe_mysql_clone_index_file_name;
    index_file = &m_mysql_clone_index_file;
    index_file_name = m_mysql_clone_index_file_name;
    index_file_key = &PSI_consistent_archive_mysql_log_index_key;
    index_file_cache_key = &PSI_consistent_archive_mysql_log_index_cache_key;
  } else if (arch_type == ARCHIVE_SE) {
    crash_safe_index_file_name = m_crash_safe_se_backup_index_file_name;
    index_file = &m_se_backup_index_file;
    index_file_name = m_se_backup_index_file_name;
    index_file_key = &PSI_consistent_archive_se_log_index_key;
    index_file_cache_key = &PSI_consistent_archive_se_log_index_cache_key;
  } else {
    crash_safe_index_file_name =
        m_crash_safe_consistent_snapshot_index_file_name;
    index_file = &m_consistent_snapshot_index_file;
    index_file_name = m_consistent_snapshot_index_file_name;
    index_file_key = &PSI_consistent_snapshot_file_key;
    index_file_cache_key = &PSI_consistent_snapshot_file_cache_key;
  }

  /*
    First open of this class instance
    Create an index file that will hold all file names uses for logging.
    Add new entries to the end of it.
  */
  myf opt = MY_UNPACK_FILENAME | MY_REPLACE_DIR;

  if (my_b_inited(index_file)) goto end;

  if (!index_file_name_arg) {
    index_file_name_arg = log_name;  // Use same basename for index file
    opt = MY_UNPACK_FILENAME | MY_REPLACE_EXT | MY_REPLACE_DIR;
  }
  fn_format(index_file_name, index_file_name_arg, m_mysql_archive_data_dir,
            ".index", opt);

  if (set_crash_safe_index_file_name(index_file_name_arg, arch_type)) {
    error = true;
    goto end;
  }

  /*
    We need move m_crash_safe_mysql_clone_index_file to m_mysql_clone_index_file
    if the m_mysql_clone_index_file does not exist and
    m_crash_safe_mysql_clone_index_file exists when mysqld server restarts.
  */
  if (my_access(index_file_name, F_OK) &&
      !my_access(crash_safe_index_file_name, F_OK) &&
      my_rename(crash_safe_index_file_name, index_file_name, MYF(MY_WME))) {
    error = true;
    goto end;
  }

  // Check if the index file exists in s3, if so, download it to local.
  // And rename it to index_file_name.
  if (snapshot_objstore != nullptr) {
    std::string index_file_name_str;
    index_file_name_str.assign(crash_safe_index_file_name);
    std::string index_keyid;
    index_keyid.assign(index_file_name_arg);
    auto status = snapshot_objstore->get_object_to_file(
        std::string_view(opt_objstore_bucket), index_keyid,
        index_file_name_str);
    // If the index file exists in s3, download it to local.
    if (status.is_succ()) {
      if (my_rename(crash_safe_index_file_name, index_file_name, MYF(MY_WME))) {
        error = true;
        goto end;
      }
    }
  }

  if ((index_file_nr = mysql_file_open(*index_file_key, index_file_name,
                                       O_RDWR | O_CREAT, MYF(MY_WME))) < 0 ||
      mysql_file_sync(index_file_nr, MYF(MY_WME)) ||
      init_io_cache_ext(index_file, index_file_nr, IO_SIZE, READ_CACHE,
                        mysql_file_seek(index_file_nr, 0L, MY_SEEK_END, MYF(0)),
                        false, MYF(MY_WME | MY_WAIT_IF_FULL),
                        *index_file_cache_key) ||
      DBUG_EVALUATE_IF("fault_injection_openning_index", 1, 0)) {
    /*
      TODO: all operations creating/deleting the index file or a log, should
      call my_sync_dir() or my_sync_dir_by_file() to be durable.
      TODO: file creation should be done with mysql_file_create()
      not mysql_file_open().
    */
    if (index_file_nr >= 0) mysql_file_close(index_file_nr, MYF(0));
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
  if (set_purge_index_file_name(index_file_name_arg) ||
      open_purge_index_file(false) || purge_index_entry(nullptr) ||
      close_purge_index_file()) {
    LogErr(ERROR_LEVEL, ER_BINLOG_ARCHIVE_LOG,
           "Failed to set purge index file.");
    error = true;
    goto end;
  }

end:
  return error;
}

void Consistent_archive::close_index_file(Archive_type arch_type) {
  DBUG_TRACE;
  IO_CACHE *index_file;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    index_file = &m_mysql_clone_index_file;
  } else if (arch_type == ARCHIVE_SE) {
    index_file = &m_se_backup_index_file;
  } else {
    index_file = &m_consistent_snapshot_index_file;
  }

  if (my_b_inited(index_file)) {
    end_io_cache(index_file);
    if (mysql_file_close(index_file->file, MYF(0)))
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
             "Failed to close index file");
  }
}

static int compare_log_name(const char *log_1, const char *log_2) {
  DBUG_TRACE;
  const char *log_1_basename = log_1 + dirname_length(log_1);
  const char *log_2_basename = log_2 + dirname_length(log_2);

  return strcmp(log_1_basename, log_2_basename);
}

/**
  Find the position in the log-index-file for the given log name.

  @param[out] linfo The found log file name will be stored here, along
  with the byte offset of the next log file name in the index file.
  @param match_name Filename to find in the index file, or NULL if we
  want to read the first entry.
  @param need_lock_index If false, this function acquires LOCK_index;
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
int Consistent_archive::find_line_from_index(LOG_INFO *linfo,
                                             const char *match_name,
                                             Archive_type arch_type) {
  DBUG_TRACE;
  int error = 0;
  char *fname = linfo->log_file_name;
  IO_CACHE *index_file;

  fname[0] = 0;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    index_file = &m_mysql_clone_index_file;
  } else if (arch_type == ARCHIVE_SE) {
    index_file = &m_se_backup_index_file;
  } else {
    index_file = &m_consistent_snapshot_index_file;
  }

  if (!my_b_inited(index_file)) {
    error = LOG_INFO_IO;
    return error;
  }

  /* As the file is flushed, we can't get an error here */
  my_b_seek(index_file, (my_off_t)0);

  for (;;) {
    size_t length;
    my_off_t offset = my_b_tell(index_file);

    /* If we get 0 or 1 characters, this is the end of the file */
    if ((length = my_b_gets(index_file, fname, FN_REFLEN)) <= 1) {
      /* Did not find the given entry; Return not found or error */
      error = !index_file->error ? LOG_INFO_EOF : LOG_INFO_IO;
      break;
    }

    length = strlen(fname);
    // Strips the CR+LF at the end of log name and \0-terminates it.
    if (length && fname[length - 1] == '\n') {
      fname[length - 1] = 0;
      length--;
      if (length && fname[length - 1] == '\r') {
        fname[length - 1] = 0;
        length--;
      }
    }
    if (!length) {
      error = LOG_INFO_EOF;
      break;
    }

    // if the log entry matches, null string matching anything
    if (!match_name || !compare_log_name(fname, match_name)) {
      DBUG_PRINT("info", ("Found log file entry"));
      linfo->index_file_start_offset = offset;
      linfo->index_file_offset = my_b_tell(index_file);
      break;
    }
    linfo->entry_index++;
  }

  // if (need_lock_index) mysql_mutex_unlock(&LOCK_index);
  return error;
}

/**
  Find the position in the log-index-file for the given log name.

  @param[out] linfo The filename will be stored here, along with the
  byte offset of the next filename in the index file.

  @param need_lock_index If true, LOCK_index will be acquired;
  otherwise it should already be held by the caller.

  @note
    - Before calling this function, one has to call find_line_from_index()
    to set up 'linfo'
    - Mutex needed because we need to make sure the file pointer does not move
    from under our feet

  @retval 0 ok
  @retval LOG_INFO_EOF End of log-index-file found
  @retval LOG_INFO_IO Got IO error while reading file
*/
int Consistent_archive::find_next_line_from_index(LOG_INFO *linfo,
                                                  Archive_type arch_type) {
  DBUG_TRACE;
  int error = 0;
  size_t length;
  char *fname = linfo->log_file_name;
  IO_CACHE *index_file;
  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    index_file = &m_mysql_clone_index_file;
  } else if (arch_type == ARCHIVE_SE) {
    index_file = &m_se_backup_index_file;
  } else {
    index_file = &m_consistent_snapshot_index_file;
  }

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
    length = strlen(fname);
    // Strips the CR+LF at the end of log name and \0-terminates it.
    if (length && fname[length - 1] == '\n') {
      fname[length - 1] = 0;
      length--;
      if (length && fname[length - 1] == '\r') {
        fname[length - 1] = 0;
        length--;
      }
    }
    if (!length) {
      error = LOG_INFO_EOF;
      goto err;
    }
  }

  linfo->index_file_offset = my_b_tell(index_file);

err:
  // if (need_lock_index) mysql_mutex_unlock(&LOCK_index);
  return error;
}

/**
 * @brief Purge the pesistent consistent snapshot archive file.
 * @return std::tuple<bool, std::string>
 */
std::tuple<int, std::string> Consistent_archive::purge_consistent_snapshot() {
  DBUG_TRACE;
  int error = 0;
  Consistent_snapshot consistent_snapshot{};
  std::string err_msg{};

  mysql_mutex_lock(&m_consistent_index_lock);
  if (read_consistent_snapshot_file(&consistent_snapshot)) {
    mysql_mutex_unlock(&m_consistent_index_lock);
    err_msg.assign("Failed to read #consistent_snapshot");
    error = 1;
    goto err;
  }
  mysql_mutex_unlock(&m_consistent_index_lock);

  mysql_mutex_lock(&m_mysql_innodb_clone_index_lock);
  error =
      purge_archive(consistent_snapshot.mysql_clone_name, ARCHIVE_MYSQL_INNODB);
  if (error) {
    err_msg.assign("Failed to purge mysql innodb persistent file: ");
    err_msg.append(consistent_snapshot.mysql_clone_name);
    mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);
    goto err;
  }
  mysql_mutex_unlock(&m_mysql_innodb_clone_index_lock);

  mysql_mutex_lock(&m_se_backup_index_lock);
  error = purge_archive(consistent_snapshot.se_backup_name, ARCHIVE_SE);
  if (error) {
    err_msg.assign("Failed to purge mysql innodb persistent file: ");
    err_msg.append(consistent_snapshot.mysql_clone_name);
    mysql_mutex_unlock(&m_se_backup_index_lock);
    goto err;
  }
  mysql_mutex_unlock(&m_se_backup_index_lock);

  err_msg.assign("Purge consistent snapshot successfully: ");
  err_msg.append(consistent_snapshot.mysql_clone_name);
  err_msg.append(" ");
  err_msg.append(consistent_snapshot.se_backup_name);
  LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
  return std::make_tuple(error, err_msg);
err:
  LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
  return std::make_tuple(error, err_msg);
}

int Consistent_archive::purge_archive(const char *match_name,
                                      Archive_type arch_type) {
  DBUG_TRACE;
  int error = 0;
  std::string err_msg{};
  LOG_INFO log_info;

  // Check if exists.
  if ((error = find_line_from_index(&log_info, match_name, arch_type))) {
    err_msg.assign("Failed to find log pos");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    goto err;
  }

  if ((error = open_purge_index_file(true))) {
    err_msg.assign("Failed to open purge index");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    goto err;
  }

  /*
    File name exists in index file; delete until we find this file
    or a file that is used.
  */
  if ((error = find_line_from_index(&log_info, NullS, arch_type))) {
    err_msg.assign("Failed to find log pos");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    goto err;
  }

  while (compare_log_name(match_name, log_info.log_file_name)) {
    if ((error = register_purge_index_entry(log_info.log_file_name))) {
      err_msg.assign("Failed to register purge");
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      goto err;
    }

    if (find_next_line_from_index(&log_info, arch_type)) break;
  }

  if ((error = sync_purge_index_file())) {
    err_msg.assign("Failed to sync purge index");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    goto err;
  }

  /* We know how many files to delete. Update index file. */
  if ((error = remove_line_from_index(&log_info, arch_type))) {
    err_msg.assign("Failed to remove logs from index");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    goto err;
  }

err:
  int error_index = 0, close_error_index = 0;
  /* Read each entry from m_purge_index_file and delete the file. */
  if (!error && purge_index_file_is_inited() &&
      (error_index = purge_index_entry(nullptr))) {
    err_msg.assign("Failed to purge index entry");
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
  }

  close_error_index = close_purge_index_file();

  /*
    Error codes from purge logs take precedence.
    Then error codes from purging the index entry.
    Finally, error codes from closing the purge index file.
  */
  error = error ? error : (error_index ? error_index : close_error_index);
  return error;
}

int Consistent_archive::set_purge_index_file_name(const char *base_file_name) {
  int error = 0;
  DBUG_TRACE;
  if (fn_format(m_purge_index_file_name, base_file_name,
                m_mysql_archive_data_dir, ".~rec~",
                MYF(MY_UNPACK_FILENAME | MY_SAFE_PATH | MY_REPLACE_EXT)) ==
      nullptr) {
    error = 1;
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "failed to set purge index");
  }
  return error;
}

int Consistent_archive::open_purge_index_file(bool destroy) {
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
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
             "failed to open purge index");
    }
  }
  return error;
}

int Consistent_archive::close_purge_index_file() {
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

bool Consistent_archive::purge_index_file_is_inited() {
  DBUG_TRACE;
  return my_b_inited(&m_purge_index_file);
}

int Consistent_archive::sync_purge_index_file() {
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

int Consistent_archive::register_purge_index_entry(const char *entry) {
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
 * @brief Delete the archive file from the object store.
 *
 * @param decrease_log_space
 * @return int
 */
int Consistent_archive::purge_index_entry(ulonglong *decrease_log_space) {
  DBUG_TRACE;
  MY_STAT s;
  int error = 0;
  LOG_INFO log_info;
  LOG_INFO check_log_info;

  assert(my_b_inited(&m_purge_index_file));

  if ((error =
           reinit_io_cache(&m_purge_index_file, READ_CACHE, 0, false, false))) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "failed to reinit_io_cache");
    goto err;
  }

  for (;;) {
    size_t length = 0;

    if ((length = my_b_gets(&m_purge_index_file, log_info.log_file_name,
                            FN_REFLEN)) <= 1) {
      if (m_purge_index_file.error) {
        error = m_purge_index_file.error;
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, error);
        goto err;
      }

      /* Reached EOF */
      break;
    }

    /* Get rid of the trailing '\n' */
    log_info.log_file_name[length - 1] = 0;

    // delete the archived se_backup.tar or innodb.tar file from the object
    // store.
    if (snapshot_objstore) {
      std::string archive_keyid;
      archive_keyid.assign(log_info.log_file_name);
      archive_keyid.append(CONSISTENT_TAR_SUFFIX);

      objstore::Status ss = snapshot_objstore->delete_object(
          std::string_view(opt_objstore_bucket), archive_keyid);
      if (!ss.is_succ()) {
        std::string err_msg;
        err_msg.assign("Failed to delet archive file from object store: ");
        err_msg.append(archive_keyid);
        err_msg.append(" error=");
        err_msg.append(ss.error_message());
        LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
        error = 1;
        goto err;
      }
      // TODO: should check error code, if failed because of NOT_FOUND, should
      // report WARNING and continue to delete next binlog file.
    } else {
      // delete the archived file from the local archive directory.
      if (!mysql_file_stat(PSI_archive_file_key, log_info.log_file_name, &s,
                           MYF(0))) {
        if (my_errno() == ENOENT) {
          /*
            It's not fatal if we can't stat a log file that does not exist;
            If we could not stat, we won't delete.
          */
          LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
                 log_info.log_file_name);
          set_my_errno(0);
        } else {
          /*
            Other than ENOENT are fatal
          */
          LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
                 log_info.log_file_name);
          error = LOG_INFO_FATAL;
          goto err;
        }
      } else {
        DBUG_PRINT("info", ("purging %s", log_info.log_file_name));
        if (!mysql_file_delete(PSI_archive_file_key, log_info.log_file_name,
                               MYF(0))) {
          if (decrease_log_space) *decrease_log_space -= s.st_size;
        } else {
          if (my_errno() == ENOENT) {
            LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
                   log_info.log_file_name);
            set_my_errno(0);
          } else {
            LogErr(INFORMATION_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
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
 * @brief Remove line entry from index file.
 *
 * @param log_info
 * @return int
 */
int Consistent_archive::remove_line_from_index(LOG_INFO *log_info,
                                               Archive_type arch_type) {
  DBUG_TRACE;
  IO_CACHE *crash_safe_index_file;
  IO_CACHE *index_file;
  std::string index_file_name;
  std::string index_keyid;

  if (arch_type == ARCHIVE_MYSQL_INNODB) {
    crash_safe_index_file = &m_crash_safe_mysql_clone_index_file;
    index_file = &m_mysql_clone_index_file;
    index_file_name.assign(m_mysql_clone_index_file_name);
    index_keyid.assign(CONSISTENT_INNODB_ARCHIVE_INDEX_FILE);
  } else if (arch_type == ARCHIVE_SE) {
    crash_safe_index_file = &m_crash_safe_se_backup_index_file;
    index_file = &m_se_backup_index_file;
    index_file_name.assign(m_se_backup_index_file_name);
    index_keyid.assign(CONSISTENT_SE_ARCHIVE_INDEX_FILE);
  } else {
    crash_safe_index_file = &m_crash_safe_consistent_snapshot_index_file;
    index_file = &m_consistent_snapshot_index_file;
    index_file_name.assign(m_consistent_snapshot_index_file_name);
    index_keyid.assign(CONSISTENT_SNAPSHOT_INDEX_FILE);
  }

  if (!my_b_inited(index_file)) {
    goto err;
  }

  if (open_crash_safe_index_file(arch_type)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "failed to open_crash_safe_index_file in remove_line_from_index.");
    goto err;
  }

  if (copy_file(index_file, crash_safe_index_file,
                log_info->index_file_start_offset)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "failed to copy_file in remove_line_from_index.");
    goto err;
  }

  if (close_crash_safe_index_file(arch_type)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "failed to close_crash_safe_index_file.");
    goto err;
  }

  // First, update the local index file, then upload it to the object store. If
  // the local update fails, this purge of binlogs will also fail. If the local
  // update succeeds but the upload fails, it will result in inconsistency
  // between the local and object store indexes. Therefore, forcibly close the
  // index file to allow the archive thread to download the index from the
  // object store again and reopen it.
  if (move_crash_safe_index_file_to_index_file(arch_type)) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG,
           "failed to move_crash_safe_index_file_to_index_file.");
    goto err;
  }

  if (snapshot_objstore != nullptr) {
    // refresh index to object store
    objstore::Status ss = snapshot_objstore->put_object_from_file(
        std::string_view(opt_objstore_bucket), index_keyid, index_file_name);
    if (!ss.is_succ()) {
      std::string err_msg;
      err_msg.assign("Failed to upload index file to object store: ");
      err_msg.append(index_keyid);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
      // forcibly close the index file to allow the archive thread to download
      // the index from the object store again and reopen it.
      close_index_file(arch_type);
      goto err;
    }
  }
  return 0;

err:
  return LOG_INFO_IO;
}

int Consistent_archive::show_innodb_persistent_files(
    std::vector<objstore::ObjectMeta> &objects) {
  DBUG_TRACE;
  int error = 0;
  if (snapshot_objstore != nullptr) {
    objstore::Status ss = snapshot_objstore->list_object(
        std::string_view(opt_objstore_bucket),
        std::string_view(CONSISTENT_INNODB_ARCHIVE_BASENAME), objects);
    if (!ss.is_succ() && ss.error_code() != objstore::Errors::SE_NO_SUCH_KEY) {
      std::string err_msg;
      error = 1;
      err_msg.assign("Failed to innodb files: ");
      err_msg.append(CONSISTENT_INNODB_ARCHIVE_BASENAME);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    }
  }
  return error;
}

int Consistent_archive::show_se_persistent_files(
    std::vector<objstore::ObjectMeta> &objects) {
  DBUG_TRACE;
  int error = 0;
  if (snapshot_objstore != nullptr) {
    objstore::Status ss = snapshot_objstore->list_object(
        std::string_view(opt_objstore_bucket),
        std::string_view(CONSISTENT_SE_ARCHIVE_BASENAME), objects);
    if (!ss.is_succ() && ss.error_code() != objstore::Errors::SE_NO_SUCH_KEY) {
      std::string err_msg;
      error = 1;
      err_msg.assign("Failed to smartengine files: ");
      err_msg.append(CONSISTENT_SE_ARCHIVE_BASENAME);
      err_msg.append(" error=");
      err_msg.append(ss.error_message());
      LogErr(ERROR_LEVEL, ER_CONSISTENT_SNAPSHOT_LOG, err_msg.c_str());
    }
  }
  return error;
}

/**
 * @brief Remove file or directory.
 * @param file
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
  DBUG_TRACE;
  int bytes_read;
  uchar io_buf[IO_SIZE * 2];

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

static bool file_has_suffix(const std::string &sfx, const std::string &path) {
  DBUG_TRACE;
  return (path.size() >= sfx.size() &&
          path.compare(path.size() - sfx.size(), sfx.size(), sfx) == 0);
}
