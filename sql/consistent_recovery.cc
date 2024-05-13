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

#include "sql/consistent_recovery.h"

#include <fstream>
#include <sstream>
#include <string>
#include "my_io.h"

#include "mysql/plugin.h"
#include "sql/binlog.h"
#include "sql/binlog_archive.h"
#include "sql/consistent_archive.h"
#include "sql/debug_sync.h"
#include "sql/derror.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/log.h"
#include "sql/mysqld.h"
#include "sql/rpl_replica.h"
#include "sql/sql_plugin.h"
#include "sql/sql_show.h"
#include "sql/sys_vars_shared.h"

Consistent_recovery consistent_recovery;
static int copy_directory(const std::string &from, const std::string &to);
static bool remove_file(const std::string &file);
static void root_directory(std::string in, std::string *out, std::string *left);
static void recursive_create_dir(const std::string &dir,
                                 const std::string &root);

Consistent_recovery::Consistent_recovery()
    : m_state(CONSISTENT_RECOVERY_STATE_NONE),
      recovery_objstore(nullptr),
      m_binlog_pos(0),
      is_recovery_binlog(false),
      m_mysql_clone_index_file(),
      m_se_backup_index_file(),
      m_se_backup_index(0),
      binlog_index_file() {
  m_binlog_file[0] = '\0';
  m_binlog_relative_file[0] = '\0';
  m_mysql_archive_recovery_dir[0] = '\0';
  m_mysql_archive_recovery_data_dir[0] = '\0';
  m_mysql_archive_recovery_binlog_dir[0] = '\0';
  m_mysql_innodb_clone_dir[0] = '\0';
  m_mysql_clone_name[0] = '\0';
  m_mysql_clone_index_file_name[0] = '\0';
  m_se_backup_index_file_name[0] = '\0';
  m_se_backup_dir[0] = '\0';
  m_se_backup_name[0] = '\0';
  binlog_index_file_name[0] = '\0';
}

/**
 * @brief Recovery database from consistent snapshot.
 * @note only support data source from object store.
 * @return int
 */
int Consistent_recovery::recovery_consistent_snapshot(int flags) {
  // database recovery from consistent snapshot.
  if (opt_initialize_use_objstore || opt_persistent_on_objstore) {
    std::string_view endpoint(
        opt_objstore_endpoint ? std::string_view(opt_objstore_endpoint) : "");
    recovery_objstore = objstore::create_object_store(
        std::string_view(opt_objstore_provider),
        std::string_view(opt_objstore_region),
        opt_objstore_endpoint ? &endpoint : nullptr, opt_objstore_use_https);
    if (recovery_objstore == nullptr) {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
             "Failed to create_object_store");
      return 1;
    }
  } else {
    return 0;
  }
  if (opt_initialize_use_objstore) {
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "initialize data source "
           "from object store");
  } else {
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "Recovery consistent snapshot from object store");
  }

  MY_STAT stat;
  if (opt_mysql_archive_recovery_tmpdir) {
    strmake(m_mysql_archive_recovery_dir, opt_mysql_archive_recovery_tmpdir,
            sizeof(m_mysql_archive_recovery_dir) - 1);
    convert_dirname(m_mysql_archive_recovery_dir, m_mysql_archive_recovery_dir,
                    NullS);
    if (!my_stat(m_mysql_archive_recovery_dir, &stat, MYF(0)) ||
        !MY_S_ISDIR(stat.st_mode) ||
        my_access(m_mysql_archive_recovery_dir, (F_OK | W_OK))) {
      std::string err_msg;
      err_msg.assign("mysql archive recovery path not exist: ");
      err_msg.append(m_mysql_archive_recovery_dir);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return 1;
    }
  } else {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "Consistent recovery must set --mysql-archive-recovery-tmpdir");
    return 1;
  }

  // create new recovery data dir.
  strmake(
      strmake(m_mysql_archive_recovery_data_dir, m_mysql_archive_recovery_dir,
              sizeof(m_mysql_archive_recovery_data_dir) -
                  CONSISTENT_ARCHIVE_SUBDIR_LEN - 1),
      STRING_WITH_LEN(CONSISTENT_ARCHIVE_SUBDIR));

  convert_dirname(m_mysql_archive_recovery_data_dir,
                  m_mysql_archive_recovery_data_dir, NullS);
  remove_file(m_mysql_archive_recovery_data_dir);
  if (my_mkdir(m_mysql_archive_recovery_data_dir, 0777, MYF(0))) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "Failed to create archive recovery data dir.");
    return 1;
  }
  // create new recovery binlog dir.
  strmake(
      strmake(m_mysql_archive_recovery_binlog_dir, m_mysql_archive_recovery_dir,
              sizeof(m_mysql_archive_recovery_binlog_dir) -
                  BINLOG_ARCHIVE_SUBDIR_LEN - 1),
      STRING_WITH_LEN(BINLOG_ARCHIVE_SUBDIR));
  convert_dirname(m_mysql_archive_recovery_binlog_dir,
                  m_mysql_archive_recovery_binlog_dir, NullS);
  remove_file(m_mysql_archive_recovery_binlog_dir);
  if (my_mkdir(m_mysql_archive_recovery_binlog_dir, 0777, MYF(0))) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "Failed to create archive recovery binlog dir.");
    return 1;
  }

  convert_dirname(mysql_real_data_home, mysql_real_data_home, NullS);

  m_state = CONSISTENT_RECOVERY_STATE_NONE;
  // read consistent snapshot file from object store.
  if (read_consistent_snapshot_file()) return 1;
  // recovery mysql innodb to mysql data dir
  if (flags & CONSISTENT_RECOVERY_INNODB)
    if (recovery_mysql_innodb()) return 1;
  // recovery smartengine to smartengine data dir
  if (flags & CONSISTENT_RECOVERY_SMARTENGINE)
    if (recovery_smartengine()) return 1;
  // recovery binlog to mysql
  if (flags & CONSISTENT_RECOVERY_BINLOG)
    if (recovery_binlog(opt_binlog_index_name, nullptr)) return 1;
  return 0;
}

/**
 * @brief   Read consistent snapshot file to archive dir from object store.
 *
 * @return true
 * @return false
 */
bool Consistent_recovery::read_consistent_snapshot_file() {
  if (m_state != CONSISTENT_RECOVERY_STATE_NONE) return true;

  std::string consistent_file_name;
  consistent_file_name.assign(m_mysql_archive_recovery_dir);
  consistent_file_name.append(CONSISTENT_SNAPSHOT_FILE);
  // Delete the local consistent snapshot file if it exists.
  remove_file(consistent_file_name);
  // download #consistent_snapshot from object store
  // If failed, return false, indicate not exist consistent snapshot in object
  // store.
  objstore::Status ss = recovery_objstore->get_object_to_file(
      std::string_view(opt_objstore_bucket),
      std::string_view(CONSISTENT_SNAPSHOT_FILE), consistent_file_name);
  if (!ss.is_succ()) {
    std::string err_msg;
    err_msg.assign("Consistent snapshot not exists in object store: ");
    err_msg.append(" key=");
    err_msg.append(CONSISTENT_SNAPSHOT_FILE);
    err_msg.append(" file=");
    err_msg.append(consistent_file_name);
    err_msg.append(" error=");
    err_msg.append(ss.error_message());
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    return true;
  }

  if (my_chmod(consistent_file_name.c_str(),
               USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE |
                   OTHERS_WRITE | OTHERS_READ,
               MYF(MY_WME))) {
    std::string err_msg;
    err_msg.assign("Failed to chmod: ");
    err_msg.append(consistent_file_name);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    // remove_file(mysql_clone_file_name);
    return true;
  }

  std::ifstream consistent_file;
  consistent_file.open(consistent_file_name, std::ifstream::in);
  if (!consistent_file.is_open()) {
    std::string err_msg;
    err_msg.assign("Failed to open consistent snapshot file: ");
    err_msg.append(consistent_file_name);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
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
        strncpy(m_mysql_clone_name, file_line.c_str(),
                sizeof(m_mysql_clone_name) - 1);
        break;
      case 2:
        strncpy(m_se_backup_name, file_line.c_str(),
                sizeof(m_se_backup_name) - 1);
        break;
      case 3:
        strncpy(m_binlog_file, file_line.c_str(), sizeof(m_binlog_file) - 1);
        break;
      case 4:
        file_data >> m_binlog_pos;
        break;
      default:
        LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
               "Invalid consistent snapshot file");
        break;
    }
  }
  consistent_file.close();
  // Keep consistent snapshot file for diagnosis, until the next recovery.
  // remove_file(consistent_file_name);
  m_state = CONSISTENT_RECOVERY_STATE_SNAPSHOT_FILE;
  return false;
}

/**
 * @brief Read m_mysql_clone_name to temp archive dir from object store.
 *        Then cp archived mysql innodb file to mysql data dir.
 *
 * @return true
 * @return false
 */
bool Consistent_recovery::recovery_mysql_innodb() {
  // no recovery
  if (m_state == CONSISTENT_RECOVERY_STATE_NONE) return false;
  {
    std::string err_msg;
    err_msg.assign("Recovery mysql innodb ");
    err_msg.append(m_mysql_clone_name);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
  }
  std::string mysql_clone_file_name;
  mysql_clone_file_name.assign(m_mysql_archive_recovery_data_dir);
  mysql_clone_file_name.append(m_mysql_clone_name);
  mysql_clone_file_name.append(CONSISTENT_TAR_SUFFIX);
  std::string mysql_clone_keyid;
  mysql_clone_keyid.assign(m_mysql_clone_name);
  mysql_clone_keyid.append(CONSISTENT_TAR_SUFFIX);
  // delete the local expired data if it exists.
  remove_file(mysql_clone_file_name);
  // download m_mysql_clone_name from object store
  objstore::Status ss = recovery_objstore->get_object_to_file(
      std::string_view(opt_objstore_bucket), mysql_clone_keyid,
      mysql_clone_file_name);
  if (!ss.is_succ()) {
    std::string err_msg;
    err_msg.assign("Failed to download mysql innodb clone file: ");
    err_msg.append(" key=");
    err_msg.append(mysql_clone_keyid);
    err_msg.append(" file=");
    err_msg.append(mysql_clone_file_name);
    err_msg.append(" error=");
    err_msg.append(ss.error_message());
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    return true;
  }

  if (my_chmod(mysql_clone_file_name.c_str(),
               USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE |
                   OTHERS_WRITE | OTHERS_READ,
               MYF(MY_WME))) {
    std::string err_msg;
    err_msg.assign("Failed to chmod: ");
    err_msg.append(mysql_clone_file_name);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    // remove_file(mysql_clone_file_name);
    return true;
  }

  // tar -xvzf /u01/mysql_archive/clone000034.tar
  std::stringstream cmd;
  std::string clone_untar_name;
  clone_untar_name.assign(m_mysql_archive_recovery_data_dir);
  clone_untar_name.append(m_mysql_clone_name);
  // Delete local expired data if it exists.
  remove_file(clone_untar_name);
  cmd << "tar -xzf " << mysql_clone_file_name.c_str() << " -C "
      << m_mysql_archive_recovery_data_dir;
  if (system(cmd.str().c_str())) {
    std::string err_msg;
    err_msg.assign("Failed to untar mysql innodb clone file: ");
    err_msg.append(mysql_clone_file_name);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    // mysql_archive/clone000034.tar
    remove_file(mysql_clone_file_name);
    return true;
  }

  // cp -r /u01/mysql_archive/clone000034 mysql_real_data_home
  // scan the clone dir and copy the files and sub dir to mysql data dir
  if (copy_directory(clone_untar_name, mysql_real_data_home)) {
    std::string err_msg;
    err_msg.assign("Failed to copy mysql innodb file: ");
    err_msg.append(clone_untar_name);
    err_msg.append(" to ");
    err_msg.append(mysql_real_data_home);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    // remove mysql_archive/clone000034
    remove_file(clone_untar_name);
    // mysql_archive/clone000034.tar
    remove_file(mysql_clone_file_name);
    return true;
  }

  // remove mysql_archive/clone000034
  remove_file(clone_untar_name);
  // mysql_archive/clone000034.tar
  remove_file(mysql_clone_file_name);
  m_state = CONSISTENT_RECOVERY_STATE_MYSQL_INNODB;

  return false;
}

// read m_se_backup_name to temp archive dir from object store.
// smartengine plugin is loaded.
bool Consistent_recovery::recovery_smartengine() {
  // no recovery process
  if (m_state == CONSISTENT_RECOVERY_STATE_NONE) return false;

  /* Check if the smartengine plugin is loaded. */
  LEX_CSTRING engine_name = {STRING_WITH_LEN("smartengine")};
  plugin_ref tmp_plugin = ha_resolve_by_name_raw(nullptr, engine_name);
  if (tmp_plugin == nullptr) {
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "smartengine plugin is not loaded");
    return true;
  }
  plugin_unlock(nullptr, tmp_plugin);

  {
    std::string err_msg;
    err_msg.assign("Recovery smartengine ");
    err_msg.append(m_se_backup_name);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
  }

  std::string se_file_name;
  se_file_name.assign(m_mysql_archive_recovery_data_dir);
  se_file_name.append(m_se_backup_name);
  se_file_name.append(CONSISTENT_TAR_SUFFIX);
  std::string se_backup_keyid;
  se_backup_keyid.assign(m_se_backup_name);
  se_backup_keyid.append(CONSISTENT_TAR_SUFFIX);
  // delete the local expired data if it exists.
  remove_file(se_file_name);
  // download m_se_backup_name from object store
  objstore::Status ss = recovery_objstore->get_object_to_file(
      std::string_view(opt_objstore_bucket), se_backup_keyid, se_file_name);
  if (!ss.is_succ()) {
    std::string err_msg;
    err_msg.assign("Failed to download smartengine backup file: ");
    err_msg.append(" key=");
    err_msg.append(se_backup_keyid);
    err_msg.append(" file=");
    err_msg.append(se_file_name);
    err_msg.append(" error=");
    err_msg.append(ss.error_message());
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    return true;
  }

  if (my_chmod(se_file_name.c_str(),
               USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE |
                   OTHERS_WRITE | OTHERS_READ,
               MYF(MY_WME))) {
    std::string err_msg;
    err_msg.assign("Failed to chmod: ");
    err_msg.append(se_file_name);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    // remove_file(mysql_clone_file_name);
    return true;
  }

  // tar -xvzf /u01/mysql_archive/se_backup000034.tar
  std::stringstream cmd;
  std::string se_backup_untar_name;
  se_backup_untar_name.assign(m_mysql_archive_recovery_data_dir);
  se_backup_untar_name.append(m_se_backup_name);
  // Delete local expired data if it exists.
  remove_file(se_backup_untar_name);
  cmd << "tar -xzf " << se_file_name << " -C "
      << m_mysql_archive_recovery_data_dir;
  if (system(cmd.str().c_str())) {
    std::string err_msg;
    err_msg.assign("Failed to untar smartengine backup file: ");
    err_msg.append(se_file_name);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    // mysql_archive/se_backup000034.tar
    remove_file(se_file_name);
    return true;
  }

  // Retrive smartengine option --smartengine-datadir
  String se_data_dir{};
  auto f = [&se_data_dir](const System_variable_tracker &, sys_var *var) {
    char val_buf[1024];
    SHOW_VAR show;
    show.type = SHOW_SYS;
    show.value = pointer_cast<char *>(var);
    show.name = var->name.str;

    mysql_mutex_lock(&LOCK_global_system_variables);
    size_t val_length;
    const char *value =
        get_one_variable(nullptr, &show, OPT_GLOBAL, SHOW_SYS, nullptr, nullptr,
                         val_buf, &val_length);
    if (val_length != 0) {
      se_data_dir.copy(value, val_length, nullptr);
    }
    mysql_mutex_unlock(&LOCK_global_system_variables);
  };

  System_variable_tracker sv =
      System_variable_tracker::make_tracker("SMARTENGINE_DATADIR");
  mysql_mutex_lock(&LOCK_plugin);
  if (sv.access_system_variable(nullptr, f)) {
    mysql_mutex_unlock(&LOCK_plugin);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
           "Failed to get smartengine_data_dir parameter");
    return true;
  }
  mysql_mutex_unlock(&LOCK_plugin);

  std::string smartengine_real_data_home;
  smartengine_real_data_home.assign(se_data_dir.c_ptr());
  // cp -r /u01/mysql_archive/se_backup000034 SMARTENGINE_DATADIR
  // scan the se backup dir and copy the files and sub dir to smartengine data
  // dir
  if (copy_directory(se_backup_untar_name, smartengine_real_data_home)) {
    std::string err_msg;
    err_msg.assign("Failed to copy smartengine file: ");
    err_msg.append(se_backup_untar_name);
    err_msg.append(" to ");
    err_msg.append(smartengine_real_data_home);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    // remove mysql_archive/se_backup000034
    remove_file(se_backup_untar_name);
    // mysql_archive/se_backup000034.tar
    remove_file(se_file_name);
    return true;
  }

  // remove mysql_archive/se_backup000034
  remove_file(se_backup_untar_name);
  // mysql_archive/se_backup000034.tar
  remove_file(se_file_name);
  m_state = CONSISTENT_RECOVERY_STATE_SE;
  return false;
}

/**
 * @brief Recovery binlog and index file from object store.
 * @note If opt_bin_log is not set, recover_binlog() will be not called.
 * @return true
 * @return false
 */
bool Consistent_recovery::recovery_binlog(const char *index_file_name_arg,
                                    const char *log_name) {
  // if not need recovery binlog, return false
  if (m_state == CONSISTENT_RECOVERY_STATE_NONE) return false;

  {
    std::string err_msg;
    err_msg.assign("Recovery binlog ");
    err_msg.append(m_binlog_file);
    LogErr(SYSTEM_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
  }
  // If binlog file is empty, indicate that binlog is not required to be
  // recovered. m_binlog_file is from consistent snapshot.
  if (m_binlog_file[0] == '\0') {
    m_state = CONSISTENT_RECOVERY_STATE_BINLOG;
    return false;
  }

  // TODO:Check if binlog file is exist in objstore's mysql-bin.index

  // Download m_binlog_file to archive dir from object store.
  // If binlog file is absolute path, change absolute path to relative path.
  if (m_binlog_file[0] == FN_LIBCHAR) {
    strmake(m_binlog_relative_file, m_binlog_file + 1,
            sizeof(m_binlog_relative_file) - 1);
  } else {
    strmake(m_binlog_relative_file, m_binlog_file,
            sizeof(m_binlog_relative_file) - 1);
  }

  // recursive create binlog local archive dir if not exist under
  // mysql_archive_dir.
  recursive_create_dir(m_binlog_relative_file,
                       m_mysql_archive_recovery_binlog_dir);

  std::string binlog_name;
  std::string binlog_keyid;
  binlog_name.assign(m_mysql_archive_recovery_binlog_dir);
  binlog_name.append(m_binlog_relative_file);
  binlog_keyid.assign(BINLOG_ARCHIVE_SUBDIR);
  binlog_keyid.append(FN_DIRSEP);
  binlog_keyid.append(m_binlog_relative_file);
  // Delete the local binlog file if it exists.
  remove_file(binlog_name);
  // download m_binlog_file from object store
  objstore::Status ss = recovery_objstore->get_object_to_file(
      std::string_view(opt_objstore_bucket), binlog_keyid, binlog_name);
  if (!ss.is_succ()) {
    std::string err_msg;
    err_msg.assign("Failed to download binlog file: ");
    err_msg.append(" key=");
    err_msg.append(binlog_keyid);
    err_msg.append(" file=");
    err_msg.append(binlog_name);
    err_msg.append(" error=");
    err_msg.append(ss.error_message());
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    return true;
  }

  if (my_chmod(binlog_name.c_str(),
               USER_READ | USER_WRITE | GROUP_READ | GROUP_WRITE |
                   OTHERS_WRITE | OTHERS_READ,
               MYF(MY_WME))) {
    std::string err_msg;
    err_msg.assign("Failed to chmod: ");
    err_msg.append(binlog_name);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    // remove_file(mysql_clone_file_name);
    return true;
  }

  // cp -r $m_mysql_archive_recovery_binlog_dir/file
  // $mysql_real_data_home/binlog
  std::string binlog_archive_dir;
  binlog_archive_dir.assign(m_mysql_archive_recovery_binlog_dir);

  std::string binlog_absolute_dir;
  char binlog_dir[FN_REFLEN + 1] = {0};
  size_t binlog_dir_len;
  convert_dirname(mysql_real_data_home, mysql_real_data_home, NullS);
  dirname_part(binlog_dir, m_binlog_file, &binlog_dir_len);
  if (m_binlog_file[0] != FN_LIBCHAR) {
    binlog_absolute_dir.assign(mysql_real_data_home);
  } else {
    binlog_absolute_dir.assign(FN_DIRSEP);
  }

  if (copy_directory(binlog_archive_dir, binlog_absolute_dir)) {
    std::string err_msg;
    err_msg.assign("Failed to copy binlog file: ");
    err_msg.append(binlog_archive_dir);
    err_msg.append(" to ");
    err_msg.append(binlog_absolute_dir);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    remove_file(binlog_archive_dir);
    return true;
  }

  // recovery mysql binlog index file
  myf opt = MY_UNPACK_FILENAME;
  char index_file_name[FN_REFLEN];
  if (!index_file_name_arg) {
    index_file_name_arg = log_name;  // Use same basename for index file
    opt = MY_UNPACK_FILENAME | MY_REPLACE_EXT;
  }
  fn_format(index_file_name, index_file_name_arg, mysql_data_home, ".index",
            opt);
  std::string binlog_index_real_file;
  if (index_file_name[0] != FN_LIBCHAR) {
    binlog_index_real_file.assign(mysql_real_data_home);
    binlog_index_real_file.append(index_file_name);
  } else {
    binlog_index_real_file.assign(index_file_name);
  }

  strmake(binlog_index_file_name, binlog_index_real_file.c_str(),
          sizeof(binlog_index_file_name) - 1);
  if (create_binlog_index_file(binlog_index_file_name)) {
    std::string err_msg;
    err_msg.assign("Failed to create binlog index file: ");
    err_msg.append(binlog_index_file_name);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    remove_file(binlog_archive_dir);
    return true;
  }
  if (add_line_to_index_file((uchar *)m_binlog_file, strlen(m_binlog_file))) {
    std::string err_msg;
    err_msg.assign("Failed to write binlog name to binlog index file: ");
    err_msg.append(binlog_index_file_name);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    close_binlog_index_file();
    remove_file(binlog_archive_dir);
    return true;
  }
  close_binlog_index_file();
  remove_file(binlog_archive_dir);
  // indicate that binlog is recovered from consistent snapshot.
  is_recovery_binlog = true;
  m_state = CONSISTENT_RECOVERY_STATE_BINLOG;
  return false;
}

/**
 * @brief Check if binlog recovery path validation.
 *
 */
bool Consistent_recovery::binlog_recovery_path_is_validation() {
  // if not need recovery binlog, return false
  if (m_state == CONSISTENT_RECOVERY_STATE_NONE) return false;
  // After recovery binlog.
  if (m_state != CONSISTENT_RECOVERY_STATE_BINLOG) return true;
  // No need to verify if binlog file is empty.
  if (m_binlog_file[0] == '\0') {
    return false;
  }
  assert(is_recovery_binlog);

  return false;
}

bool Consistent_recovery::recovery_consistent_snapshot_finish() {
  if (m_state == CONSISTENT_RECOVERY_STATE_NONE) return false;
  m_state = CONSISTENT_RECOVERY_STATE_END;
  return false;
}

/**
 * @brief Need recovery binlog, if binlog file from consistent snapshot is not
 * empty.
 *
 * @return true
 * @return false
 */
bool Consistent_recovery::need_recovery_binlog() { return is_recovery_binlog; }

/**
 * @brief Construct a new binlog.index file.
 *
 * @param index_file_name
 * @return int
 */
int Consistent_recovery::create_binlog_index_file(const char *index_file_name) {
  int error = false;
  File index_file_nr = -1;
  myf flags = MY_WME | MY_NABP | MY_WAIT_IF_FULL;

  if ((index_file_nr =
           my_open(index_file_name, O_RDWR | O_CREAT, MYF(MY_WME))) < 0 ||
      init_io_cache(&binlog_index_file, index_file_nr, IO_SIZE, WRITE_CACHE, 0,
                    false, flags)) {
    if (index_file_nr >= 0) my_close(index_file_nr, MYF(0));
    error = 1;
    goto end;
  }

end:
  return error;
}

int Consistent_recovery::add_line_to_index_file(uchar *log_name,
                                                size_t log_name_len) {
  if (my_b_write(&binlog_index_file, log_name, log_name_len) ||
      my_b_write(&binlog_index_file, pointer_cast<const uchar *>("\n"), 1) ||
      flush_io_cache(&binlog_index_file) ||
      mysql_file_sync(binlog_index_file.file, MYF(MY_WME))) {
    goto err;
  }
  return 0;
err:
  return 1;
}

int Consistent_recovery::close_binlog_index_file() {
  int error = 0;

  DBUG_TRACE;

  if (my_b_inited(&binlog_index_file)) {
    end_io_cache(&binlog_index_file);
    error = my_close(binlog_index_file.file, MYF(0));
  }

  return error;
}

static int copy_directory(const std::string &from, const std::string &to) {
  uint i;
  // Iterate in data directory and delete all .SDI files
  MY_DIR *a, *b;
  char from_path[FN_REFLEN + 1] = {0};
  char to_path[FN_REFLEN + 1] = {0};

  strmake(from_path, from.c_str(), sizeof(from_path) - 1);
  convert_dirname(from_path, from_path, NullS);
  strmake(to_path, to.c_str(), sizeof(to_path) - 1);
  convert_dirname(to_path, to_path, NullS);

  // Check if the source directory exists.
  if (!(a = my_dir(from_path, MYF(MY_DONT_SORT | MY_WANT_STAT)))) {
    std::string err_msg;
    err_msg.assign("Failed to open directory: ");
    err_msg.append(from_path);
    LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
    return 1;
  }

  // If the destination directory does not exist, create it.
  if (!(b = my_dir(to_path, MYF(MY_DONT_SORT | MY_WANT_STAT)))) {
    if (my_mkdir(to_path, 0777, MYF(0)) < 0) {
      std::string err_msg;
      err_msg.assign("Failed to create directory: ");
      err_msg.append(to_path);
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG, err_msg.c_str());
      return 1;
    }
  } else {
    my_dirend(b);
  }

  // Iterate through the source directory.
  for (i = 0; i < (uint)a->number_off_files; i++) {
    // Skip . and ..
    if (strcmp(a->dir_entry[i].name, ".") == 0 ||
        strcmp(a->dir_entry[i].name, "..") == 0) {
      continue;
    }
    std::string tmp_from;
    std::string tmp_to;
    tmp_from.assign(from_path);
    tmp_from.append(a->dir_entry[i].name);
    tmp_to.assign(to_path);
    tmp_to.append(a->dir_entry[i].name);
    // If its a folder, iterate it. Otherwise, copy the file.
    if (MY_S_ISDIR(a->dir_entry[i].mystat->st_mode)) {
      if (copy_directory(tmp_from, tmp_to)) {
        my_dirend(a);
        return 1;
      }
    } else {
      my_copy(tmp_from.c_str(), tmp_to.c_str(), MYF(0));
    }
  }
  my_dirend(a);
  return 0;
}

/**
 * @brief Remove file or directory.
 * @note If file is directory, remove all files and sub directories.
 *       If file or directory is not exist, return false.
 * @param file
 */
static bool remove_file(const std::string &file) {
  uint i;
  MY_DIR *dir;
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
 * @param out_str output root directory from in_str. or empty string.
 * @param left_str output left string from in_str. or in_str.
 */
static void root_directory(std::string in_str, std::string *out_str,
                           std::string *left_str) {
  // size_t idx = in.rfind(FN_DIRSEP);
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
 * @brief Create directory recursively.
 *
 * @param dir
 * @param root
 */
static void recursive_create_dir(const std::string &dir,
                                 const std::string &root) {
  std::string out;
  std::string left;
  root_directory(dir, &out, &left);
  if (out.empty()) {
    return;
  }
  std::string real_path;

  real_path.assign(root);
  real_path.append(out);
  MY_STAT stat_area;
  if (!my_stat(real_path.c_str(), &stat_area, MYF(0))) {
    if (my_mkdir(real_path.c_str(), 0777, MYF(0))) {
      LogErr(ERROR_LEVEL, ER_CONSISTENT_RECOVERY_LOG,
             "Failed to create binlog archive dir.");
    }
  }
  recursive_create_dir(left, real_path);
}
