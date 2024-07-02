/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxyEngine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxyEngine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <signal.h>

#include "consensus_applier.h"
#include "consensus_binlog.h"
#include "consensus_binlog_recovery.h"
#include "consensus_log_manager.h"
#include "consensus_recovery_manager.h"
#include "plugin.h"
#include "plugin_psi.h"
#include "system_variables.h"

#include "mysql/psi/mysql_file.h"

#include "mysql/thread_pool_priv.h"
#include "sql/consensus/consensus_info_factory.h"
#include "sql/events.h"
#include "sql/log.h"
#include "sql/log_event.h"
#include "sql/rpl_info_factory.h"
#include "sql/rpl_mi.h"
#include "sql/rpl_msr.h"
#include "sql/rpl_replica.h"
#include "sql/sql_parse.h"
#include "sql/sql_thd_internal_api.h"
#include "sql/xa.h"

using binary_log::checksum_crc32;
using std::max;

#include "rpl_consensus.h"

#define LOG_PREFIX "ML"

static std::atomic<bool> consensus_recovery_aborted = false;

ConsensusLogManager consensus_log_manager;

uint64 show_fifo_cache_size(THD *, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  long *value = reinterpret_cast<long *>(buff);
  uint64 size = !is_consensus_replication_enabled()
                    ? 0
                    : consensus_log_manager.get_fifo_cache_manager()
                          ->get_fifo_cache_size();
  *value = static_cast<long long>(size);
  return 0;
}

uint64 show_first_index_in_fifo_cache(THD *, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  long *value = reinterpret_cast<long *>(buff);
  uint64 size = !is_consensus_replication_enabled()
                    ? 0
                    : consensus_log_manager.get_fifo_cache_manager()
                          ->get_first_index_of_fifo_cache();
  *value = static_cast<long long>(size);
  return 0;
}

uint64 show_log_count_in_fifo_cache(THD *, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  long *value = reinterpret_cast<long *>(buff);
  uint64 size = !is_consensus_replication_enabled()
                    ? 0
                    : consensus_log_manager.get_fifo_cache_manager()
                          ->get_fifo_cache_log_count();
  *value = static_cast<long long>(size);
  return 0;
}

uint64 show_easy_pool_alloc_byte(THD *, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  long *value = reinterpret_cast<long *>(buff);
  uint64 size =
      !is_consensus_replication_enabled() ? 0 : rpl_consensus_get_easy_alloc_byte();
  *value = static_cast<long long>(size);
  return 0;
}


void stateChangeCb(StateType state, uint64_t term, uint64_t commitIndex) {
  ConsensusStateChange state_change = {state, term, commitIndex};
  consensus_log_manager.add_state_change_request(state_change);
}

uint32 binlog_checksum_crc32_callback(uint32 crc, const unsigned char *pos,
                                      size_t length) {
  return checksum_crc32(crc, pos, length);
}

static int reset_previous_logged_gtids_relaylog(const Gtid_set *gtid_set,
                                                Checkable_rwlock *sid_lock) {
  const Gtid_set *executed_gtids = gtid_state->get_executed_gtids();
  const Gtid_set *gtids_only_in_table = gtid_state->get_gtids_only_in_table();

  sid_lock->wrlock();
  executed_gtids->get_sid_map()->get_sid_lock()->wrlock();
  int ret = (const_cast<Gtid_set *>(gtid_set)->add_gtid_set(executed_gtids) !=
             RETURN_STATUS_OK);
  executed_gtids->get_sid_map()->get_sid_lock()->unlock();
  if (!ret) {
    gtids_only_in_table->get_sid_map()->get_sid_lock()->wrlock();
    const_cast<Gtid_set *>(gtid_set)->remove_gtid_set(gtids_only_in_table);
    gtids_only_in_table->get_sid_map()->get_sid_lock()->unlock();
  }
  sid_lock->unlock();

  return ret;
}

ConsensusLogManager::ConsensusLogManager()
    : inited(false),
      first_event_in_file(false),
      prefetch_manager(NULL),
      fifo_cache_manager(NULL),
      log_file_index(NULL),
      consensus_info(NULL),
      consensus_applier_info(NULL),
      current_term(1),
      current_index(0),
      current_state_degrade_term(0),
      cache_index(0),
      sync_index(0),
      recovery_index_hwl(0),
      start_without_log(false),
      apply_index(0),
      real_apply_index(0),
      apply_term(0),
      in_large_trx(false),
      enable_rotate(true),
      apply_catchup(false),
      stop_term(UINT64_MAX),
      status(BINLOG_WORKING),
      binlog(NULL),
      rli_info(NULL),
      consensus_state_change_is_running(false),
      already_set_start_index(false),
      already_set_start_term(false),
      event_tv_sec(0) {}

ConsensusLogManager::~ConsensusLogManager() {}

int ConsensusLogManager::init(uint64 max_fifo_cache_size_arg,
                              uint64 max_prefetch_cache_size_arg,
                              uint64 fake_current_index_arg) {
  cache_log = new IO_CACHE_binlog_cache_storage();
  if (!cache_log) return 1;

  mysql_rwlock_init(key_rwlock_ConsensusLog_status_lock,
                    &LOCK_consensuslog_status);
  mysql_rwlock_init(key_rwlock_ConsensusLog_commit_lock,
                    &LOCK_consensuslog_commit);
  mysql_mutex_init(key_CONSENSUSLOG_LOCK_ConsensusLog_term_lock,
                   &LOCK_consensuslog_term, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(key_CONSENSUSLOG_LOCK_ConsensusLog_truncate_lock,
                   &LOCK_consensuslog_truncate, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(key_CONSENSUSLOG_LOCK_ConsensusLog_apply_thread_lock,
                   &LOCK_consensus_applier_catchup, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(key_CONSENSUSLOG_LOCK_Consensus_stage_change,
                   &LOCK_consensus_state_change, MY_MUTEX_INIT_FAST);

  mysql_cond_init(key_COND_ConsensusLog_catchup, &COND_consensus_applier_catchup);
  mysql_cond_init(key_COND_Consensus_state_change,
                  &COND_consensus_state_change);

  if (cache_log->open(mysql_tmpdir, LOG_PREFIX, binlog_cache_size,
                      max_binlog_cache_size))
    return 1;

  if (cache_log->get_io_cache()->m_encryptor != nullptr) {
    delete cache_log->get_io_cache()->m_encryptor;
    cache_log->get_io_cache()->m_encryptor = nullptr;
  }

  // initialize the empty log content
  Consensus_empty_log_event eev;
  eev.common_footer->checksum_alg =
      static_cast<enum_binlog_checksum_alg>(binlog_checksum_options);
  eev.write(cache_log);
  ulonglong buf_size = cache_log->length();
  uchar *buffer = (uchar *)my_malloc(key_memory_thd_main_mem_root,
                                     (size_t)buf_size, MYF(MY_WME));
  copy_from_consensus_log_cache(cache_log, buffer, buf_size);
  int4store(buffer, 0);  // set timestamp to 0
  empty_log_event_content = std::string((char *)buffer, buf_size);
  my_free(buffer);
  cache_log->reset();

  status = BINLOG_WORKING;
  current_index = fake_current_index_arg;

  fifo_cache_manager = new ConsensusFifoCacheManager();
  fifo_cache_manager->init(max_fifo_cache_size_arg);

  prefetch_manager = new ConsensusPreFetchManager();
  prefetch_manager->init(max_prefetch_cache_size_arg);

  log_file_index = new ConsensusLogIndex();
  log_file_index->init();

  Consensus_info_factory::init_consensus_repo_metadata();
  consensus_info = Consensus_info_factory::create_consensus_info();
  Consensus_info_factory::init_consensus_applier_repo_metadata();
  consensus_applier_info = Consensus_info_factory::create_consensus_applier_info();
  Consensus_info_factory::init_consensus_applier_worker_repo_metadata();
  if (!consensus_info || !consensus_applier_info) return 1;

  inited = true;
  return 0;
}

int ConsensusLogManager::init_consensus_info() {
  if (consensus_info->init_info()) {
    sql_print_error("Fail to init consensus_info.");
    return -1;
  }

  if (consensus_applier_info->init_info()) {
    sql_print_error("Fail to init consensus_applier_info.");
    return -1;
  }

  return 0;
}

int ConsensusLogManager::dump_cluster_info_to_file(std::string meta_file_name,
                                                   std::string output_str) {
  File meta_file = -1;
  if (!my_access(meta_file_name.c_str(), F_OK) &&
      mysql_file_delete(0, meta_file_name.c_str(), MYF(0))) {
    sql_print_error("Dump meta file failed, access file or delete file error");
    return -1;
  }
  if ((meta_file = mysql_file_open(0, meta_file_name.c_str(), O_RDWR | O_CREAT,
                                   MYF(MY_WME))) < 0 ||
      mysql_file_sync(meta_file, MYF(MY_WME))) {
    sql_print_error("Dump meta file failed, create file error");
    return -1;
  }

  if (my_write(meta_file, (const uchar *)output_str.c_str(),
               output_str.length(), MYF(MY_WME)) != output_str.length() ||
      mysql_file_sync(meta_file, MYF(MY_WME))) {
    sql_print_error("Dump meta file failed, write meta error");
    return -1;
  }

  if (meta_file >= 0) mysql_file_close(meta_file, MYF(0));

  return 0;
}
int ConsensusLogManager::change_meta_if_needed() {
  Consensus_info *consensus_info = get_consensus_info();
  if (opt_cluster_force_change_meta || opt_cluster_force_reset_meta) {
    if (opt_cluster_id) consensus_info->set_cluster_id(opt_cluster_id);
    if (opt_cluster_current_term)
      consensus_info->set_current_term(opt_cluster_current_term);
    else if (opt_cluster_force_reset_meta)
      consensus_info->set_current_term(1);

    if (opt_cluster_force_recover_index || opt_cluster_force_reset_meta) {
      // backup from leader can also recover like a follower
      consensus_info->set_recover_status(RELAY_LOG_WORKING);
      consensus_info->set_last_leader_term(0);
      consensus_info->set_start_apply_index(opt_cluster_force_recover_index);
    }

    // reuse opt_cluster, if normal stands for cluster info, else stands for
    // learner info
    if (!opt_cluster_info) {
      sql_print_error(
          "[x-cluster]  cluster_info must be set when the server is running "
          "with --initialize(-insecure) ");
      return -1;
    }
    consensus_info->set_cluster_info(std::string(opt_cluster_info));
    // if change meta, flush sys info, force quit
    consensus_info->flush_info(true, true);
    sql_print_warning("Force change meta to system table successfully.");
  }

  opt_cluster_id = get_consensus_info()->get_cluster_id();

  return 0;
  }

int ConsensusLogManager::start_consensus_state_change_thread() {
  consensus_state_change_is_running = true;
  if (mysql_thread_create(key_thread_consensus_stage_change,
                          &consensus_state_change_thread_handle, NULL,
                          run_consensus_stage_change,
                          (void *)&consensus_state_change_is_running)) {
    consensus_state_change_is_running = false;
    sql_print_error("Fail to create thread run_consensus_stage_change.");
    abort();
  }
  return 0;
}

int ConsensusLogManager::init_service() {
  Consensus_info *consensus_info = get_consensus_info();

  // learner's cluster_info is empty or not contain @
  bool is_learner =
      consensus_info->get_cluster_info() == "" ||
      consensus_info->get_cluster_info().find('@') == std::string::npos;

  uint64 mock_start_index = max(get_log_file_index()->get_first_index(),
                                (uint64)opt_consensus_start_index);
  rpl_consensus_init(is_learner, mock_start_index, &consensus_log_manager);

  return 0;
}

int ConsensusLogManager::update_consensus_info() {
  int error = 0;
  if (!opt_initialize) {
    error = change_meta_if_needed();
  } else {
    Consensus_info *consensus_info = get_consensus_info();

    if (opt_cluster_id) consensus_info->set_cluster_id(opt_cluster_id);

    if (!opt_cluster_info) {
      sql_print_error(
          "[x-cluster]  cluster_info must be set when the server is running "
          "with --initialize(-insecure) ");
      return -1;
    }

    if (!opt_cluster_learner_node) {
      consensus_info->set_cluster_info(std::string(opt_cluster_info));
      consensus_info->set_cluster_learner_info("");
    } else {
      consensus_info->set_cluster_learner_info(std::string(opt_cluster_info));
      consensus_info->set_cluster_info("");
    }

    consensus_info->flush_info(true, true);
  }

  return error;
}

int ConsensusLogManager::recovery_applier_status() {
  uint64 next_index = 0;
  uint64 log_pos = 0;
  char log_name[FN_REFLEN];

  // Load mts recovery end index for snapshot recovery
  if ((consistent_snapshot_recovery || opt_cluster_recover_from_snapshot) &&
      mts_recovery_max_consensus_index()) {
    return -1;
  }

  set_status(BINLOG_WORKING);

  // Get applier start index from consensus_applier_info
  if (!opt_initialize) next_index = get_applier_start_index();

  if (!opt_initialize && !opt_cluster_log_type_instance &&
      !consensus_log_manager.get_start_without_log()) {
    assert(next_index > 0);
    if (consensus_log_manager.get_log_position(next_index, false, log_name,
                                               &log_pos)) {
      sql_print_error("Cannot find applier start index %llu.", next_index);
      abort();
    }
  }

  // Reached this, the applier start index of consensus is set
  if (!opt_cluster_log_type_instance &&
      gtid_init_after_consensus_setup(
          next_index, (!consensus_log_manager.get_start_without_log() &&
                       next_index > 0 && consistent_snapshot_recovery)
                          ? log_name
                          : nullptr)) {
    return -1;
  }

  if (!opt_initialize && !opt_cluster_log_type_instance &&
      consensus_log_manager.get_start_without_log()) {
    assert(next_index > 0);
    if (consensus_log_manager.get_log_position(next_index, false, log_name,
                                               &log_pos)) {
      sql_print_error("Cannot find applier start index %llu.", next_index);
      abort();
    }
  }

  if (opt_cluster_recover_from_snapshot || opt_cluster_recover_from_backup) {
    if (opt_cluster_archive_recovery && opt_archive_log_index_name != nullptr) {
      uint64 recover_status = consensus_info->get_recover_status();
      if (recover_status != RELAY_LOG_WORKING ||
          consensus_info->get_start_apply_index() == 0) {
        if (next_index > 0) truncate_log(next_index);

        if (next_index > 0 && consensus_info->get_start_apply_index() == 0)
          consensus_info->set_start_apply_index(next_index - 1);

        if (recover_status != RELAY_LOG_WORKING)
          consensus_info->set_recover_status(RELAY_LOG_WORKING);

        consensus_info->flush_info(true, true);
      }

      /* Generate new binlog files from archive logs */
      if (consensus_open_archive_log(get_log_file_index()->get_first_index(),
                                     consensus_log_manager.get_sync_index()))
        return -1;

      /* Advance term to last log term */
      if (get_current_term() > consensus_info->get_current_term()) {
        consensus_info->set_current_term(get_current_term());
        consensus_info->flush_info(true, true);
        sql_print_information("Archive recovery, advance log term to %llu",
                              get_current_term());
      }
    } else if (next_index > 0) {
      truncate_log(next_index);
    }
  }

  if (!opt_initialize) {
    // Recovery finished, start consensus service from follower
    set_status(RELAY_LOG_WORKING);

    if (!opt_cluster_log_type_instance) {
      mysql_mutex_lock(binlog->get_log_lock());
      binlog->switch_and_seek_log(log_name, log_pos, true);
      mysql_mutex_unlock(binlog->get_log_lock());
    }

    // Set right current term for apply thread
    set_current_term(get_consensus_info()->get_current_term());
  }

  return 0;
}

int ConsensusLogManager::stop_consensus_state_change_thread() {
  if (inited && consensus_state_change_is_running) {
    consensus_state_change_is_running = false;
    mysql_mutex_lock(&LOCK_consensus_state_change);
    mysql_cond_broadcast(&COND_consensus_state_change);
    mysql_cond_broadcast(&COND_server_started);
    mysql_mutex_unlock(&LOCK_consensus_state_change);
    my_thread_join(&consensus_state_change_thread_handle, NULL);
  }
  return 0;
}

int ConsensusLogManager::cleanup() {
  if (inited) {
    fifo_cache_manager->cleanup();
    prefetch_manager->cleanup();
    log_file_index->cleanup();
    cache_log->close();

    consensus_info->end_info();

    delete consensus_info;
    delete consensus_applier_info;
    delete fifo_cache_manager;
    delete prefetch_manager;
    delete log_file_index;
    delete cache_log;

    mysql_rwlock_destroy(&LOCK_consensuslog_status);
    mysql_rwlock_destroy(&LOCK_consensuslog_commit);
    mysql_mutex_destroy(&LOCK_consensuslog_term);
    mysql_mutex_destroy(&LOCK_consensuslog_truncate);
    mysql_mutex_destroy(&LOCK_consensus_applier_catchup);
    mysql_mutex_destroy(&LOCK_consensus_state_change);
    mysql_cond_destroy(&COND_consensus_applier_catchup);
    mysql_cond_destroy(&COND_consensus_state_change);
  }
  return 0;
}

int ConsensusLogManager::set_start_apply_index_if_need(uint64 consensus_index) {
  if (opt_cluster_log_type_instance) return 0;
  mysql_rwlock_rdlock(&LOCK_consensuslog_status);
  if (!already_set_start_index && status == BINLOG_WORKING) {
    consensus_info->set_start_apply_index(consensus_index);
    if (consensus_info->flush_info(true, true)) {
      mysql_rwlock_unlock(&LOCK_consensuslog_status);
      return 1;
    }
    already_set_start_index = true;
  }
  mysql_rwlock_unlock(&LOCK_consensuslog_status);
  return 0;
}

int ConsensusLogManager::set_start_apply_term_if_need(uint64 consensus_term) {
  if (opt_cluster_log_type_instance) return 0;
  mysql_rwlock_rdlock(&LOCK_consensuslog_status);
  if (!already_set_start_term && status == BINLOG_WORKING) {
    consensus_info->set_last_leader_term(consensus_term);
    if (consensus_info->flush_info(true, true)) {
      mysql_rwlock_unlock(&LOCK_consensuslog_status);
      return 1;
    }
    already_set_start_term = true;
  }
  mysql_rwlock_unlock(&LOCK_consensuslog_status);
  return 0;
}

int ConsensusLogManager::write_log_entry(ConsensusLogEntry &log,
                                         uint64 *consensus_index,
                                         bool with_check) {
  int error = 0;

  mysql_rwlock_rdlock(&LOCK_consensuslog_status);

  enable_rotate = !(log.flag & Consensus_log_event_flag::FLAG_LARGE_TRX);
  if (status == Consensus_Log_System_Status::BINLOG_WORKING) {
    if ((error = append_consensus_log(binlog, log, consensus_index, nullptr,
                                      with_check))) {
      goto end;
    }
    if (*consensus_index == 0) goto end;
  } else {
    bool do_rotate = false;
    Master_info *mi = rli_info->mi;
    mysql_mutex_lock(&mi->data_lock);
    if (append_consensus_log(&rli_info->relay_log, log, consensus_index,
                             rli_info, with_check)) {
      mysql_mutex_unlock(&mi->data_lock);
      goto end;
    }
    if (*consensus_index == 0) {
      mysql_mutex_unlock(&mi->data_lock);
      goto end;
    }

    if (enable_rotate) {
      mysql_mutex_lock(rli_info->relay_log.get_log_lock());
      error = rli_info->relay_log.rotate(false, &do_rotate);
      mysql_mutex_unlock(rli_info->relay_log.get_log_lock());

      if (!error && do_rotate) {
        bool use_new_created_thd = false;
        if (current_thd == nullptr) {
          use_new_created_thd = true;
          current_thd = create_internal_thd();
        }
        rli_info->relay_log.auto_purge();
        if (use_new_created_thd) {
          destroy_internal_thd(current_thd);
          current_thd = nullptr;
        }
      }
    }
    mysql_mutex_unlock(&mi->data_lock);
  }
end:
  mysql_rwlock_unlock(&LOCK_consensuslog_status);
  if (error)
    sql_print_error(
        "ConsensusLogManager::write_log_entry error, consensus index: %llu.",
        *consensus_index);
  if (*consensus_index == 0)
    sql_print_error(
        "ConsensusLogManager::write_log_entry error, consensus index: %llu, "
        "because of a failed term check.",
        *consensus_index);
  return error;
}

int ConsensusLogManager::write_log_entries(std::vector<ConsensusLogEntry> &logs,
                                           uint64 *max_index) {
  int error = 0;

  // only follower will call this function
  mysql_rwlock_rdlock(&LOCK_consensuslog_status);
  bool do_rotate = false;
  MYSQL_BIN_LOG *log = status == Consensus_Log_System_Status::BINLOG_WORKING
                           ? binlog
                           : &rli_info->relay_log;
  enable_rotate =
      !(logs.back().flag & Consensus_log_event_flag::FLAG_LARGE_TRX);
  if ((error = append_multi_consensus_logs(log, logs, max_index, rli_info))) {
    goto end;
  }

  if (status != Consensus_Log_System_Status::BINLOG_WORKING && enable_rotate) {
    Master_info *mi = rli_info->mi;
    mysql_mutex_lock(&mi->data_lock);

    mysql_mutex_lock(log->get_log_lock());
    error = log->rotate(false, &do_rotate);
    mysql_mutex_unlock(log->get_log_lock());

    if (!error && do_rotate) {
      bool use_new_created_thd = false;
      if (current_thd == nullptr) {
        use_new_created_thd = true;
        current_thd = create_internal_thd();
      }
      log->auto_purge();
      if (use_new_created_thd) {
        destroy_internal_thd(current_thd);
        current_thd = nullptr;
      }
    }

    mysql_mutex_unlock(&mi->data_lock);
  }
end:
  mysql_rwlock_unlock(&LOCK_consensuslog_status);

  if (error)
    sql_print_error(
        "ConsensusLogManager::write_log_entries error, batch size: %d, max "
        "consensus index: %llu.",
        logs.size(), *max_index);
  return error;
}

int ConsensusLogManager::get_log_directly(uint64 consensus_index,
                                          uint64 *consensus_term,
                                          std::string &log_content, bool *outer,
                                          uint *flag, uint64 *checksum,
                                          bool need_content) {
  int error = 0;
  if (consensus_index == 0) {
    *consensus_term = 1;
    *outer = false;
    *flag = 0;
    return error;
  }
  mysql_rwlock_rdlock(&LOCK_consensuslog_status);
  if (consensus_get_log_entry(consensus_index, consensus_term, log_content,
                                   outer, flag, checksum, need_content))
    error = 1;
  mysql_rwlock_unlock(&LOCK_consensuslog_status);
  if (error)
    sql_print_error(
        "ConsensusLogManager::get_log_directly error,  consensus index: %llu.",
        consensus_index);
  return error;
}

int ConsensusLogManager::prefetch_log_directly(THD *thd, uint64 channel_id,
                                               uint64 consensus_index) {
  int error = 0;
  mysql_rwlock_rdlock(&LOCK_consensuslog_status);
  if (consensus_prefetch_log_entries(thd, channel_id, consensus_index))
    error = 1;
  mysql_rwlock_unlock(&LOCK_consensuslog_status);
  if (error)
    sql_print_error(
        "ConsensusLogManager::prefetch_log_directly error,  consensus index: "
        "%llu.",
        consensus_index);
  return error;
}

int ConsensusLogManager::get_log_entry(uint64 channel_id,
                                       uint64 consensus_index,
                                       uint64 *consensus_term,
                                       std::string &log_content, bool *outer,
                                       uint *flag, uint64 *checksum,
                                       bool fast_fail) {
  int error = 0;
  if (!opt_consensus_disable_fifo_cache && consensus_index > cache_index)
    return 1;
  if (consensus_index == 0) {
    *consensus_term = 0;
    log_content = "";
    return 0;
  }

  error = fifo_cache_manager->get_log_from_cache(
      consensus_index, consensus_term, log_content, outer, flag, checksum);
  if (error == ALREADY_SWAP_OUT) {
    uint64_t last_sync_index = sync_index;
    if (consensus_index > last_sync_index) {
      // don't prefetch log if it is not written to disk
      sql_print_information(
          "ConsensusLogManager::get_log_entry %llu fail, log has not been "
          "flushed to disk, sync index is %llu",
          consensus_index, last_sync_index);
      return 1;
    }
    ConsensusPreFetchChannel *channel =
        prefetch_manager->get_prefetch_channel(channel_id);
    if ((error = channel->get_log_from_prefetch_cache(
             consensus_index, consensus_term, log_content, outer, flag,
             checksum))) {
      if (!fast_fail) {
        error = get_log_directly(consensus_index, consensus_term, log_content,
                                 outer, flag, checksum,
                                 channel_id == 0 ? false : true);
      } else {
        error = 1;
      }
      channel->set_prefetch_request(consensus_index);
    }
  } else if (error == OUT_OF_RANGE) {
    sql_print_error(
        "ConsensusLogManager::get_log_entry fail, out of fifo range. "
        "channel_id %llu consensus index : %llu",
        channel_id, consensus_index);
  }
  if (error == 1)
    sql_print_information(
        "ConsensusLogManager::get_log_entry fail, channel_id %llu consensus "
        "index: %llu ,start prefetch.",
        channel_id, consensus_index);
  return error;
}

uint64_t ConsensusLogManager::get_left_log_size(uint64 start_log_index,
                                                uint64 max_packet_size) {
  uint64 total_size = 0;
  total_size += fifo_cache_manager->get_log_size_from_cache(
      start_log_index, cache_index, max_packet_size);
  // do not consider prefetch cache here
  return total_size;
}

int ConsensusLogManager::get_log_position(uint64 consensus_index,
                                          bool need_lock, char *log_name,
                                          uint64 *pos) {
  int error = 0;
  if (need_lock) mysql_rwlock_rdlock(&LOCK_consensuslog_status);
  if (consensus_get_log_position(consensus_index, log_name, pos)) {
    error = 1;
  }
  if (need_lock) mysql_rwlock_unlock(&LOCK_consensuslog_status);
  if (error)
    sql_print_error(
        "ConsensusLogManager::get_log_position error, consensus index: %llu.",
        consensus_index);
  return error;
}

uint64 ConsensusLogManager::get_next_trx_index(uint64 consensus_index,
                                               bool need_lock) {
  uint64 retIndex = consensus_index;
  if (need_lock) mysql_rwlock_rdlock(&LOCK_consensuslog_status);
  retIndex = consensus_get_trx_end_index(consensus_index);
  if (need_lock) mysql_rwlock_unlock(&LOCK_consensuslog_status);
  if (retIndex == 0) {
    sql_print_error("ConsensusLogManager: fail to find next trx index.");
    abort();
  }
  sql_print_information("ConsensusLogManager: next transaction index is %llu.",
                        retIndex + 1);
  return retIndex + 1;
}

int ConsensusLogManager::truncate_log(uint64 consensus_index) {
  int error = 0;
  uint64 start_index = 0;
  uint64 offset;
  std::string file_name;

  sql_print_information("Consensus truncate log, index is %llu",
                        consensus_index);
  prefetch_manager->stop_prefetch_threads();
  mysql_rwlock_rdlock(&LOCK_consensuslog_status);
  mysql_mutex_lock(&LOCK_consensuslog_truncate);
  MYSQL_BIN_LOG *log =
      status == BINLOG_WORKING ? binlog : &(rli_info->relay_log);
  mysql_mutex_lock(log->get_log_lock());

  // truncate log file
  Relay_log_info *rli = status == BINLOG_WORKING ? nullptr : rli_info;
  if (consensus_find_log_by_index(consensus_index, file_name, start_index) ||
      consensus_find_pos_by_index(file_name.c_str(), start_index,
                                  consensus_index, &offset)) {
    offset = 0;
    error = 1;
  } else if (log->truncate_log(file_name.c_str(), offset, rli)) {
    error = 1;
  }

  if (!error) {
    set_sync_index(consensus_index - 1);
    set_current_index(consensus_index);
    log_file_index->truncate_after(file_name);
    log_file_index->truncate_pos_map_of_file(start_index, consensus_index);
    fifo_cache_manager->trunc_log_from_cache(consensus_index);
    prefetch_manager->trunc_log_from_prefetch_cache(consensus_index);
  }

  // truncate cache
  mysql_mutex_unlock(log->get_log_lock());

  // reset the previous_gtid_set_of_relaylog after truncate log
  if (!error && rli != nullptr) {
    mysql_mutex_lock(&rli->data_lock);
    error = rli->reset_previous_gtid_set_of_consensus_log();
    mysql_mutex_unlock(&rli->data_lock);
  }

  mysql_mutex_unlock(&LOCK_consensuslog_truncate);
  mysql_rwlock_unlock(&LOCK_consensuslog_status);
  if (error) {
    sql_print_error(
        "Consensus truncate log failed, truncate to file %s offset %d",
        file_name.c_str(), offset);
    abort();
  }
  prefetch_manager->start_prefetch_threads();
  return error;
}

int ConsensusLogManager::purge_log(uint64 consensus_index) {
  int error = 0;
  std::string file_name;
  uint64 start_index;
  mysql_rwlock_rdlock(&LOCK_consensuslog_status);
  uint64 purge_index = 0;

  if (status == BINLOG_WORKING) {
    // server still work as leader, so we should at least retain 1 binlog file
    // apply pos must at the last binlog file
    purge_index = consensus_index;
  } else {
    uint64 start_apply_index = consensus_info->get_start_apply_index();
    if (start_apply_index == 0) {
      // apply thread already start, use slave applied index as purge index
      purge_index = consensus_applier_info->get_consensus_apply_index();
    } else {
      // apply thread not start
      purge_index = start_apply_index;
    }
    purge_index = opt_cluster_log_type_instance
                      ? consensus_index
                      : std::min(purge_index, consensus_index);
  }

  MYSQL_BIN_LOG *log =
      status == BINLOG_WORKING ? binlog : &(rli_info->relay_log);
  if (status == RELAY_LOG_WORKING) {
    mysql_mutex_lock(&rli_info->data_lock);
  }

  sql_print_information("purge_log in ConsensusLogManager, purge_index:%d",
                        purge_index);
  log->lock_index();
  if (consensus_find_log_by_index(purge_index, file_name, start_index) ||
      log->purge_logs(file_name.c_str(), false /**include*/,
                      false /*need index lock*/, true /*update threads*/, NULL,
                      true)) {
    error = 1;
  }
  log->unlock_index();

  sql_print_information("purge_logs before %s in ConsensusLogManager",
                        file_name.c_str());

  if (status == RELAY_LOG_WORKING) {
    mysql_mutex_unlock(&rli_info->data_lock);
  }

  mysql_rwlock_unlock(&LOCK_consensuslog_status);
  if (error)
    sql_print_error(
        "ConsensusLogManager::purge_log error, consensus index: %llu.",
        consensus_index);
  return error;
}

uint64 ConsensusLogManager::get_exist_log_length() {
  // first index in log index may not be exact value
  // so we should check whether sync_index is larger
  uint64 start_index = log_file_index->get_first_index();
  uint64 end_index = sync_index;
  if (end_index >= start_index)
    return end_index - start_index;
  else
    return 0;
}

void ConsensusLogManager::wait_replay_log_finished() {
  mysql_mutex_lock(&LOCK_consensus_applier_catchup);
  if (apply_catchup) {
    apply_catchup = false;
    mysql_mutex_unlock(&LOCK_consensus_applier_catchup);
  } else {
    struct timespec abstime;
    set_timespec(&abstime, 2);
    while (!apply_catchup && consensus_state_change_is_running) {
      mysql_cond_timedwait(&COND_consensus_applier_catchup,
                           &LOCK_consensus_applier_catchup, &abstime);
    }
    apply_catchup = false;
    mysql_mutex_unlock(&LOCK_consensus_applier_catchup);
  }

}

void ConsensusLogManager::wait_apply_threads_stop() {
  while (rli_info->slave_running && consensus_state_change_is_running) {
    my_sleep(200);
  }
}

// #ifdef HAVE_REPLICATION
int ConsensusLogManager::wait_leader_degraded(uint64 term, uint64 index) {
  sql_print_information(
      "ConsensusLogManager::wait_leader_degraded, consensus term: %llu, "
      "consensus index: %llu",
      term, index);
  int error = 0;

  DBUG_EXECUTE_IF("simulate_leader_degrade_slow", {
    my_sleep(10000000);
    sql_print_information("wait_leader_degraded sleep 10 seconds");
  });

  // prefetch stop
  prefetch_manager->disable_all_prefetch_channels();

  // rollback transactions not reached before_commit
  acquire_transaction_control_services();

  if (!opt_cluster_log_type_instance) {
    // Persist start apply index into consensus_info before killing all threads
    // and waiting all transactions finished. The transactions before it will be
    // committed. And the transactions after it will be rollbacked firstly and
    // replayed by applier thread if needed.
    consensus_info->set_start_apply_index(index);
    if (consensus_info->flush_info(true, true)) {
      sql_print_error(
          "Failed in flush_info() called from "
          "ConsensusLog::wait_leader_degraded.");
      error = 1;
      goto end;
    }

    // Update state change term after flushed consensus info
    current_state_degrade_term = term;
  }

  assert(this->status == Consensus_Log_System_Status::BINLOG_WORKING);

  DBUG_EXECUTE_IF("signal_before_downgrade_init_rli", {
    const char act[] = "now wait_for signal_downgrade_init_rli";
    assert(!debug_sync_set_action(current_thd, STRING_WITH_LEN(act)));
  });

  channel_map.rdlock();
  rli_info->mi->channel_wrlock();
  mysql_rwlock_wrlock(&LOCK_consensuslog_status);

  // Wait all active trx commit
  mysql_rwlock_wrlock(&LOCK_consensuslog_commit);
  mysql_rwlock_unlock(&LOCK_consensuslog_commit);

  // make sure that the transactions are committed in engines
  ha_flush_logs();

  // open relay log system
  mysql_mutex_lock(&rli_info->mi->data_lock);
  mysql_mutex_lock(&rli_info->data_lock);
  rli_info->cli_init_info();
  mysql_mutex_unlock(&rli_info->data_lock);
  mysql_mutex_unlock(&rli_info->mi->data_lock);

  // record new term
  current_term = term;
  this->status = Consensus_Log_System_Status::RELAY_LOG_WORKING;

  DBUG_EXECUTE_IF("signal_after_downgrade_init_rli", {
    const char act[] = "now signal after_downgrade_init_rli";
    assert(!debug_sync_set_action(current_thd, STRING_WITH_LEN(act)));
  });

  stop_term = UINT64_MAX;

  if (!opt_cluster_log_type_instance) {
    // Save executed_gtids to tables and reset rli_info->gtid_set.
    if (gtid_state->save_gtids_of_last_binlog_into_table() ||
        (index < get_sync_index()
             ? rli_info->reset_previous_gtid_set_of_consensus_log()
             : reset_previous_logged_gtids_relaylog(
                   rli_info->get_gtid_set(), rli_info->get_sid_lock()))) {
      mysql_rwlock_unlock(&LOCK_consensuslog_status);
      sql_print_error("Failed in save last binlog gtid into table.");
      error = 1;
      goto end;
    }
    current_state_degrade_term = 0;

    error = start_consensus_apply_threads(rli_info->mi);
  }

  mysql_rwlock_unlock(&LOCK_consensuslog_status);

end:
  release_transaction_control_services();
  rli_info->mi->channel_unlock();
  channel_map.unlock();
  // recover prefetch
  prefetch_manager->enable_all_prefetch_channels();
  return error;
}

int ConsensusLogManager::wait_follower_upgraded(uint64 term, uint64 index) {
  sql_print_information(
      "ConsensusLogManager::wait_follower_upgraded, consensus term: %llu, "
      "consensus index: %llu",
      term, index);
  int error = 0;

  assert(this->status == Consensus_Log_System_Status::RELAY_LOG_WORKING);

  // record new term
  // notice, the order of stop term and current term is important for apply
  // thread, because both stop term and current term is atomic variables. so do
  // not care reorder problem
  stop_term = term;

  // wait replay thread to commit index
  if (!opt_cluster_log_type_instance) {
    DBUG_EXECUTE_IF("simulate_apply_too_slow", my_sleep(5000000););
    wait_replay_log_finished();
    wait_apply_threads_stop();
    if (!consensus_state_change_is_running) return 0;
  }

  // prefetch stop, and release LOCK_consensuslog_status
  prefetch_manager->disable_all_prefetch_channels();

  // kill all binlog dump thd
  binlog_dump_thread_kill();

  channel_map.rdlock();
  rli_info->mi->channel_wrlock();
  mysql_rwlock_wrlock(&LOCK_consensuslog_status);

  // Ensure all GTIDs to persist on disk.
  ha_flush_logs();

  mysql_mutex_lock(&rli_info->data_lock);
  rli_info->end_info();
  mysql_mutex_unlock(&rli_info->data_lock);

  mysql_mutex_t *log_lock = binlog->get_log_lock();
  mysql_mutex_lock(log_lock);
  binlog->lock_index();

  // close binlog system
  binlog->close(LOG_CLOSE_INDEX | LOG_CLOSE_TO_BE_OPENED, false, false);

  // open binlog index
  if (binlog->open_index_file(opt_binlog_index_name, opt_bin_logname, false)) {
    binlog->unlock_index();
    mysql_mutex_unlock(log_lock);
    mysql_rwlock_unlock(&LOCK_consensuslog_status);
    sql_print_error(
        "ConsensusLog::wait_follower_upgraded, failed in open_index_file()");
    error = 1;
    goto end;
  }

  if (binlog->open_exist_consensus_binlog(opt_bin_logname, max_binlog_size,
                                          true, false)) {
    binlog->unlock_index();
    mysql_mutex_unlock(log_lock);
    mysql_rwlock_unlock(&LOCK_consensuslog_status);
    sql_print_error(
        "ConsensusLog::wait_follower_upgraded, failed in open_log()");
    error = 2;
    goto end;
  }
  binlog->unlock_index();
  mysql_mutex_unlock(log_lock);
  this->status = Consensus_Log_System_Status::BINLOG_WORKING;

  // reset apply start point displayed in information_schema
  apply_index = 0;
  real_apply_index = 0;
  already_set_start_index = false;
  already_set_start_term = false;
  current_term = term;

  // log type instance do not to recover start index
  if (!opt_cluster_log_type_instance) {
    consensus_info->set_last_leader_term(term);
  }
  consensus_info->set_recover_status(
      Consensus_Log_System_Status::BINLOG_WORKING);

  if (consensus_info->flush_info(true, true)) {
    mysql_rwlock_unlock(&LOCK_consensuslog_status);
    sql_print_error(
        "Failed in flush_info() called from "
        "ConsensusLog::wait_follower_upgraded.");
    error = 3;
    goto end;
  }
  mysql_rwlock_unlock(&LOCK_consensuslog_status);
end:
  rli_info->mi->channel_unlock();
  channel_map.unlock();
  // recover prefetch
  prefetch_manager->enable_all_prefetch_channels();
  return error;
}

int ConsensusLogManager::wait_follower_change_term(uint64 term) {
  sql_print_information(
      "ConsensusLogManager::wait_follower_change_term, consensus term: %llu.",
      term);
  current_term = term;
  return 0;
}

// #endif

uint64 ConsensusLogManager::get_cache_index() { return cache_index; }

void ConsensusLogManager::set_cache_index(uint64 cache_index_arg) {
  cache_index = cache_index_arg;
}

uint64 ConsensusLogManager::get_sync_index() { return sync_index; }

void ConsensusLogManager::set_sync_index(uint64 sync_index_arg) {
  sync_index = sync_index_arg;
}

void ConsensusLogManager::set_sync_index_if_greater(uint64 sync_index_arg) {
  for (;;) {
    uint64 old = sync_index.load();
    if (old >= sync_index_arg ||
        (old < sync_index_arg &&
         sync_index.compare_exchange_weak(old, sync_index_arg)))
      break;
  }
}

void ConsensusLogManager::add_state_change_request(
    ConsensusStateChange &state_change) {
  mysql_mutex_lock(&LOCK_consensus_state_change);
  consensus_state_change_queue.push_back(state_change);
  mysql_mutex_unlock(&LOCK_consensus_state_change);
  mysql_cond_broadcast(&COND_consensus_state_change);
}

void ConsensusLogManager::lock_consensus_state_change() {
  mysql_mutex_lock(&LOCK_consensus_state_change);
}

void ConsensusLogManager::unlock_consensus_state_change() {
  mysql_mutex_unlock(&LOCK_consensus_state_change);
}

void ConsensusLogManager::wait_state_change_cond() {
  mysql_cond_wait(&COND_consensus_state_change, &LOCK_consensus_state_change);
}

ConsensusStateChange ConsensusLogManager::get_stage_change_from_queue() {
  ConsensusStateChange state_change;

  if (consensus_state_change_queue.size() > 0) {
    state_change = consensus_state_change_queue.front();
    consensus_state_change_queue.pop_front();
  }
  return state_change;
}

// #ifdef HAVE_REPLICATION
void *run_consensus_stage_change(void *arg) {
  THD *thd = nullptr;
  thd = new THD;
  thd->set_new_thread_id();

  if (my_thread_init()) return NULL;

  thd->thread_stack = (char *)&thd;
  thd->store_globals();

  int error = 0;
  std::atomic<bool> *is_running = reinterpret_cast<std::atomic<bool> *>(arg);

  /*
    Waiting until mysqld_server_started == true to ensure that all server
    components have been successfully initialized.
  */
  mysql_mutex_lock(&LOCK_server_started);
  while (!mysqld_server_started && (*is_running))
    mysql_cond_wait(&COND_server_started, &LOCK_server_started);
  mysql_mutex_unlock(&LOCK_server_started);
  while (*is_running) {
    consensus_log_manager.lock_consensus_state_change();
    if (consensus_log_manager.is_state_change_queue_empty() && (*is_running))
      consensus_log_manager.wait_state_change_cond();
    if (consensus_log_manager.is_state_change_queue_empty()) {
      consensus_log_manager.unlock_consensus_state_change();
      continue;
    }
    ConsensusStateChange state_change =
        consensus_log_manager.get_stage_change_from_queue();

    sql_print_information("run_consensus_stage_change start: %d,%d",
                          state_change.state,
                          consensus_log_manager.get_status());

    if (state_change.state != LEADER) {
      if (consensus_log_manager.get_status() == BINLOG_WORKING) {
        error = consensus_log_manager.wait_leader_degraded(state_change.term,
                                                           state_change.index);
      } else if (state_change.state != CANDIDATE) {
        error =
            consensus_log_manager.wait_follower_change_term(state_change.term);
      }
    } else {
      // must be candidate ->leader
      error = consensus_log_manager.wait_follower_upgraded(state_change.term,
                                                           state_change.index);
    }

    consensus_log_manager.unlock_consensus_state_change();

    sql_print_information("run_consensus_stage_change end: %d,%d,error=%d",
                          state_change.state,
                          consensus_log_manager.get_status(), error);

    if (error) {
      sql_print_error("Consensus state change failed");
      abort();
    }
  }
  thd->release_resources();
  delete thd;
  my_thread_end();
  return NULL;
}
// #endif

bool ConsensusLogManager::is_state_machine_ready() {
  assert(rpl_consensus_get_term() >= get_current_term());
  return status == Consensus_Log_System_Status::BINLOG_WORKING &&
         rpl_consensus_get_term() == get_current_term();
}


