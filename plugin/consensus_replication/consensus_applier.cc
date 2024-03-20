#include "consensus_applier.h"
#include "consensus_log_manager.h"
#include "plugin_psi.h"
#include "system_variables.h"

#include "my_config.h"
#include "my_dbug.h"

#include "sql/log.h"
#include "my_loglevel.h"
#include "mysql/components/services/log_builtins.h"

#include "sql/binlog.h"
#include "sql/binlog_reader.h"
#include "sql/changestreams/apply/replication_thread_status.h"
#include "sql/consensus/consensus_applier_info.h"
#include "sql/consensus/consensus_applier_worker.h"
#include "sql/consensus/consensus_info_factory.h"
#include "sql/log.h"
#include "sql/rpl_info_factory.h"
#include "sql/rpl_mi.h"
#include "sql/rpl_msr.h"
#include "sql/rpl_replica.h"
#include "sql/rpl_rli.h"
#include "sql/rpl_rli_pdb.h"
#include "sql/transaction.h"

struct mysql_cond_t;
struct mysql_mutex_t;

typedef uint64_t LOG_INDEX_COORD;

int mts_recovery_max_consensus_index() {
  DBUG_TRACE;

  int error = 0;
  ulonglong max_consensus_apply_index = 0;
  Consensus_applier_info *applier_info =
      consensus_log_manager.get_applier_info();

  if (applier_info->recovery_parallel_workers == 0) {
    return error;
  }

  max_consensus_apply_index =
      applier_info->get_consensus_apply_index();

  for (uint id = 0; id < applier_info->recovery_parallel_workers; id++) {
    Consensus_applier_worker *worker =
        Consensus_info_factory::create_consensus_applier_woker(id, true);
    if (!worker) {
      error = 1;
      break;
    }
    if (worker->get_consensus_apply_index() > max_consensus_apply_index) {
      max_consensus_apply_index = worker->get_consensus_apply_index();
    }
    delete worker;
  }

  if (!error) {
    applier_info->set_mts_consensus_hwm_index(
        max_consensus_apply_index);
    sql_print_information(
        "Mts recover hwm consensus index %llu, checkpoint consensus index "
        "%llu.",
        max_consensus_apply_index,
        applier_info->get_consensus_apply_index());
  }

  return error;
}

bool is_consensus_applier_running() {
  DBUG_TRACE;
  Relay_log_info *rli = nullptr;
  bool ret = false;

  channel_map.assert_some_lock();

  mysql_rwlock_wrlock(consensus_log_manager.get_consensuslog_status_lock());

  rli = consensus_log_manager.get_relay_log_info();
  if (rli) ret = rli->inited;

  mysql_rwlock_unlock(consensus_log_manager.get_consensuslog_status_lock());

  return ret;
}

int start_consensus_replica() {
  DBUG_TRACE;
  Master_info *mi = NULL;
  int error = 0;

  channel_map.rdlock();
  for (mi_map::iterator it = channel_map.begin(CONSENSUS_REPLICATION_CHANNEL);
       it != channel_map.end(CONSENSUS_REPLICATION_CHANNEL); it++) {
    mi = it->second;
    if (mi) {
      mi->channel_wrlock();
      error = start_consensus_apply_threads(mi);
      mi->channel_unlock();
    }
  }
  channel_map.unlock();

  return error;
}

int start_consensus_apply_threads(Master_info *mi) {
  DBUG_TRACE;
  int thread_mask = SLAVE_SQL;
  int error = 0;

  assert(mi &&
         channel_map.is_consensus_replication_channel_name(mi->get_channel()));

  /* same as in start_slave() cache the global var values into rli's
   * members */
  mi->rli->opt_replica_parallel_workers = opt_mts_replica_parallel_workers;
  mi->rli->checkpoint_group = opt_mta_checkpoint_group;
  if (mts_parallel_option == MTS_PARALLEL_TYPE_DB_NAME)
    mi->rli->channel_mts_submode = MTS_PARALLEL_TYPE_DB_NAME;
  else
    mi->rli->channel_mts_submode = MTS_PARALLEL_TYPE_LOGICAL_CLOCK;

  if (start_slave_threads(true /*need_lock_slave=true*/,
                          false /*wait_for_start=false*/, mi, thread_mask)) {
    sql_print_error("Failed to start consensus apply threads");
    error = 1;
  }

  return error;
}

/*
  Release slave threads at time of executing shutdown.
*/
void stop_consensus_apply_threads() {
  DBUG_TRACE;

  Master_info *mi = nullptr;

  channel_map.wrlock();
  mysql_rwlock_wrlock(consensus_log_manager.get_consensuslog_status_lock());

  if (consensus_log_manager.get_relay_log_info())
    mi = consensus_log_manager.get_relay_log_info()->mi;

  if (mi) {
    mi->channel_wrlock();
    terminate_slave_threads(mi, SLAVE_SQL, rpl_stop_replica_timeout);
    mi->channel_unlock();
  }

  mysql_rwlock_unlock(consensus_log_manager.get_consensuslog_status_lock());
  channel_map.unlock();
}

int check_exec_consensus_log_end_condition(Relay_log_info *rli) {
  DBUG_ENTER("check_exec_consensus_log_end_condition");
  while (rpl_consensus_check_commit_index(
             consensus_log_manager.get_real_apply_index(),
             consensus_log_manager.get_current_term()) <
         consensus_log_manager.get_real_apply_index()) {
    if (sql_slave_killed(rli->info_thd, rli)) {
      sql_print_information("Apply thread is terminated because of shutdown");
      DBUG_RETURN(1);
    }

    if (rpl_consensus_is_shutdown()) {
      sql_print_information("Apply thread is terminated because of shutdown");
      DBUG_RETURN(1);
    }

    // determine whether exit
    uint64 stop_term = consensus_log_manager.get_stop_term();
    if (stop_term == UINT64_MAX) {
      my_sleep(opt_consensus_check_commit_index_interval);
      continue;
    } else if (consensus_log_manager.get_apply_term() >= stop_term) {
      mysql_mutex_lock(consensus_log_manager.get_apply_thread_lock());
      consensus_log_manager.set_apply_catchup(true);
      mysql_cond_broadcast(consensus_log_manager.get_catchup_cond());
      rli->sql_thread_kill_accepted = true;
      mysql_mutex_unlock(consensus_log_manager.get_apply_thread_lock());
      LogErr(SYSTEM_LEVEL, ER_CONSENSUS_APPLY_THREAD_STOP_POS,
             consensus_log_manager.get_apply_index(),
             consensus_log_manager.get_current_term(),
             consensus_log_manager.get_stop_term());
      DBUG_RETURN(1);
    } else if (rpl_consensus_get_commit_index() >
               consensus_log_manager.get_real_apply_index()) {
      // not reach commit index, continue to read log
      break;
    } else {
      // reach commit index, continue to wait exit condition
      my_sleep(opt_consensus_check_commit_index_interval);
      continue;
    }
  }

  DBUG_RETURN(0);
}

void update_consensus_apply_pos(Relay_log_info *rli, Log_event *ev) {
  DBUG_TRACE;

  // update apply index
  /* for large trx, use the first one */
  if (ev->get_type_code() == binary_log::CONSENSUS_LOG_EVENT) {
    Consensus_log_event *r_ev = static_cast<Consensus_log_event *>(ev);
    uint64 consensus_index = r_ev->get_index();
    uint64 consensus_term = r_ev->get_term();
    uint64 last_apply_term = consensus_log_manager.get_apply_term();
    if (r_ev->get_flag() & Consensus_log_event_flag::FLAG_LARGE_TRX) {
      if (!consensus_log_manager.get_in_large_trx()) {
        consensus_log_manager.set_apply_index(consensus_index);
        consensus_log_manager.set_apply_term(consensus_term);
        consensus_log_manager.set_in_large_trx(true);
      }
    } else if (r_ev->get_flag() &
               Consensus_log_event_flag::FLAG_LARGE_TRX_END) {
      consensus_log_manager.set_in_large_trx(false);
    } else {
      /* normal case */
      consensus_log_manager.set_apply_index(consensus_index);
      consensus_log_manager.set_apply_term(consensus_term);
      consensus_log_manager.set_in_large_trx(false);
    }
    consensus_log_manager.set_real_apply_index(consensus_index);
    if (!rli->is_parallel_exec())
      rpl_consensus_update_applied_index(consensus_index);

    // force the coordinator to start a new binlog segment.
    if ((r_ev->get_flag() & Consensus_log_event_flag::FLAG_ROTATE) ||
        last_apply_term != consensus_log_manager.get_apply_term()) {
      assert(rli->is_parallel_exec()
                 ? (rli->mts_group_status != Relay_log_info::MTS_IN_GROUP)
                 : !rli->is_in_group());
      if (!is_mts_db_partitioned(rli)) {
        static_cast<Mts_submode_logical_clock *>(rli->current_mts_submode)
            ->start_new_group();
      }
    }
  } else {
    MYSQL_BIN_LOG *binlog = consensus_log_manager.get_binlog();
    mysql_mutex_lock(binlog->get_log_lock());
    binlog->switch_and_seek_log(rli->get_event_relay_log_name(),
                                ev->future_event_relay_log_pos, true);
    mysql_mutex_unlock(binlog->get_log_lock());
  }
}

int calculate_consensus_apply_start_pos(Relay_log_info *rli) {
  DBUG_TRACE;

  uint64 recover_status = 0;
  ulonglong start_apply_index = 0;
  uint64 rli_appliedindex = 0;
  uint64 log_pos = 0;
  char log_name[FN_REFLEN];
  uint64 first_index;
  Consensus_applier_info *applier_info =
      consensus_log_manager.get_applier_info();

  assert(consensus_log_manager.get_status() == RELAY_LOG_WORKING);

  first_index = consensus_log_manager.get_log_file_index()->get_first_index();
  recover_status =
      consensus_log_manager.get_consensus_info()->get_recover_status();
  start_apply_index =
      consensus_log_manager.get_consensus_info()->get_start_apply_index();

  LogErr(SYSTEM_LEVEL, ER_CONSENSUS_APPLY_THREAD_START, recover_status,
         start_apply_index,
         applier_info->get_consensus_apply_index());

  if (recover_status == BINLOG_WORKING) {
    std::string recover_log_content;
    uint64 start_index =
        start_apply_index > applier_info->get_consensus_apply_index()
            ? start_apply_index
            : applier_info->get_consensus_apply_index();
    uint64 next_index =
        start_index < first_index
            ? first_index
            : consensus_log_manager.get_next_trx_index(start_index, false);

    if (consensus_log_manager.get_log_position(next_index, false, log_name,
                                               &log_pos)) {
      sql_print_error("Apply thread cannot find start index %llu.", next_index);
      abort();
    }
    LogErr(SYSTEM_LEVEL, ER_CONSENSUS_APPLY_START_POS, log_name, log_pos,
           start_index);

    applier_info->set_consensus_apply_index(start_index);
    applier_info->flush_info(true);
  } else {
    uint64 start_index = applier_info->get_consensus_apply_index();
    uint64 first_index =
        consensus_log_manager.get_log_file_index()->get_first_index();
    uint64 next_index =
        start_index < first_index
            ? first_index
            : consensus_log_manager.get_next_trx_index(start_index, false);
    if (consensus_log_manager.get_log_position(next_index, false, log_name,
                                               &log_pos)) {
      sql_print_error("Apply thread cannot find start index %llu.", next_index);
      abort();
    }
    LogErr(SYSTEM_LEVEL, ER_CONSENSUS_APPLY_START_POS, log_name, log_pos,
           start_index);
  }

  rli->set_group_relay_log_name(log_name);
  rli->set_group_relay_log_pos(log_pos);
  rli->flush_info(true);

  // deal with appliedindex
  rli_appliedindex = applier_info->get_consensus_apply_index();
  update_consensus_applied_index(rli_appliedindex);

  // set consensus info to relay-working
  consensus_log_manager.get_consensus_info()->set_start_apply_index(0);
  consensus_log_manager.get_consensus_info()->set_last_leader_term(0);
  consensus_log_manager.get_consensus_info()->set_recover_status(
      RELAY_LOG_WORKING);
  if (consensus_log_manager.get_consensus_info()->flush_info(true, true)) {
    rli->report(ERROR_LEVEL, ER_REPLICA_FATAL_ERROR,
                "Error flush consensus info set recover status");
    return -1;
  }

  return 0;
}

void update_consensus_applied_index(uint64 applied_index) {
  applied_index = opt_appliedindex_force_delay >= applied_index
                         ? 0
                         : applied_index - opt_appliedindex_force_delay;
  rpl_consensus_update_applied_index(applied_index);
}


/* Initialize slave structures */
int init_consensus_replica() {
  DBUG_TRACE;
  int channel_error = 0;
  int thread_mask = SLAVE_IO | SLAVE_SQL;
  Master_info *mi = nullptr;

  /*
    This is called when mysqld starts. Before client connections are
    accepted. However bootstrap may conflict with us if it does START SLAVE.
    So it's safer to take the lock.
  */
  channel_map.wrlock();

  /*
    Create and initialize the channels.
  */
  const char *cname = channel_map.get_consensus_replication_applier_channel();

  mi = channel_map.get_mi(cname);

  if (mi == nullptr) {
    channel_error = !(mi = Rpl_info_factory::create_mi_and_rli_objects(
                          INFO_REPOSITORY_TABLE, INFO_REPOSITORY_TABLE, cname,
                          false, &channel_map));
    /*
      Read the channel configuration from the repository if the channel name
      was read from the repository.
    */
    if (!channel_error) {
      channel_error = load_mi_and_rli_from_repositories(mi, false, thread_mask,
                                                        false, true);
    }

    if (channel_error) {
      LogErr(ERROR_LEVEL, ER_RPL_REPLICA_FAILED_TO_INIT_A_CONNECTION_METADATA_STRUCTURE,
             cname);
    }
  }

  if (!channel_error) {
    // Ensure mi is configured
    strcpy(mi->host, "null");

    mi->rli->replicate_same_server_id = true;

    // bind relay log info to global consensuslog
    consensus_log_manager.set_relay_log_info(mi->rli);
  }

  channel_map.unlock();

  return channel_error;
}

/*
  Release slave threads at time of executing shutdown.

  SYNOPSIS
    end_slave()
*/

void end_consensus_replica() {
  DBUG_TRACE;

  Master_info *mi = nullptr;

  /*
    This is called when the server terminates, in close_connections().
    It terminates slave threads. However, some CHANGE MASTER etc may still be
    running presently. If a START SLAVE was in progress, the mutex lock below
    will make us wait until slave threads have started, and START SLAVE
    returns, then we terminate them here.
  */
  channel_map.wrlock();

  /* traverse through the map and terminate the threads */
  for (mi_map::iterator it = channel_map.begin(CONSENSUS_REPLICATION_CHANNEL);
       it != channel_map.end(CONSENSUS_REPLICATION_CHANNEL); it++) {
    mi = it->second;

    if (mi)
      terminate_slave_threads(mi, SLAVE_SQL, rpl_stop_replica_timeout);
  }
  channel_map.unlock();
}

static bool is_autocommit_off_and_infotables(THD *thd) {
  DBUG_TRACE;
  return (thd && thd->in_multi_stmt_transaction_mode() &&
          (opt_mi_repository_id == INFO_REPOSITORY_TABLE ||
           opt_rli_repository_id == INFO_REPOSITORY_TABLE))
             ? true
             : false;
}

static int mts_event_coord_cmp(LOG_INDEX_COORD &id1, LOG_INDEX_COORD &id2) {
  return id1 < id2 ? -1 : (id1 == id2 ? 0 : 1);
}

bool applier_mts_recovery_groups(Relay_log_info *rli) {
  Log_event *ev = nullptr;
  bool is_error = false;
  bool flag_group_seen_begin = false;
  uint recovery_group_cnt = 0;
  bool not_reached_commit = true;
  Consensus_applier_info *applier_info =
      consensus_log_manager.get_applier_info();

  // Value-initialization, to avoid compiler warnings on push_back.
  Slave_job_group job_worker = Slave_job_group();

  LOG_INFO linfo;
  my_off_t offset = 0;
  MY_BITMAP *groups = &rli->recovery_groups;
  THD *thd = current_thd;
  uint64 consensus_index = 0;

  DBUG_TRACE;

  assert(rli->replica_parallel_workers == 0);

  /*
     Although mts_recovery_groups() is reentrant it returns
     early if the previous invocation raised any bit in
     recovery_groups bitmap.
  */
  if (rli->is_mts_recovery()) return false;

  /*
    The process of relay log recovery for the multi threaded applier
    is focused on marking transactions as already executed so they are
    skipped when the SQL thread applies them.
    This is important as the position stored for the last executed relay log
    position may be behind what transactions workers already handled.
    When GTID_MODE=ON however we can use the old relay log position, even if
    stale as applied transactions will be skipped due to GTIDs auto skip
    feature.
  */
  if (global_gtid_mode.get() == Gtid_mode::ON) {
    rli->mts_recovery_group_cnt = 0;
    return false;
  }

  /*
    Save relay log position to compare with worker's position.
  */
  LOG_INDEX_COORD cp;
  cp = applier_info->get_consensus_apply_index();

  /*
    Gathers information on valuable workers and stores it in
    above_lwm_jobs in asc ordered by the master binlog coordinates.
  */
  Prealloced_array<Slave_job_group, 16> above_lwm_jobs(
      PSI_NOT_INSTRUMENTED);
  above_lwm_jobs.reserve(rli->recovery_parallel_workers);

  /*
    When info tables are used and autocommit= 0 we force a new
    transaction start to avoid table access deadlocks when START SLAVE
    is executed after STOP SLAVE with MTS enabled.
  */
  if (is_autocommit_off_and_infotables(thd))
    if (trans_begin(thd)) goto err;

  for (uint id = 0; id < rli->recovery_parallel_workers; id++) {
    Slave_worker *worker =
        Rpl_info_factory::create_worker(INFO_REPOSITORY_TABLE, id, rli, true);
    Consensus_applier_worker *consensus_worker =
        Consensus_info_factory::create_consensus_applier_woker(id, true);

    if (!worker || !consensus_worker) {
      if (is_autocommit_off_and_infotables(thd)) trans_rollback(thd);
      goto err;
    }

    LOG_INDEX_COORD w_last;
    w_last = consensus_worker->get_consensus_apply_index();

    if (mts_event_coord_cmp(w_last, cp) > 0) {
      /*
        Inserts information into a dynamic array for further processing.
        The jobs/workers are ordered by the last checkpoint positions
        workers have seen.
      */
      job_worker.worker = worker;
      job_worker.checkpoint_log_pos = worker->checkpoint_relay_log_pos;
      job_worker.checkpoint_log_name = worker->checkpoint_relay_log_name;
      job_worker.consensus_index = consensus_worker->get_consensus_apply_index();

      above_lwm_jobs.push_back(job_worker);
    } else {
      /*
        Deletes the worker because its jobs are included in the latest
        checkpoint.
      */
      delete worker;
    }
    delete consensus_worker;
  }

  /*
    When info tables are used and autocommit= 0 we force transaction
    commit to avoid table access deadlocks when START SLAVE is executed
    after STOP SLAVE with MTS enabled.
  */
  if (is_autocommit_off_and_infotables(thd))
    if (trans_commit(thd)) goto err;

  /*
    In what follows, the group Recovery Bitmap is constructed.

     seek(lwm);

     while(w= next(above_lwm_w))
       do
         read G
         if G == w->last_comm
           w.B << group_cnt++;
           RB |= w.B;
            break;
         else
           group_cnt++;
        while(!eof);
        continue;
  */
  assert(!rli->recovery_groups_inited);

  if (!above_lwm_jobs.empty()) {
    bitmap_init(groups, nullptr, MTS_MAX_BITS_IN_GROUP);
    rli->recovery_groups_inited = true;
    bitmap_clear_all(groups);
  }
  rli->mts_recovery_group_cnt = 0;
  for (Slave_job_group *jg = above_lwm_jobs.begin(); jg != above_lwm_jobs.end();
       ++jg) {
    Slave_worker *w = jg->worker;
    LOG_INDEX_COORD w_last;
    w_last = jg->consensus_index;

    LogErr(INFORMATION_LEVEL,
           ER_CONSENSUS_MTS_GROUP_RECOVERY_APPLIER_INFO_FOR_WORKER, w->id,
           w->get_group_relay_log_name(), w->get_group_relay_log_pos(), w_last);

    recovery_group_cnt = 0;
    not_reached_commit = true;

    char log_name[FN_REFLEN];
    uint64 log_pos = 0;
    uint64 next_index = consensus_log_manager.get_next_trx_index(
        applier_info->get_consensus_apply_index(), false);
    if (consensus_log_manager.get_log_position(next_index, false, log_name,
                                                &log_pos)) {
      sql_print_error("Mts recover cannot find start index %llu.",
                      next_index);
      abort();
    }
    if (rli->relay_log.find_log_pos(&linfo, log_name, 1)) {
      sql_print_error("Error looking for %s.", log_name);
      goto err;
    }
    offset = log_pos;

    Relaylog_file_reader relaylog_file_reader(opt_replica_sql_verify_checksum);

    for (int checking = 0; not_reached_commit; checking++) {
      if (relaylog_file_reader.open(linfo.log_file_name, offset)) {
        LogErr(ERROR_LEVEL, ER_BINLOG_FILE_OPEN_FAILED,
                relaylog_file_reader.get_error_str());
        goto err;
      }

      bool in_large_trx = false;
      while (not_reached_commit &&
            (ev = relaylog_file_reader.read_event_object())) {
        assert(ev->is_valid());

        if (ev->get_type_code() == binary_log::ROTATE_EVENT ||
            ev->get_type_code() == binary_log::PREVIOUS_CONSENSUS_INDEX_LOG_EVENT ||
            ev->get_type_code() == binary_log::CONSENSUS_LOG_EVENT ||
            ev->get_type_code() == binary_log::CONSENSUS_CLUSTER_INFO_EVENT ||
            ev->get_type_code() == binary_log::CONSENSUS_EMPTY_EVENT ||
            ev->get_type_code() == binary_log::FORMAT_DESCRIPTION_EVENT ||
            ev->get_type_code() == binary_log::PREVIOUS_GTIDS_LOG_EVENT) {
          if (ev->get_type_code() == binary_log::CONSENSUS_LOG_EVENT) {
            Consensus_log_event *r_ev = (Consensus_log_event *)ev;
            if (r_ev->get_flag() & Consensus_log_event_flag::FLAG_LARGE_TRX) {
              if (!in_large_trx) {
                consensus_index = r_ev->get_index();
                in_large_trx = true;
              }
            } else if (r_ev->get_flag() &
                      Consensus_log_event_flag::FLAG_LARGE_TRX_END)
              in_large_trx = false;
            else
              consensus_index = r_ev->get_index();
          }
          delete ev;
          ev = nullptr;
          continue;
        }

        DBUG_PRINT(
            "mts",
            ("Event Recoverying relay log info "
            "group_mster_log_name %s, event_master_log_pos %llu type code %u.",
            linfo.log_file_name, ev->common_header->log_pos,
            ev->get_type_code()));

        if (ev->starts_group()) {
          flag_group_seen_begin = true;
        } else if ((ev->ends_group() || !flag_group_seen_begin) &&
                  !is_gtid_event(ev)) {
          int ret = 0;
          LOG_INDEX_COORD ev_coord = consensus_index;

          flag_group_seen_begin = false;
          recovery_group_cnt++;

          LogErr(INFORMATION_LEVEL, ER_RPL_MTA_GROUP_RECOVERY_APPLIER_METADATA,
                 linfo.log_file_name, ev->common_header->log_pos);
          if ((ret = mts_event_coord_cmp(ev_coord, w_last)) == 0) {
#ifndef NDEBUG
            for (uint i = 0; i <= w->worker_checkpoint_seqno; i++) {
              if (bitmap_is_set(&w->group_executed, i))
                DBUG_PRINT("mts", ("Bit %u is set.", i));
              else
                DBUG_PRINT("mts", ("Bit %u is not set.", i));
            }
#endif
            DBUG_PRINT("mts",
                      ("Doing a shift ini(%lu) end(%lu).",
                        (w->worker_checkpoint_seqno + 1) - recovery_group_cnt,
                        w->worker_checkpoint_seqno));

            for (uint i = (w->worker_checkpoint_seqno + 1) - recovery_group_cnt,
                      j = 0;
                i <= w->worker_checkpoint_seqno; i++, j++) {
              if (bitmap_is_set(&w->group_executed, i)) {
                DBUG_PRINT("mts", ("Setting bit %u.", j));
                bitmap_test_and_set(groups, j);
              }
            }
            not_reached_commit = false;
          } else {
            assert(ret < 0);
          }
        }
        delete ev;
        ev = nullptr;
      }

      relaylog_file_reader.close();
      offset = BIN_LOG_HEADER_SIZE;
      if (not_reached_commit && rli->relay_log.find_next_log(&linfo, true)) {
        LogErr(ERROR_LEVEL, ER_RPL_CANT_FIND_FOLLOWUP_FILE,
              linfo.log_file_name);
        goto err;
      }
    }

    rli->mts_recovery_group_cnt =
        (rli->mts_recovery_group_cnt < recovery_group_cnt
             ? recovery_group_cnt
             : rli->mts_recovery_group_cnt);
  }

  assert(!rli->recovery_groups_inited ||
         rli->mts_recovery_group_cnt <= groups->n_bits);

  goto end;
err:
  is_error = true;
end:

  for (Slave_job_group *jg = above_lwm_jobs.begin(); jg != above_lwm_jobs.end();
       ++jg) {
    delete jg->worker;
  }

  if (rli->mts_recovery_group_cnt == 0) rli->clear_mts_recovery_groups();

  return is_error;
}
