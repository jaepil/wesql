#include <stddef.h>

#include "consensus_applier.h"
#include "consensus_log_manager.h"
#include "observer_applier.h"
#include "plugin.h"

#include "sql/consensus/consensus_applier_info.h"
#include "sql/consensus/consensus_info_factory.h"
#include "sql/log_event.h"
#include "sql/rpl_applier_reader.h"
#include "sql/rpl_mi.h"   // Master_info
#include "sql/rpl_msr.h"  // channel_map
#include "sql/rpl_rli.h"
#include "sql/rpl_rli_pdb.h"

static int consensus_applier_rli_init_info(Binlog_applier_param *param,
                                      bool force_retriever_gtid,
                                      bool &exit_init) {
  exit_init = false;

  if (!plugin_is_consensus_replication_enabled()) return 0;

  mysql_mutex_assert_owner(&param->rli->data_lock);

  DBUG_TRACE;
  Relay_log_info *rli = param->rli;

  if (channel_map.is_consensus_replication_channel_name(rli->get_channel())) {
    exit_init = true;
    return rli->cli_init_info(force_retriever_gtid);
  }

  return 0;
}

static int consensus_applier_rli_end_info(Binlog_applier_param *param,
                                      bool &exit_end) {
  exit_end = false;

  if (!plugin_is_consensus_replication_enabled()) return 0;

  DBUG_TRACE;
  Relay_log_info *rli = param->rli;

  if (channel_map.is_consensus_replication_channel_name(rli->get_channel())) {
    exit_end = true;
    rli->cli_end_info();
  }

  return 0;
}

static int consensus_applier_before_start(Binlog_applier_param *param,
                                          ulong n_workers) {
  if (opt_initialize || !plugin_is_consensus_replication_enabled()) return 0;

  DBUG_TRACE;
  int error = 0;
  Relay_log_info *rli = param->rli;

  if (channel_map.is_consensus_replication_channel_name(rli->get_channel())) {
    THD *thd = rli->info_thd;
    thd->consensus_context.consensus_replication_applier = true;
    thd->variables.option_bits &= ~OPTION_BIN_LOG;

    // Coordinator
    if (!is_mts_worker(thd)) {
      mysql_mutex_assert_owner(&param->rli->run_lock);

      error = calculate_consensus_apply_start_pos(rli);

      if (!error && n_workers > 0) {
        Consensus_applier_info *applier_info =
            consensus_log_manager.get_applier_info();
        error = create_applier_workers(param->rli, applier_info, n_workers);
      }
    }
  }
  return error;
}

static int consensus_applier_reader_before_open(
    Binlog_applier_param *param, Rpl_applier_reader *applier_reader) {
  if (!plugin_is_consensus_replication_enabled()) return 0;

  mysql_mutex_assert_not_owner(&param->rli->data_lock);

  DBUG_TRACE;
  THD *thd = param->rli->info_thd;

  if (thd->consensus_context.consensus_replication_applier) {
    LOG_INFO *linfo = applier_reader->get_log_info();
    linfo->thread_id = thd->thread_id();
    param->rli->relay_log.register_log_info(linfo);
  }

  return 0;
}

static int consensus_applier_reader_before_read_event(
    Binlog_applier_param *param, Rpl_applier_reader *applier_reader) {
  if (!plugin_is_consensus_replication_enabled()) return 0;

  DBUG_TRACE;
  THD *thd = param->rli->info_thd;

  assert(!is_mts_worker(thd));
  mysql_mutex_assert_owner(&param->rli->data_lock);

  if (thd->consensus_context.consensus_replication_applier) {
    if (applier_reader->is_reading_active_log() &&
        param->rli->is_relay_log_truncated()) {
      if (applier_reader->reload_active_log_end_pos()) return 1;
    }
  }

  return 0;
}

static int consensus_applier_before_read_next_event(Binlog_applier_param *param,
                                                    bool &applier_stop) {
  if (!plugin_is_consensus_replication_enabled()) return 0;

  DBUG_TRACE;
  Relay_log_info *rli = param->rli;
  THD *thd = param->rli->info_thd;

  assert(!is_mts_worker(thd));
  mysql_mutex_assert_not_owner(&param->rli->data_lock);

  if (thd->consensus_context.consensus_replication_applier) {
    applier_stop = check_exec_consensus_log_end_condition(rli);
  }

  return 0;
}

static int consensus_applier_before_apply_event(Binlog_applier_param *param,
                                                Log_event *ev) {
  if (!plugin_is_consensus_replication_enabled()) return 0;

  DBUG_TRACE;
  THD *thd = param->rli->info_thd;

  assert(!is_mts_worker(thd));
  mysql_mutex_assert_owner(&param->rli->data_lock);

  if (thd->consensus_context.consensus_replication_applier) {
    update_consensus_apply_pos(param->rli, ev);
  }
  return 0;
}

static int consensus_applier_on_mts_groups_assigned(Binlog_applier_param *param,
                                                    Slave_job_group *ptr_g) {
  if (!plugin_is_consensus_replication_enabled()) return 0;

  DBUG_TRACE;
  Relay_log_info *rli = param->rli;

  assert(!is_mts_worker(rli->info_thd));

  if (rli->info_thd->consensus_context.consensus_replication_applier) {
    ptr_g->reset_consensus_index(consensus_log_manager.get_apply_index());
  }

  return 0;
}

static int consensus_applier_on_stmt_done(Binlog_applier_param *param) {
  if (!plugin_is_consensus_replication_enabled()) return 0;

  DBUG_TRACE;
  Relay_log_info *rli = param->rli;
  int error = 0;

  assert(!is_mts_worker(rli->info_thd));
  mysql_mutex_assert_not_owner(&param->rli->data_lock);

  if (rli->info_thd->consensus_context.consensus_replication_applier &&
      (rli->is_parallel_exec() || !rli->is_in_group()) &&
      rli->mts_group_status == Relay_log_info::MTS_NOT_IN_GROUP) {
    uint64 event_consensus_index = consensus_log_manager.get_apply_index();
    Consensus_applier_info *applier_info =
        consensus_log_manager.get_applier_info();
    applier_info->set_consensus_apply_index(event_consensus_index);
    error = applier_info->flush_info(true, true);
  }

  return error;
}

static int consensus_applier_on_checkpoint_routine(
    Binlog_applier_param *param) {
  if (!plugin_is_consensus_replication_enabled()) return 0;

  DBUG_TRACE;
  Relay_log_info *rli = param->rli;

  assert(!is_mts_worker(rli->info_thd));
  mysql_mutex_assert_owner(&param->rli->data_lock);

  if (rli->info_thd->consensus_context.consensus_replication_applier) {
    Consensus_applier_info *applier_info =
        consensus_log_manager.get_applier_info();
    applier_info->set_consensus_apply_index(rli->gaq->lwm.consensus_index);
    applier_info->flush_info(true, true);
    update_consensus_applied_index(rli->gaq->lwm.consensus_index);
  }

  return 0;
}

static int consensus_applier_before_commit(Binlog_applier_param *param) {
  if (!plugin_is_consensus_replication_enabled()) return 0;

  DBUG_TRACE;
  THD *thd = param->rli->info_thd;
  int error = 0;

  assert(!is_mts_worker(thd));
  mysql_mutex_assert_owner(&param->rli->data_lock);

  if (thd->consensus_context.consensus_replication_applier &&
      !thd->get_transaction()->xid_state()->check_in_xa(false)) {
    Consensus_applier_info *applier_info =
        consensus_log_manager.get_applier_info();
    error = applier_info->commit_positions(consensus_log_manager.get_apply_index());
  }

  return error;
}

static int consensus_applier_after_commit(Binlog_applier_param *param,
                                          bool on_rollback) {
  if (!plugin_is_consensus_replication_enabled()) return 0;

  DBUG_TRACE;
  THD *thd = param->rli->info_thd;
  int error = 0;

  assert(!is_mts_worker(thd));
  mysql_mutex_assert_owner(&param->rli->data_lock);

  if (thd->consensus_context.consensus_replication_applier && on_rollback) {
    Consensus_applier_info *applier_info =
        consensus_log_manager.get_applier_info();
    error = applier_info->rollback_positions();
  }
  return error;
}

static int consensus_applier_on_commit_positions(Binlog_applier_param *param,
                                                 Slave_job_group *ptr_g) {
  if (!plugin_is_consensus_replication_enabled()) return 0;

  DBUG_TRACE;
  THD *thd = param->rli->info_thd;
  int error = 0;

  if (thd->consensus_context.consensus_replication_applier) {
    Consensus_applier_info *applier_info =
        consensus_log_manager.get_applier_info();
    if (!ptr_g) {
      mysql_mutex_assert_owner(&param->rli->data_lock);
      error = applier_info->commit_positions(
          consensus_log_manager.get_apply_index());
    } else {
      Consensus_applier_worker *applier_worker =
          applier_info->get_worker(ptr_g->worker_id);
      if (applier_worker != nullptr) {
        error = applier_worker->commit_positions(ptr_g->consensus_index);
      }
    }
  }
  return error;
}

static int consensus_applier_on_mts_recovery_groups(
    Binlog_applier_param *param) {
  if (!plugin_is_consensus_replication_enabled()) return 0;

  DBUG_TRACE;
  int error = 0;

  if (channel_map.is_consensus_replication_channel_name(param->rli->get_channel())) {
    error = applier_mts_recovery_groups(param->rli);
  }

  return error;
}

static int consensus_applier_on_mts_finalize_recovery(
    Binlog_applier_param *param) {
  if (!plugin_is_consensus_replication_enabled()) return 0;

  DBUG_TRACE;
  int error = 0;

  if (channel_map.is_consensus_replication_channel_name(param->rli->get_channel())) {
    Consensus_applier_info *applier_info =
        consensus_log_manager.get_applier_info();
    if ((error = applier_info->mts_finalize_recovery())) {
      Consensus_info_factory::reset_consensus_applier_workers(applier_info);
    } else {
      error = applier_info->flush_info(true, true);
    }
  }

  return error;
}

static int consensus_applier_reader_before_close(
    Binlog_applier_param *param, Rpl_applier_reader *applier_reader) {
  if (!plugin_is_consensus_replication_enabled()) {
    return 0;
  }

  DBUG_TRACE;
  THD *thd = param->rli->info_thd;

  assert(!is_mts_worker(thd));

  if (thd->consensus_context.consensus_replication_applier) {
    param->rli->relay_log.unregister_log_info(applier_reader->get_log_info());
  }
  return 0;
}

static int consensus_applier_after_stop(Binlog_applier_param *param) {
  if (!plugin_is_consensus_replication_enabled()) {
    return 0;
  }

  THD *thd = param->rli->info_thd;
  if (thd->consensus_context.consensus_replication_applier) {
    if (!is_mts_worker(thd)) {
      Consensus_applier_info *applier_info =
          consensus_log_manager.get_applier_info();
      destory_applier_workers(param->rli, applier_info);
    }
  }

  return 0;
}

Binlog_applier_observer cr_binlog_applier_observer = {
    sizeof(Binlog_manager_observer),

    consensus_applier_rli_init_info,
    consensus_applier_rli_end_info,
    consensus_applier_before_start,
    consensus_applier_on_mts_recovery_groups,
    consensus_applier_on_mts_finalize_recovery,
    consensus_applier_after_stop,
    consensus_applier_before_read_next_event,
    consensus_applier_before_apply_event,
    consensus_applier_on_mts_groups_assigned,
    consensus_applier_on_stmt_done,
    consensus_applier_on_commit_positions,
    consensus_applier_before_commit,
    consensus_applier_after_commit,
    consensus_applier_on_checkpoint_routine,
    consensus_applier_reader_before_open,
    consensus_applier_reader_before_read_event,
    consensus_applier_reader_before_close,
};