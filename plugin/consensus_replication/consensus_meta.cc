
#include "consensus_meta.h"
#include "consensus_state_process.h"
#include "plugin.h"
#include "plugin_psi.h"
#include "system_variables.h"

#include "mysqld_error.h"
#include "sql/mysqld.h"

#include "my_loglevel.h"
#include "mysql/components/services/log_builtins.h"
#include "sql/log.h"

#include "sql/consensus/consensus_info_factory.h"
#include "sql/rpl_info_factory.h"

ConsensusMeta consensus_meta;

ConsensusMeta::ConsensusMeta()
    : inited(false),
      consensus_info(nullptr),
      consensus_applier_info(nullptr),
      already_set_start_index(false),
      already_set_start_term(false) {}

ConsensusMeta::~ConsensusMeta() {}

int ConsensusMeta::init() {
  DBUG_TRACE;
  Consensus_info_factory::init_consensus_repo_metadata();
  consensus_info = Consensus_info_factory::create_consensus_info();
  Consensus_info_factory::init_consensus_applier_repo_metadata();
  consensus_applier_info =
      Consensus_info_factory::create_consensus_applier_info();
  Consensus_info_factory::init_consensus_applier_worker_repo_metadata();
  if (!consensus_info || !consensus_applier_info) return 1;

  inited = true;
  return 0;
}

int ConsensusMeta::init_consensus_info() {
  DBUG_TRACE;
  if (consensus_info->init_info()) {
    return -1;
  }

  if (consensus_applier_info->init_info()) {
    return -1;
  }

  return 0;
}

int ConsensusMeta::change_meta_if_needed() {
  DBUG_TRACE;
  Consensus_info *consensus_info = get_consensus_info();
  if (opt_cluster_force_change_meta || opt_cluster_force_reset_meta) {
    if (opt_cluster_id) consensus_info->set_cluster_id(opt_cluster_id);

    if (opt_cluster_current_term)
      consensus_info->set_current_term(opt_cluster_current_term);
    else if (opt_cluster_force_reset_meta)
      consensus_info->set_current_term(1);

    if (opt_cluster_force_recover_index || opt_cluster_force_reset_meta) {
      consensus_info->set_recover_status(
          Consensus_Log_System_Status::RELAY_LOG_WORKING);
      consensus_info->set_last_leader_term(0);
      consensus_info->set_start_apply_index(opt_cluster_force_recover_index);
    }

    if (!opt_cluster_info) {
      LogPluginErr(ERROR_LEVEL, ER_CONSENSUS_CHANGE_META_ERROR,
                   "consensus_replication_cluster_info "
                   "must be set when the server is "
                   "running with --initialize(-insecure)");
      return -1;
    }
    consensus_info->set_cluster_info(std::string(opt_cluster_info));
    consensus_info->flush_info(true, true);

    LogPluginErr(SYSTEM_LEVEL, ER_CONSENSUS_CHANGE_META_LOG);
  }

  opt_cluster_id = get_consensus_info()->get_cluster_id();

  return 0;
}

int ConsensusMeta::update_consensus_info() {
  int error = 0;
  DBUG_TRACE;

  if (!opt_initialize) {
    error = change_meta_if_needed();
  } else {
    Consensus_info *consensus_info = get_consensus_info();

    if (opt_cluster_id) consensus_info->set_cluster_id(opt_cluster_id);

    if (!opt_cluster_info) {
      LogPluginErr(ERROR_LEVEL, ER_CONSENSUS_CHANGE_META_ERROR,
                   "consensus_replication_cluster_info "
                   "must be set when the server is "
                   "running with --initialize(-insecure)");
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

int ConsensusMeta::set_start_apply_index_if_need(uint64 consensus_index) {
  DBUG_TRACE;

  if (opt_cluster_log_type_instance) return 0;

  mysql_rwlock_rdlock(consensus_state_process.get_consensuslog_status_lock());
  if (!already_set_start_index &&
      consensus_state_process.get_status() ==
          Consensus_Log_System_Status::BINLOG_WORKING) {
    consensus_info->set_start_apply_index(consensus_index);
    if (consensus_info->flush_info(true, true)) {
      mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());
      return 1;
    }
    already_set_start_index = true;
  }
  mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());

  return 0;
}

int ConsensusMeta::set_start_apply_term_if_need(uint64 consensus_term) {
  DBUG_TRACE;

  if (opt_cluster_log_type_instance) return 0;

  mysql_rwlock_rdlock(consensus_state_process.get_consensuslog_status_lock());
  if (!already_set_start_term &&
      consensus_state_process.get_status() ==
          Consensus_Log_System_Status::BINLOG_WORKING) {
    consensus_info->set_last_leader_term(consensus_term);
    if (consensus_info->flush_info(true, true)) {
      mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());
      return 1;
    }
    already_set_start_term = true;
  }
  mysql_rwlock_unlock(consensus_state_process.get_consensuslog_status_lock());

  return 0;
}

int ConsensusMeta::cleanup() {
  DBUG_TRACE;

  if (inited) {
    consensus_info->end_info();
    delete consensus_info;
    delete consensus_applier_info;
  }
  return 0;
}