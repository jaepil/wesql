#include "consensus_binlog_recovery.h"
#include "consensus_binlog.h"
#include "consensus_log_manager.h"
#include "consensus_recovery_manager.h"
#include "system_variables.h"

#include "my_loglevel.h"
#include "sql/log.h"
#include "mysql/components/services/log_builtins.h"

#include "mysql/plugin.h"
#include "scope_guard.h"  // Scope_guard
#include "sql/binlog.h"
#include "sql/binlog/decompressing_event_object_istream.h"  // binlog::Decompressing_event_object_istream
#include "sql/binlog/recovery.h"         // binlog::Binlog_recovery
#include "sql/binlog_istream.h"
#include "sql/binlog_ostream.h"
#include "sql/log_event.h"
#include "sql/rpl_rli.h"
#include "sql/xa/recovery.h"
#include "sql/xa/xid_extract.h"  // xa::XID_extractor

static const int MIN_XID_LIST_SIZE = 128;
static const int MAX_XID_LIST_SIZE = 1024 * 128;

static int consensus_ha_recover(ConsensusRecoveryManager *recovery_manager,
                                Xid_commit_list *commit_list,
                                Xa_state_list::list *binlog_xid_map) {
  xarecover_st info;
  Xa_state_list *se_xa_list;
  Xa_state_list xa_list{*binlog_xid_map};
  DBUG_TRACE;
  info.commit_list = commit_list;
  info.dry_run = false;
  info.list = nullptr;

  // destruct se_xa_list when this function returns
  std::unique_ptr<MEM_ROOT> mem_root{nullptr};
  std::unique_ptr<Xa_state_list::allocator> map_alloc{nullptr};
  std::unique_ptr<Xa_state_list::list> se_xid_map{nullptr};
  std::unique_ptr<Xa_state_list> se_xids{nullptr};
  std::tie(mem_root, map_alloc, se_xid_map, se_xids) =
      Xa_state_list::new_instance();
  se_xa_list = se_xids.get();

  assert(total_ha_2pc > 1);

  if (info.commit_list) LogErr(SYSTEM_LEVEL, ER_XA_STARTING_RECOVERY);

  for (info.len = MAX_XID_LIST_SIZE;
       info.list == nullptr && info.len > MIN_XID_LIST_SIZE; info.len /= 2) {
    info.list = new (std::nothrow) XA_recover_txn[info.len];
  }
  if (!info.list) {
    LogErr(ERROR_LEVEL, ER_SERVER_OUTOFMEMORY,
           static_cast<int>(info.len * sizeof(XID)));
    return 1;
  }
  auto clean_up_guard = create_scope_guard([&] { delete[] info.list; });

  /* fetch the transactions which are in the prepared state from storage
   * engine to info.xa_list */
  info.xa_list = se_xa_list;
  if (plugin_foreach(nullptr, xa::recovery::recover_prepared_in_tc_one_ht,
                     MYSQL_STORAGE_ENGINE_PLUGIN, &info)) {
    return 1;
  }

  // reconstrunct xa_list and commit_list by se_xid_map and binlog_xid_map
  (void)recovery_manager->reconstruct_binlog_trx_list(
      *commit_list, xa_list, *se_xid_map, *binlog_xid_map);
  info.xa_list = &xa_list;

  if (plugin_foreach(nullptr, xa::recovery::recover_one_ht,
                     MYSQL_STORAGE_ENGINE_PLUGIN, &info)) {
    return 1;
  }

  if (info.found_foreign_xids)
    LogErr(WARNING_LEVEL, ER_XA_RECOVER_FOUND_XA_TRX, info.found_foreign_xids);
  if (info.commit_list) LogErr(SYSTEM_LEVEL, ER_XA_RECOVERY_DONE);

  return 0;
}

class Consensus_binlog_recovery : public binlog::Binlog_recovery {
 public:
  /**
    Class constructor.
   */
  Consensus_binlog_recovery(Binlog_file_reader &binlog_file_reader);
  virtual ~Consensus_binlog_recovery() = default;

  Consensus_binlog_recovery &consensus_recover(bool ha_recover);

 private:
  ConsensusRecoveryManager recovery_manager;
  bool begin_consensus{false};
  uint64 current_term{0};
  uint64 current_index{0};
  uint64 current_length{0};
  uint64 valid_index{0};
  uint current_flag{0};
  uint64 start_pos{BIN_LOG_HEADER_SIZE};
  uint64 end_pos{BIN_LOG_HEADER_SIZE};
  uint32 current_ev_ts{0};

  void after_process_xa_prepare_event(XA_prepare_log_event const &ev);
  void after_process_xid_event(Xid_log_event const &ev);
  void after_process_query_event(Query_log_event const &ev);
  void after_process_atomic_ddl(Query_log_event const &ev);
  void after_process_xa_commit_or_rollback(std::string const &query);

  void process_consensus_log_event(Consensus_log_event const &ev);
  void process_previous_consensus_index_event(
      Previous_consensus_index_log_event const &ev);
};

Consensus_binlog_recovery::Consensus_binlog_recovery(
    Binlog_file_reader &binlog_file_reader)
    : binlog::Binlog_recovery(binlog_file_reader) {}

void Consensus_binlog_recovery::process_consensus_log_event(
    Consensus_log_event const &ev) {
  DBUG_TRACE;
  if (this->current_index > ev.get_index()) {
    this->m_is_malformed = true;
    this->m_failure_message.assign(
        "Consensus_log_event log index out of order");
    return;
  }

  if (this->end_pos < this->start_pos) {
    this->m_is_malformed = true;
    this->m_failure_message.assign("Consensus_log_event log struct broken");
    return;
  }

  if (this->current_term > 0 && ev.get_term() > this->current_term) {
    this->m_internal_xids.clear();
  }

  this->current_index = ev.get_index();
  this->current_term = ev.get_term();
  this->current_length = ev.get_length();
  this->current_flag = ev.get_flag();
  this->current_ev_ts = ev.common_header->when.tv_sec;
  this->start_pos = this->m_reader.position();
  this->end_pos = this->m_reader.position();

  this->begin_consensus = true;
}

void Consensus_binlog_recovery::process_previous_consensus_index_event(
    Previous_consensus_index_log_event const &ev) {
  DBUG_TRACE;
  this->current_index = ev.get_index() - 1;
  this->current_ev_ts = ev.common_header->when.tv_sec;
}

void Consensus_binlog_recovery::after_process_query_event(
    Query_log_event const &ev) {
  std::string query{ev.query};
  DBUG_TRACE;

  if (is_atomic_ddl_event(&ev))
    this->after_process_atomic_ddl(ev);

  else if (query.find("XA COMMIT") == 0)
    this->after_process_xa_commit_or_rollback(query);

  else if (query.find("XA ROLLBACK") == 0)
    this->after_process_xa_commit_or_rollback(query);
}

void Consensus_binlog_recovery::after_process_xid_event(
    Xid_log_event const &ev) {
  recovery_manager.add_trx_to_binlog_map(ev.xid, this->current_index,
                                         this->current_term);
}

void Consensus_binlog_recovery::after_process_xa_prepare_event(
    XA_prepare_log_event const &ev) {
  XID xid;
  DBUG_TRACE;
  xid = ev.get_xid();
  recovery_manager.add_trx_to_binlog_xa_map(
      &xid, false, this->current_index,
      this->current_term);
}

void Consensus_binlog_recovery::after_process_atomic_ddl(
    Query_log_event const &ev) {
  DBUG_TRACE;
  recovery_manager.add_trx_to_binlog_map(ev.ddl_xid, this->current_index,
                                         this->current_term);
}

void Consensus_binlog_recovery::after_process_xa_commit_or_rollback(
    std::string const &query) {
  DBUG_TRACE;
  xa::XID_extractor tokenizer{query, 1};

  assert(tokenizer.size() > 0);

  recovery_manager.add_trx_to_binlog_xa_map(
      &tokenizer[0], true, this->current_index, this->current_term);
}

Consensus_binlog_recovery &Consensus_binlog_recovery::consensus_recover(
    bool ha_recover) {
  bool pos_set_by_previous = false;
  bool enable_rotate = true;
  uint64 start_index = 0;
  DBUG_TRACE;
  binlog::Decompressing_event_object_istream istream{this->m_reader};
  std::shared_ptr<Log_event> ev;
  this->m_valid_pos = this->m_reader.position();

  while (istream >> ev) {
    switch (ev->get_type_code()) {
      case binary_log::QUERY_EVENT: {
        if (ha_recover) {
          this->process_query_event(dynamic_cast<Query_log_event &>(*ev));
          this->after_process_query_event(dynamic_cast<Query_log_event &>(*ev));
        }
        break;
      }
      case binary_log::XID_EVENT: {
        if (ha_recover) {
          this->process_xid_event(dynamic_cast<Xid_log_event &>(*ev));
          this->after_process_xid_event(dynamic_cast<Xid_log_event &>(*ev));
        }
        break;
      }
      case binary_log::XA_PREPARE_LOG_EVENT: {
        if (ha_recover) {
          this->process_xa_prepare_event(
              dynamic_cast<XA_prepare_log_event &>(*ev));
          this->after_process_xa_prepare_event(
              dynamic_cast<XA_prepare_log_event &>(*ev));
        }
        break;
      }
      case binary_log::CONSENSUS_LOG_EVENT: {
        this->process_consensus_log_event(
            dynamic_cast<Consensus_log_event &>(*ev));
        this->begin_consensus = true;
        if (start_index > 0) {
          update_pos_map_by_start_index(
              start_index, dynamic_cast<Consensus_log_event *>(ev.get()),
              this->m_reader.event_start_pos(),
              this->m_reader.position() +
                  (dynamic_cast<Consensus_log_event *>(ev.get()))->get_length(),
              pos_set_by_previous);
        }
        break;
      }
      case binary_log::PREVIOUS_CONSENSUS_INDEX_LOG_EVENT: {
          this->process_previous_consensus_index_event(
              dynamic_cast<Previous_consensus_index_log_event &>(*ev));
          start_index = this->current_index + 1;
        break;
      }
      default: {
        break;
      }
    }

    if (!this->m_is_malformed && !ev->is_control_event()) {
      this->end_pos = this->m_reader.position();
      /* find a integrated consensus log */
      if (this->begin_consensus && this->end_pos > this->start_pos &&
          this->end_pos - this->start_pos == this->current_length &&
          !(this->current_flag & Consensus_log_event_flag::FLAG_BLOB)) {
        enable_rotate =
            !(this->current_flag & Consensus_log_event_flag::FLAG_LARGE_TRX);
        this->begin_consensus = false;
      }
    }

    if (!this->m_is_malformed && !this->begin_consensus && !is_gtid_event(ev.get()) &&
        !is_session_control_event(ev.get())) {
      this->valid_index = this->current_index;
      this->m_valid_pos = this->m_reader.position();
    }

    this->m_is_malformed = istream.has_error() || this->m_is_malformed;
    if (this->m_is_malformed) break;
  }

  if (istream.has_error()) {
    using Status_t = binlog::Decompressing_event_object_istream::Status_t;
    switch (istream.get_status()) {
      case Status_t::corrupted:
      case Status_t::out_of_memory:
      case Status_t::exceeds_max_size:
        // @todo Uncomment this to fix BUG#34828252
        // this->m_is_malformed = true;
        // this->m_failure_message.assign(istream.get_error_str());
        // break;
      case Status_t::success:
      case Status_t::end:
      case Status_t::truncated:
        // not malformed, just truncated
        break;
    }
  }

  assert(total_ha_2pc > 1);
  if (!this->m_is_malformed && ha_recover) {
    this->m_no_engine_recovery = consensus_ha_recover(
        &recovery_manager, &this->m_internal_xids, &this->m_external_xids);
    if (this->m_no_engine_recovery) {
      this->m_failure_message.assign("Recovery failed in storage engines");
    }
  }

  if (this->has_failures()) return (*this);

  if (ha_recover) {
    consensus_log_manager.set_recovery_index_hwl(
        recovery_manager.max_committed_index > 0
            ? recovery_manager.max_committed_index
            : start_index - 1);
  } else {
    consensus_log_manager.set_recovery_index_hwl(this->valid_index);
  }

  consensus_log_manager.set_cache_index(this->valid_index);
  consensus_log_manager.set_sync_index(this->valid_index);
  consensus_log_manager.set_current_index(this->valid_index + 1);
  consensus_log_manager.set_enable_rotate(enable_rotate);
  consensus_log_manager.set_event_timestamp(this->current_ev_ts);

  LogErr(SYSTEM_LEVEL, ER_CONSENSUS_LOG_RECOVERY_FINISHED,
         recovery_manager.max_committed_index, this->valid_index,
         recovery_manager.max_committed_index > 0
             ? recovery_manager.max_committed_index
             : start_index - 1);

  return (*this);
}

uint64 get_applier_start_index() {
  uint64 first_index;
  uint64 recover_status;
  ulonglong start_apply_index;
  uint64 next_index;
  DBUG_TRACE;
  Consensus_info *consensus_info = consensus_log_manager.get_consensus_info();
  Consensus_applier_info *applier_info =
      consensus_log_manager.get_applier_info();

  first_index = consensus_log_manager.get_log_file_index()->get_first_index();
  recover_status = consensus_info->get_recover_status();
  assert(consensus_log_manager.get_status() == BINLOG_WORKING);
  start_apply_index = consensus_info->get_start_apply_index();
  uint64 last_applied_index = consensus_log_manager.get_recovery_index_hwl();

  LogErr(SYSTEM_LEVEL, ER_CONSENSUS_RECOVERY_APPLIED_INDEX_START,
         recover_status, first_index, start_apply_index, last_applied_index,
         applier_info->get_consensus_apply_index());

  if (recover_status == BINLOG_WORKING) {
    if (opt_cluster_recover_from_backup || start_apply_index == 0) {
      next_index = last_applied_index < first_index
                       ? first_index
                       : consensus_log_manager.get_next_trx_index(
                             last_applied_index, false);
      consensus_info->set_start_apply_index(last_applied_index);
      if (consensus_info->flush_info(true, true)) {
        next_index = 0;
        LogErr(ERROR_LEVEL, ER_CONSENSUS_FATAL_ERROR,
               "Error flush consensus info set start apply index");
      }
    } else {
      assert(start_apply_index >= first_index);
      next_index =
          consensus_log_manager.get_next_trx_index(start_apply_index, false);
    }
  } else {
    uint64 consensus_recovery_index =
        (opt_cluster_recover_from_snapshot &&
         applier_info->recovery_parallel_workers > 0)
            ? applier_info->get_mts_consensus_hwm_index()
            : applier_info->get_consensus_apply_index();
    next_index = consensus_recovery_index < first_index
                     ? first_index
                     : consensus_log_manager.get_next_trx_index(
                           consensus_recovery_index, false);
  }

  LogErr(SYSTEM_LEVEL, ER_CONSENSUS_RECOVERY_APPLIED_INDEX_STOP, next_index);

  return next_index;
}

/* init executed_gtids/lost_gtids/gtids_only_in_table after consenus module
 * setup */
int gtid_init_after_consensus_setup(uint64 last_index) {
  DBUG_TRACE;
  /*
    Initialize GLOBAL.GTID_EXECUTED and GLOBAL.GTID_PURGED from
    gtid_executed table and binlog files during server startup.
    */
  Gtid_set *executed_gtids =
      const_cast<Gtid_set *>(gtid_state->get_executed_gtids());
  Gtid_set *lost_gtids = const_cast<Gtid_set *>(gtid_state->get_lost_gtids());
  Gtid_set *gtids_only_in_table =
      const_cast<Gtid_set *>(gtid_state->get_gtids_only_in_table());

  Gtid_set purged_gtids_from_binlog(global_sid_map, global_sid_lock);
  Gtid_set gtids_in_binlog(global_sid_map, global_sid_lock);
  Gtid_set gtids_in_binlog_not_in_table(global_sid_map, global_sid_lock);

  uint64 recover_status =
      consensus_log_manager.get_consensus_info()->get_recover_status();

  MYSQL_BIN_LOG *log = consensus_log_manager.get_binlog();
  assert(log != NULL);
  if (log->consensus_init_gtid_sets(
          &gtids_in_binlog, &purged_gtids_from_binlog,
          opt_source_verify_checksum, true,
          consensus_log_manager.get_start_without_log(), last_index))
    return -1;

  global_sid_lock->wrlock();

  /* Add unsaved set of GTIDs into gtid_executed table */
  if (recover_status == BINLOG_WORKING &&  // just for leader
      !gtids_in_binlog.is_empty() &&
      !gtids_in_binlog.is_subset(executed_gtids)) {
    gtids_in_binlog_not_in_table.add_gtid_set(&gtids_in_binlog);
    if (!executed_gtids->is_empty()) {
      gtids_in_binlog_not_in_table.remove_gtid_set(executed_gtids);
    }
    if (gtid_state->save(&gtids_in_binlog_not_in_table) == -1) {
      global_sid_lock->unlock();
      return -1;
    }
    executed_gtids->add_gtid_set(&gtids_in_binlog_not_in_table);
  }

  /* gtids_only_in_table= executed_gtids - gtids_in_binlog */
  if (gtids_only_in_table->add_gtid_set(executed_gtids) != RETURN_STATUS_OK) {
    global_sid_lock->unlock();
    return -1;
  }
  gtids_only_in_table->remove_gtid_set(&gtids_in_binlog);
  /*
    lost_gtids = executed_gtids -
                 (gtids_in_binlog - purged_gtids_from_binlog)
               = gtids_only_in_table + purged_gtids_from_binlog;
  */
  assert(lost_gtids->is_empty());
  if (lost_gtids->add_gtid_set(gtids_only_in_table) != RETURN_STATUS_OK ||
      lost_gtids->add_gtid_set(&purged_gtids_from_binlog) != RETURN_STATUS_OK) {
    global_sid_lock->unlock();
    return -1;
  }

  if (consensus_log_manager.get_start_without_log()) {
    /*
      Write the previous set of gtids at this point because during
      the creation of the binary log this is not done as we cannot
      move the init_gtid_sets() to a place before opening the binary
      log. This requires some investigation.

      /Alfranio
    */
    Previous_gtids_log_event prev_gtids_ev(&gtids_in_binlog);

    global_sid_lock->unlock();

    (prev_gtids_ev.common_footer)->checksum_alg =
        static_cast<enum_binlog_checksum_alg>(binlog_checksum_options);

    if (log->write_event_to_binlog_and_sync(&prev_gtids_ev)) return -1;
  } else {
    global_sid_lock->unlock();
  }

  return 0;
}

int consensus_binlog_recovery(MYSQL_BIN_LOG *binlog) {
  LOG_INFO log_info;
  Log_event *ev = nullptr;
  int error = 0;
  bool should_execute_ha_recover = false;
  char log_name[FN_REFLEN];
  my_off_t valid_pos = 0;
  my_off_t binlog_size = 0;
  Binlog_file_reader binlog_file_reader(opt_source_verify_checksum);

  DBUG_TRACE;

  if ((error = binlog->find_log_pos(&log_info, NullS,
                                    true /*need_lock_index=true*/))) {
    if (error != LOG_INFO_EOF)
      LogErr(ERROR_LEVEL, ER_BINLOG_CANT_FIND_LOG_IN_INDEX, error);
    else {
      error = 0;
      should_execute_ha_recover = true;
    }
    goto err;
  }

  do {
    strmake(log_name, log_info.log_file_name, sizeof(log_name) - 1);
  } while (!(
      error = binlog->find_next_log(&log_info, true /*need_lock_index=true*/)));

  if (error != LOG_INFO_EOF) {
    LogErr(ERROR_LEVEL, ER_BINLOG_CANT_FIND_LOG_IN_INDEX, error);
    goto err;
  }

  if (binlog_file_reader.open(log_name)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_FILE_OPEN_FAILED,
           binlog_file_reader.get_error_str());
    return 1;
  }

  /*
    If the binary log was not properly closed it means that the server
    may have crashed. In that case, we need to call
    binlog::Binlog_recovery::recover()
    to:

      a) collect logged XIDs;
      b) complete the 2PC of the pending XIDs;
      c) collect the last valid position.

    Therefore, we do need to iterate over the binary log, even if
    total_ha_2pc == 1, to find the last valid group of events written.
    Later we will take this value and truncate the log if need be.
  */
  if ((ev = binlog_file_reader.read_event_object()) &&
      ev->get_type_code() == binary_log::FORMAT_DESCRIPTION_EVENT) {
    LogErr(INFORMATION_LEVEL, ER_BINLOG_RECOVERING_AFTER_CRASH_USING, log_name);
    bool ha_recover =
        (ev->common_header->flags & LOG_EVENT_BINLOG_IN_USE_F ||
         DBUG_EVALUATE_IF("eval_force_bin_log_recovery", true, false));

    Consensus_binlog_recovery bl_recovery{binlog_file_reader};
    error = bl_recovery                         //
                .consensus_recover(ha_recover)  //
                .has_failures();
    valid_pos = bl_recovery.get_valid_pos();
    binlog_size = binlog_file_reader.ifile()->length();
    if (error) {
      if (bl_recovery.is_binlog_malformed())
        LogErr(ERROR_LEVEL, ER_BINLOG_CRASH_RECOVERY_MALFORMED_LOG, log_name,
               valid_pos, binlog_file_reader.position(),
               bl_recovery.get_failure_message().data());
      if (bl_recovery.has_engine_recovery_failed())
        LogErr(ERROR_LEVEL, ER_BINLOG_CRASH_RECOVERY_ERROR_RETURNED_SE);
    }
    should_execute_ha_recover = !ha_recover;
  } else
    should_execute_ha_recover = true;

  delete ev;

  /* Trim the crashed binlog file to last valid transaction
    or event (non-transaction) base on valid_pos. */
  if (!error && valid_pos > 0) {
    error = truncate_binlog_file_to_valid_pos(log_name, valid_pos, binlog_size);
    LogErr(INFORMATION_LEVEL, ER_BINLOG_CRASHED_BINLOG_TRIMMED, log_name,
           binlog_size, valid_pos, valid_pos);
  }

err:
  if (should_execute_ha_recover) {
    error = ha_recover();
    if (error) LogErr(ERROR_LEVEL, ER_BINLOG_CRASH_RECOVERY_ERROR_RETURNED_SE);
  }

  return error;
}
