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

#ifndef CONSENSUS_LOG_MANAGER_INCLUDE
#define CONSENSUS_LOG_MANAGER_INCLUDE
#include <atomic>
#include <iterator>
#include <map>
#include <queue>
#include <vector>

#include "consensus_fifo_cache_manager.h"
#include "consensus_log_index.h"
#include "consensus_prefetch_manager.h"

#include "my_macros.h"

#include "rpl_consensus.h"
#include "sql/binlog.h"
#include "sql/binlog_ostream.h"
#include "sql/consensus/consensus_applier_info.h"
#include "sql/consensus/consensus_info.h"

#define CACHE_BUFFER_SIZE (IO_SIZE * 16)

#define CLUSTER_INFO_EXTRA_LENGTH \
  27  // LOG_EVENT_HEADER_LEN + POST_HEADER_LENGTH + BINLOG_CHECKSUM_LEN

struct SHOW_VAR;

class MYSQL_BIN_LOG;
class Relay_log_info;

enum Consensus_Log_System_Status { RELAY_LOG_WORKING = 0, BINLOG_WORKING = 1 };

enum Consensus_log_event_flag {
  FLAG_GU1 = 1,
  FLAG_GU2 = 1 << 1, /* FLAG_GU2 = 3 - FLAG_GU1 */
  FLAG_LARGE_TRX = 1 << 2,
  FLAG_LARGE_TRX_END = 1 << 3,
  FLAG_CONFIG_CHANGE = 1 << 4,
  FLAG_BLOB = 1 << 5,
  FLAG_BLOB_END = 1 << 6,
  FLAG_BLOB_START = 1 << 7, /* we should mark the start for SDK */
  FLAG_ROTATE = 1 << 8
};

struct ConsensusLogEntry {
  uint64 term;
  uint64 index;
  size_t buf_size;
  uchar *buffer;
  bool outer; /* whether created by consensus module */
  uint flag;  /* atomic flag marked */
  uint64 checksum;
};

struct ConsensusStateChange {
  StateType state;
  uint64_t term;
  uint64_t index;
};

class ConsensusLogManager {
 public:
  ConsensusLogManager();
  ~ConsensusLogManager();
  int init(uint64 max_fifo_cache_size_arg, uint64 max_prefetch_cache_size_arg,
           uint64 fake_current_index = 0);
  int init_service();
  int cleanup();

  IO_CACHE_binlog_cache_storage *get_log_cache() { return cache_log; }
  std::string get_empty_log() { return empty_log_event_content; }

  MYSQL_BIN_LOG *get_binlog() { return binlog; }
  void set_binlog(MYSQL_BIN_LOG *binlog_arg) { binlog = binlog_arg; }
  Relay_log_info *get_relay_log_info() { return rli_info; }
  void set_relay_log_info(Relay_log_info *rli_info_arg) {
    rli_info = rli_info_arg;
  }

  bool get_first_event_in_file() { return first_event_in_file; }
  void set_first_event_in_file(bool arg) { first_event_in_file = arg; }

  ConsensusLogIndex *get_log_file_index() { return log_file_index; }
  ConsensusPreFetchManager *get_prefetch_manager() { return prefetch_manager; }
  ConsensusFifoCacheManager *get_fifo_cache_manager() {
    return fifo_cache_manager;
  }

  Consensus_info *get_consensus_info() { return consensus_info; }
  Consensus_applier_info *get_applier_info() { return consensus_applier_info; }

  // consensus status
  Consensus_Log_System_Status get_status() { return status; }
  void set_status(Consensus_Log_System_Status status_arg) {
    status = status_arg;
  }
  uint64 get_current_state_degrade_term() { return current_state_degrade_term; }

  // consensus term/index
  uint64 get_current_term() { return current_term; }
  uint64 get_current_index() { return current_index; }
  uint64 get_cache_index();
  uint64 get_sync_index();
  uint64 get_final_sync_index();
  void set_current_term(uint64 current_term_arg) {
    current_term = current_term_arg;
  }
  void set_current_index(uint64 current_index_arg) {
    current_index = current_index_arg;
  }
  void incr_current_index() { current_index++; }
  void set_cache_index(uint64 cache_index_arg);
  void set_sync_index(uint64 sync_index_arg);
  void set_sync_index_if_greater(uint64 sync_index_arg);

  // recovery and applier infos
  int recovery_applier_status();
  uint64 get_recovery_index_hwl() { return recovery_index_hwl; }
  bool get_start_without_log() { return start_without_log; }
  uint64 get_apply_index() { return apply_index; }
  uint64 get_real_apply_index() { return real_apply_index; }
  uint64 get_apply_term() { return apply_term; }
  uint64 get_stop_term() { return stop_term; }
  bool get_in_large_trx() { return in_large_trx; }
  void set_recovery_index_hwl(uint64 index_arg) {
    recovery_index_hwl = index_arg;
  }
  void set_start_without_log(bool start_without_log_arg) {
    start_without_log = start_without_log_arg;
  }
  void set_apply_index(uint64 apply_index_arg) {
    apply_index = apply_index_arg;
  }
  void set_real_apply_index(uint64 real_apply_index_arg) {
    real_apply_index = real_apply_index_arg;
  }
  void set_apply_term(uint64 apply_term_arg) { apply_term = apply_term_arg; }

  void set_apply_catchup(bool apply_catchup_arg) {
    apply_catchup = apply_catchup_arg;
  }
  void set_in_large_trx(bool in_large_trx_arg) {
    in_large_trx = in_large_trx_arg;
  }

  bool get_enable_rotate() { return enable_rotate; }
  void set_enable_rotate(bool arg) { enable_rotate = arg; }

  // for concurrency
  inline mysql_mutex_t *get_log_term_lock() { return &LOCK_consensuslog_term; }
  inline mysql_rwlock_t *get_consensuslog_status_lock() {
    return &LOCK_consensuslog_status;
  }
  inline mysql_rwlock_t *get_consensuslog_commit_lock() {
    return &LOCK_consensuslog_commit;
  }
  inline mysql_rwlock_t *get_consensuslog_truncate_lock() {
    return &LOCK_consensuslog_truncate;
  }

  mysql_mutex_t *get_apply_thread_lock() {
    return &LOCK_consensus_applier_catchup;
  }
  mysql_cond_t *get_catchup_cond() { return &COND_consensus_applier_catchup; }

  // consensus_info
  int init_consensus_info();
  int update_consensus_info();
  int set_start_apply_index_if_need(uint64 consensus_index);
  int set_start_apply_term_if_need(uint64 consensus_term);

  // for log operation
  int write_log_entry(ConsensusLogEntry &log, uint64 *consensus_index,
                      bool with_check = false);
  int write_log_entries(std::vector<ConsensusLogEntry> &logs,
                        uint64 *max_index);
  int get_log_entry(uint64 channel_id, uint64 consensus_index,
                    uint64 *consensus_term, std::string &log_content,
                    bool *outer, uint *flag, uint64 *checksum, bool fast_fail);
  int get_log_directly(uint64 consensus_index, uint64 *consensus_term,
                       std::string &log_content, bool *outer, uint *flag,
                       uint64 *checksum, bool need_content = true);
  uint64_t get_left_log_size(uint64 start_log_index, uint64 max_packet_size);
  int prefetch_log_directly(THD *thd, uint64 channel_id,
                            uint64 consensus_index);
  int get_log_position(uint64 consensus_index, bool need_lock, char *log_name,
                       uint64 *pos);
  uint64 get_next_trx_index(uint64 consensus_index, bool need_lock = true);
  int truncate_log(uint64 consensus_index);
  int purge_log(uint64 consensus_index);
  uint64 get_exist_log_length();

  void lock_consensus_state_change();
  void unlock_consensus_state_change();
  void wait_state_change_cond();
  bool is_state_change_queue_empty() {
    return consensus_state_change_queue.empty();
  }
  void add_state_change_request(ConsensusStateChange &state_change);
  ConsensusStateChange get_stage_change_from_queue();

  int wait_leader_degraded(uint64 term, uint64 index);
  int wait_follower_upgraded(uint64 term, uint64 index);
  int wait_follower_change_term(uint64 term);

  void set_event_timestamp(uint32 t) { event_tv_sec.store(t); }
  uint32 get_event_timestamp() { return event_tv_sec.load(); }

  int start_consensus_state_change_thread();
  int stop_consensus_state_change_thread();

  bool is_state_machine_ready();

 private:
  int change_meta_if_needed();
  void wait_replay_log_finished();
  void wait_apply_threads_stop();
  int dump_cluster_info_to_file(std::string meta_file_name,
                                std::string output_str);

 private:
  bool inited;
  bool first_event_in_file;

  std::string empty_log_event_content;
  IO_CACHE_binlog_cache_storage *cache_log;  // cache a ConsensusLogEntry, and
                                             // communicate with algorithm layer
  ConsensusPreFetchManager *prefetch_manager;      // prefetch module
  ConsensusFifoCacheManager *fifo_cache_manager;   // fifo cache module
  ConsensusLogIndex *log_file_index;               // consensus log file index

  Consensus_info *consensus_info;                  // consensus system info
  Consensus_applier_info *consensus_applier_info;  // consensus system info

  mysql_mutex_t LOCK_consensuslog_term;  // protect bl_consensus_log::term

  /* protected by LOCK_consensuslog_status */
  std::atomic<uint64> current_term;   // the current system term, changed
                                      // by stageChange callback
  std::atomic<uint64> current_index;  // last log index in the log system
  std::atomic<uint64>
      current_state_degrade_term;   // the term when degrade

  mysql_rwlock_t LOCK_consensuslog_truncate;
  std::atomic<uint64> cache_index;  // last cache log entry
  std::atomic<uint64> sync_index;   // last log entry

  /* Consensus recovery and applier */
  uint64 recovery_index_hwl;  // for crash recovery
  bool start_without_log;
  std::atomic<uint64> apply_index;         // sql thread coordinator apply index
  std::atomic<uint64> real_apply_index;  // for large trx
  std::atomic<uint64> apply_term;        // sql thread coordinator apply term
  std::atomic<bool> in_large_trx;
  std::atomic<bool> enable_rotate;  // do not rotate if in middle of large trx

  /* Consensus applier exit */
  mysql_mutex_t LOCK_consensus_applier_catchup;
  mysql_cond_t COND_consensus_applier_catchup;  // whether apply
                                           // thread arrived commit index
  bool apply_catchup;  // protect by LOCK_consensus_applier_catchup, determine
                       // whether apply thread catchup
  std::atomic<uint64> stop_term;    // atomic read/write, mark sql thread
                                    // coordinator stop condition

  mysql_rwlock_t LOCK_consensuslog_status;  // protect consensus log
  Consensus_Log_System_Status
      status;             // leader: binlog system is working,
                          // follower or candidator: relaylog system is working
  MYSQL_BIN_LOG *binlog;  // the MySQL binlog object
  Relay_log_info *rli_info;  // the MySQL relay log info object, include
                             // relay_log, protected by LOCK_consensuslog_status

  mysql_rwlock_t LOCK_consensuslog_commit;  // protect consensus commit

  std::atomic<bool> consensus_state_change_is_running;
  std::deque<ConsensusStateChange> consensus_state_change_queue;
  my_thread_handle consensus_state_change_thread_handle;
  mysql_cond_t COND_consensus_state_change;
  mysql_mutex_t LOCK_consensus_state_change;

  std::map<uint64, uint64>
      consensus_pos_index;  // <consensusIndex, pos> for search log

  // consensus_info
  std::atomic<bool> already_set_start_index;  // set at first downgrade, used to
                                              // set correct start apply index
  std::atomic<bool> already_set_start_term;   // set at first downgrade, used to
                                              // set correct start apply term

  std::atomic<uint32>
      event_tv_sec;  // last log event timestamp received from leader
};

extern ConsensusLogManager consensus_log_manager;

void *run_consensus_stage_change(void *arg);
uint64 show_fifo_cache_size(THD *, SHOW_VAR *var, char *buff);
uint64 show_first_index_in_fifo_cache(THD *, SHOW_VAR *var, char *buff);
uint64 show_log_count_in_fifo_cache(THD *, SHOW_VAR *var, char *buff);
int show_appliedindex_checker_queue(THD *, SHOW_VAR *var, char *);
uint64 show_easy_pool_alloc_byte(THD *, SHOW_VAR *var, char *buff);

#endif
