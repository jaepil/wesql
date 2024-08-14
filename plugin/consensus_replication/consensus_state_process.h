
#ifndef CONSENSUS_STATE_PROCESS_INCLUDE
#define CONSENSUS_STATE_PROCESS_INCLUDE
#include <atomic>
#include <queue>

#include "rpl_consensus.h"

#include "sql/binlog.h"
#include "sql/consensus/consensus_applier_info.h"
#include "sql/consensus/consensus_info.h"

struct SHOW_VAR;

class MYSQL_BIN_LOG;
class Relay_log_info;

enum Consensus_Log_System_Status { RELAY_LOG_WORKING = 0, BINLOG_WORKING = 1 };

struct ConsensusStateChange {
  StateType state;
  uint64_t term;
  uint64_t index;
};

class ConsensusStateProcess {
 public:
  ConsensusStateProcess()
      : inited(false),
        current_term(1),
        current_state_degrade_term(0),
        recovery_index_hwl(0),
        status(Consensus_Log_System_Status::BINLOG_WORKING),
        binlog(nullptr),
        rli_info(nullptr),
        consensus_state_change_is_running(false) {}
  ~ConsensusStateProcess() {}

  int init();
  int cleanup();
  int init_service();

  uint64 get_current_term() { return current_term; }
  void set_current_term(uint64 current_term_arg) {
    current_term = current_term_arg;
  }

  // consensus status
  Consensus_Log_System_Status get_status(bool need_lock = false) {
    Consensus_Log_System_Status ret;
    if (need_lock) mysql_rwlock_rdlock(&LOCK_consensuslog_status);
    ret = status;
    if (need_lock) mysql_rwlock_unlock(&LOCK_consensuslog_status);
    return ret;
  }

  void set_status(Consensus_Log_System_Status status_arg) {
    status = status_arg;
  }
  uint64 get_current_state_degrade_term() { return current_state_degrade_term; }

  MYSQL_BIN_LOG *get_consensus_log();
  MYSQL_BIN_LOG *get_binlog() { return binlog; }
  void set_binlog(MYSQL_BIN_LOG *binlog_arg) { binlog = binlog_arg; }
  Relay_log_info *get_relay_log_info() { return rli_info; }
  void set_relay_log_info(Relay_log_info *rli_info_arg) {
    rli_info = rli_info_arg;
  }

  // recovery and applier infos
  int recovery_applier_status();
  uint64 get_recovery_index_hwl() { return recovery_index_hwl; }
  void set_recovery_index_hwl(uint64 index_arg) {
    recovery_index_hwl = index_arg;
  }

  // for concurrency
  inline mysql_mutex_t *get_log_term_lock() { return &LOCK_consensuslog_term; }
  inline mysql_rwlock_t *get_consensuslog_status_lock() {
    return &LOCK_consensuslog_status;
  }
  inline mysql_rwlock_t *get_consensuslog_commit_lock() {
    return &LOCK_consensuslog_commit;
  }

  void lock_consensus_state_change();
  void unlock_consensus_state_change();
  void wait_state_change_cond();
  bool is_state_change_queue_empty() {
    return consensus_state_change_queue.empty();
  }
  bool is_state_change_running() { return consensus_state_change_is_running; }
  void add_state_change_request(ConsensusStateChange &state_change);
  ConsensusStateChange get_stage_change_from_queue();

  int wait_leader_degraded(uint64 term, uint64 index);
  int wait_follower_upgraded(uint64 term, uint64 index);
  int wait_follower_change_term(uint64 term);

  int start_consensus_state_change_thread();
  int stop_consensus_state_change_thread();

  bool is_state_machine_ready();

 private:
  bool inited;
  mysql_mutex_t LOCK_consensuslog_term;  // protect bl_consensus_log::term

  /* protected by LOCK_consensuslog_status */
  std::atomic<uint64> current_term;  // the current system term, changed
                                     // by stageChange callback
  std::atomic<uint64> current_state_degrade_term;  // the term when degrade

  /* Consensus recovery and applier */
  uint64 recovery_index_hwl;  // for crash recovery

  mysql_rwlock_t LOCK_consensuslog_commit;  // protect consensus commit

  mysql_rwlock_t LOCK_consensuslog_status;  // protect consensus log
  Consensus_Log_System_Status
      status;             // leader: binlog system is working,
                          // follower or candidator: relaylog system is working
  MYSQL_BIN_LOG *binlog;  // the MySQL binlog object
  Relay_log_info *rli_info;  // the MySQL relay log info object, include
                             // relay_log, protected by LOCK_consensuslog_status

  std::atomic<bool> consensus_state_change_is_running;
  std::deque<ConsensusStateChange> consensus_state_change_queue;
  my_thread_handle consensus_state_change_thread_handle;
  mysql_cond_t COND_consensus_state_change;
  mysql_mutex_t LOCK_consensus_state_change;
};

extern ConsensusStateProcess consensus_state_process;

#endif  // CONSENSUS_STATE_PROCESS_INCLUDE