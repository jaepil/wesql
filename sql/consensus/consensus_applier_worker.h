#ifndef CONSENSUS_APPLIER_WORKER_INCLUDE
#define CONSENSUS_APPLIER_WORKER_INCLUDE

#include <atomic>
#include <string>

#include "mysql/components/services/bits/psi_rwlock_bits.h"
#include "sql/consensus/consensus_applier_info.h"
#include "sql/rpl_info.h"

class Consensus_applier_worker : public Rpl_info {
  friend class Rpl_info_factory;

 public:
  Consensus_applier_worker(
#ifdef HAVE_PSI_INTERFACE
      PSI_mutex_key *param_key_info_run_lock,
      PSI_mutex_key *param_key_info_data_lock,
      PSI_mutex_key *param_key_info_sleep_lock,
      PSI_mutex_key *param_key_info_thd_lock,
      PSI_mutex_key *param_key_info_data_cond,
      PSI_mutex_key *param_key_info_start_cond,
      PSI_mutex_key *param_key_info_stop_cond,
      PSI_mutex_key *param_key_info_sleep_cond
#endif
      , uint param_id);

  int init_info(bool on_recovery);
  void end_info();
  int flush_info(bool force, bool need_commit = false);
  bool set_info_search_keys(Rpl_info_handler *to) override;
  static size_t get_number_fields();
  static const uint *get_table_pk_field_indexes();

  virtual const char *get_for_channel_str(
      bool upper_case [[maybe_unused]]) const override {
    return NULL;
  }

  static void set_nullable_fields(MY_BITMAP *nullable_fields);

  inline void set_consensus_apply_index(ulonglong log_index) {
    consensus_apply_index = log_index;
  }
  inline ulonglong get_consensus_apply_index() { return consensus_apply_index; }

  uint64 saved_consensus_apply_index;

  int commit_positions(uint64 event_consensus_index);
  int rollback_positions();

  bool reset_recovery_info();
  int init_worker(ulong i);
 private:
  bool read_info(Rpl_info_handler *from) override;
  bool write_info(Rpl_info_handler *to) override;

  mysql_mutex_t LOCK_Consensus_applier_worker;
  std::atomic<uint64> consensus_apply_index;
  ulong id;
};
#endif
