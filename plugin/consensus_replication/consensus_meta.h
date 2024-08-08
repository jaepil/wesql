#ifndef CONSENSUS_META_INCLUDE
#define CONSENSUS_META_INCLUDE
#include <atomic>

#include "sql/consensus/consensus_applier_info.h"
#include "sql/consensus/consensus_info.h"

class ConsensusMeta {
 public:
  ConsensusMeta()
      : inited(false),
        consensus_info(nullptr),
        consensus_applier_info(nullptr),
        already_set_start_index(false),
        already_set_start_term(false) {}
  ~ConsensusMeta() {}

  int init();
  int cleanup();

  Consensus_info *get_consensus_info() { return consensus_info; }
  Consensus_applier_info *get_applier_info() { return consensus_applier_info; }

  void clear_already_set_start() {
    already_set_start_index = false;
    already_set_start_term = false;
  }

  // consensus_info
  int init_consensus_info();
  int update_consensus_info();
  int set_start_apply_index_if_need(uint64 consensus_index);
  int set_start_apply_term_if_need(uint64 consensus_term);

 private:
  bool inited;
  Consensus_info *consensus_info;                  // consensus system info
  Consensus_applier_info *consensus_applier_info;  // consensus system info

  // consensus_info
  std::atomic<bool> already_set_start_index;  // set at first downgrade, used to
                                              // set correct start apply index
  std::atomic<bool> already_set_start_term;   // set at first downgrade, used to
                                              // set correct start apply term

  int change_meta_if_needed();
};

extern ConsensusMeta consensus_meta;

#endif // CONSENSUS_META_INCLUDE