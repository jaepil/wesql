#ifndef CONSENSUS_APPLIER_INCLUDE
#define CONSENSUS_APPLIER_INCLUDE

#include "my_inttypes.h"

class Master_info;
class Log_event;
class Relay_log_info;

int init_consensus_replica();
int start_consensus_replica();
void end_consensus_replica();
bool is_consensus_applier_running();
bool applier_mts_recovery_groups(Relay_log_info *rli);
int mts_recovery_max_consensus_index();
int start_consensus_apply_threads(Master_info *mi);
void stop_consensus_apply_threads();
int check_exec_consensus_log_end_condition(Relay_log_info *rli);
void update_consensus_apply_pos(Relay_log_info *rli, Log_event *ev);
int calculate_consensus_apply_start_pos(Relay_log_info *rli);
void update_consensus_applied_index(uint64 applied_index);

#endif
