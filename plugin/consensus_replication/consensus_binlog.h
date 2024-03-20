#ifndef CONSENSUS_PLUGIN_BINLOG_INCLUDE
#define CONSENSUS_PLUGIN_BINLOG_INCLUDE

#include "sql/binlog.h"
#include "sql/binlog_istream.h"
#include "sql/log_event.h"
#include "sql/consensus_log_event.h"
#include "sql/rpl_rli.h"

class binlog_cache_data;
class IO_CACHE_binlog_cache_storage;
struct ConsensusLogEntry;

void consensus_before_commit(THD *thd);
int write_cache_to_consensus_log(THD *thd, MYSQL_BIN_LOG *binlog,
                                 Gtid_log_event &gtid_event,
                                 binlog_cache_data *cache_data,
                                 bool have_checksum);
int append_consensus_log(MYSQL_BIN_LOG *binlog, ConsensusLogEntry &log,
                         uint64 *index, Relay_log_info *rli,
                         bool with_check = false);
int append_multi_consensus_logs(MYSQL_BIN_LOG *binlog,
                                std::vector<ConsensusLogEntry> &logs,
                                uint64 *max_index, Relay_log_info *rli);
uint64 consensus_get_trx_end_index(uint64 firstIndex);
int consensus_get_log_entry(uint64 consensus_index, uint64 *consensus_term,
                            std::string &log_content, bool *outer, uint *flag,
                            uint64 *checksum, bool need_content);
int consensus_get_log_position(uint64 consensus_index, char *log_name,
                               uint64 *pos);
int consensus_find_log_by_index(uint64 consensus_index, std::string &file_name,
                                uint64 &start_index);
int consensus_prefetch_log_entries(THD *thd, uint64 channel_id,
                                   uint64 consensus_index);
int consensus_find_pos_by_index(const char *file_name, const uint64 start_index,
                                const uint64 consensus_index, uint64 *pos);
int consensus_get_first_index();

int build_consensus_log_index(MYSQL_BIN_LOG *binlog);
int copy_from_consensus_log_cache(IO_CACHE_binlog_cache_storage *from,
                                  uchar *to, my_off_t max_len);

int consensus_binlog_after_purge_file(MYSQL_BIN_LOG *mysql_binlog,
                                      const char *log_file_name);

void update_pos_map_by_start_index(const uint64 start_index,
                                   Consensus_log_event *ev, uint64 start_pos,
                                   uint64 end_pos, bool &next_set);

int rotate_consensus_log(THD *thd, bool force_rotate);
int purge_consensus_logs_on_conditions(ulong purge_time, ulong purge_size,
                                       const char *to_log, bool auto_purge);

int reencrypt_consensus_logs();

int consensus_open_archive_log(uint64 first_index, uint64 current_index);

bool consensus_show_log_events(THD *thd);

bool consensus_show_logs(THD *thd);
#endif