/*
   Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2015, Facebook, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#include "se_properties_collector.h"
#include <algorithm>
#include <map>
#include <string>
#include <vector>
#include "my_stacktrace.h"
#include "sql_array.h"
#include "dict/se_ddl_manager.h"
#include "util/se_utils.h"
#include "smartengine/se_constants.h"

namespace smartengine
{

bool se_compaction_sequential_deletes_count_sd = false;

SeTablePropertyCollector::SeTablePropertyCollector(
    SeDdlManager *const ddl_manager,
    const SeCompactParams &params,
    const uint32_t &cf_id,
    const uint8_t &table_stats_sampling_pct)
    : m_cf_id(cf_id),
      m_ddl_manager(ddl_manager),
      m_last_stats(nullptr),
      m_rows(0l),
      m_window_pos(0l),
      m_deleted_rows(0l),
      m_max_deleted_rows(0l),
      m_file_size(0),
      m_params(params),
      m_table_stats_sampling_pct(table_stats_sampling_pct),
      m_seed(time(nullptr)),
      m_card_adj_extra(1.)
{
  assert(ddl_manager != nullptr);

  // We need to adjust the index cardinality numbers based on the sampling
  // rate so that the output of "SHOW INDEX" command will reflect reality
  // more closely. It will still be an approximation, just a better one.
  if (m_table_stats_sampling_pct > 0) {
    m_card_adj_extra = 100. / m_table_stats_sampling_pct;
  }

  m_deleted_rows_window.resize(m_params.m_window, false);
}

/*
 This fuction is called on adding extent
*/
common::Status SeTablePropertyCollector::add_extent(
    bool add_flag,
    const common::Slice &smallest_key,
    const common::Slice &largest_key,
    int64_t file_size,
    int64_t num_entries,
    int64_t num_deletes)
{
  if (!m_ddl_manager) {
    common::Status::OK();
  }

  ((void)num_deletes);  // UNUSED avoid warning
  if (smallest_key.size() >= 4 && largest_key.size() >= 4) {
    uint32_t start_index_id = se_netbuf_to_uint32(
                          reinterpret_cast<const uchar *>(smallest_key.data()));
    uint32_t end_index_id = se_netbuf_to_uint32(
                          reinterpret_cast<const uchar *>(largest_key.data()));
    SeIndexStats stats;
    if (start_index_id == end_index_id) {
      // the extent belong to one index
      stats.m_gl_index_id = { m_cf_id, start_index_id };
      stats.m_data_size = file_size;
      stats.m_rows = num_entries;
      stats.m_actual_disk_size = storage::MAX_EXTENT_SIZE;

      m_ddl_manager->adjust_stats2(stats, add_flag);

    } else {
      // several indes, only count begin and end
      SeIndexStats stats_end;

      stats.m_data_size = (file_size / 2);
      stats.m_rows = (num_entries / 2);
      stats.m_actual_disk_size = (storage::MAX_EXTENT_SIZE / 2);
      stats.m_gl_index_id = { m_cf_id, start_index_id };

      stats_end = stats;
      stats_end.m_gl_index_id = { m_cf_id, end_index_id };

      m_ddl_manager->adjust_stats2(stats, add_flag);
      m_ddl_manager->adjust_stats2(stats_end, add_flag);}
    }

  return common::Status::OK();
}


/*
  This function is called on adding user defined key in the SST file
*/
common::Status SeTablePropertyCollector::AddUserKey(
    const common::Slice &key,
    const common::Slice &value,
    table::EntryType type,
    common::SequenceNumber seq,
    uint64_t file_size)
{
  if (key.size() >= 4) {
    AdjustDeletedRows(type);

    m_rows++;

    CollectStatsForRow(key, value, type, file_size);
  }

  return common::Status::OK();
}

void SeTablePropertyCollector::AdjustDeletedRows(table::EntryType type)
{
  if (m_params.m_window > 0) {
    // record the "is deleted" flag into the sliding window
    // the sliding window is implemented as a circular buffer
    // in m_deleted_rows_window vector
    // the current position in the circular buffer is pointed at by
    // m_rows % m_deleted_rows_window.size()
    // m_deleted_rows is the current number of 1's in the vector
    // --update the counter for the element which will be overridden
    const bool is_delete = (type == table::kEntryDelete ||
                            (type == table::kEntrySingleDelete &&
                             se_compaction_sequential_deletes_count_sd));

    // Only make changes if the value at the current position needs to change
    if (is_delete != m_deleted_rows_window[m_window_pos]) {
      // Set or clear the flag at the current position as appropriate
      m_deleted_rows_window[m_window_pos] = is_delete;
      if (!is_delete) {
        m_deleted_rows--;
      } else if (++m_deleted_rows > m_max_deleted_rows) {
        m_max_deleted_rows = m_deleted_rows;
      }
    }

    if (++m_window_pos == m_params.m_window) {
      m_window_pos = 0;
    }
  }
}

SeIndexStats *SeTablePropertyCollector::AccessStats(const common::Slice &key)
{
  GL_INDEX_ID gl_index_id = {.cf_id = m_cf_id,
                             .index_id = se_netbuf_to_uint32(
                                 reinterpret_cast<const uchar *>(key.data()))};

  if (m_last_stats == nullptr || m_last_stats->m_gl_index_id != gl_index_id) {
    m_keydef = nullptr;

    // starting a new table
    // add the new element into m_stats
    m_stats.emplace_back(gl_index_id);
    m_last_stats = &m_stats.back();

    if (m_ddl_manager) {
      // safe_find() returns a std::shared_ptr<SeKeyDef> with the count
      // incremented (so it can't be deleted out from under us) and with
      // the mutex locked (if setup has not occurred yet).  We must make
      // sure to free the mutex (via unblock_setup()) when we are done
      // with this object.  Currently this happens earlier in this function
      // when we are switching to a new SeKeyDef and when this object
      // is destructed.
      m_keydef = m_ddl_manager->safe_find(gl_index_id);
      if (m_keydef != nullptr) {
        // resize the array to the number of columns.
        // It will be initialized with zeroes
        m_last_stats->m_distinct_keys_per_prefix.resize(
            m_keydef->get_key_parts());
        m_last_stats->m_name = m_keydef->get_name();
      }
    }
    m_last_key.clear();
  }

  return m_last_stats;
}

void SeTablePropertyCollector::CollectStatsForRow(
    const common::Slice &key,
    const common::Slice &value,
    const table::EntryType &type,
    const uint64_t &file_size)
{
  const auto stats = AccessStats(key);

  stats->m_data_size += key.size() + value.size();

  // Incrementing per-index entry-type statistics
  switch (type) {
  case table::kEntryPut:
    stats->m_rows++;
    break;
  case table::kEntryDelete:
    stats->m_entry_deletes++;
    break;
  case table::kEntrySingleDelete:
    stats->m_entry_single_deletes++;
    break;
  case table::kEntryMerge:
    stats->m_entry_merges++;
    break;
  case table::kEntryOther:
    stats->m_entry_others++;
    break;
  default:
    // NO_LINT_DEBUG
    sql_print_error("SE: Unexpected entry type found: %u. "
                    "This should not happen so aborting the system.",
                    type);
    abort_with_stack_traces();
    break;
  }

  stats->m_actual_disk_size += file_size - m_file_size;
  m_file_size = file_size;

  if (m_keydef != nullptr && ShouldCollectStats()) {
    std::size_t column = 0;
    bool new_key = true;

    if (!m_last_key.empty()) {
      common::Slice last(m_last_key.data(), m_last_key.size());
      new_key = (m_keydef->compare_keys(&last, &key, &column) == 0);
    }

    if (new_key) {
      assert(column <= stats->m_distinct_keys_per_prefix.size());

      for (auto i = column; i < stats->m_distinct_keys_per_prefix.size(); i++) {
        stats->m_distinct_keys_per_prefix[i]++;
      }

      // assign new last_key for the next call
      // however, we only need to change the last key
      // if one of the first n-1 columns is different
      // If the n-1 prefix is the same, no sense in storing
      // the new key
      if (column < stats->m_distinct_keys_per_prefix.size()) {
        m_last_key.assign(key.data(), key.size());
      }
    }
  }
}

const char *SeTablePropertyCollector::INDEXSTATS_KEY = "__indexstats__";

/*
  This function is called to compute properties to store in sst file
*/
common::Status SeTablePropertyCollector::Finish(table::UserCollectedProperties *const properties)
{
  assert(properties != nullptr);

  properties->insert({INDEXSTATS_KEY,
                      SeIndexStats::materialize(m_stats, m_card_adj_extra)});
  return common::Status::OK();
}

bool SeTablePropertyCollector::NeedCompact() const
{
  return m_params.m_deletes && (m_params.m_window > 0) &&
         (m_file_size > m_params.m_file_size) &&
         (m_max_deleted_rows > m_params.m_deletes);
}

bool SeTablePropertyCollector::ShouldCollectStats()
{
  // Zero means that we'll use all the keys to update statistics.
  if (!m_table_stats_sampling_pct ||
      SE_TBL_STATS_SAMPLE_PCT_MAX == m_table_stats_sampling_pct) {
    return true;
  }

  const int val = rand_r(&m_seed) % (SE_TBL_STATS_SAMPLE_PCT_MAX -
                                     SE_TBL_STATS_SAMPLE_PCT_MIN + 1) +
                  SE_TBL_STATS_SAMPLE_PCT_MIN;

  assert(val >= SE_TBL_STATS_SAMPLE_PCT_MIN);
  assert(val <= SE_TBL_STATS_SAMPLE_PCT_MAX);

  return val <= m_table_stats_sampling_pct;
}

/*
  Returns the same as above, but in human-readable way for logging
*/
table::UserCollectedProperties SeTablePropertyCollector::GetReadableProperties() const
{
  std::string s;
#ifdef DBUG_OFF
  s.append("[...");
  s.append(std::to_string(m_stats.size()));
  s.append("  records...]");
#else
  bool first = true;
  for (auto it : m_stats) {
    if (first) {
      first = false;
    } else {
      s.append(",");
    }
    s.append(GetReadableStats(it));
  }
#endif
  return table::UserCollectedProperties{{INDEXSTATS_KEY, s}};
}

std::string SeTablePropertyCollector::GetReadableStats(const SeIndexStats &it)
{
  std::string s;
  s.append("(");
  s.append(std::to_string(it.m_gl_index_id.cf_id));
  s.append(", ");
  s.append(std::to_string(it.m_gl_index_id.index_id));
  s.append("):{name:");
  s.append(it.m_name);
  s.append(", size:");
  s.append(std::to_string(it.m_data_size));
  s.append(", m_rows:");
  s.append(std::to_string(it.m_rows));
  s.append(", m_actual_disk_size:");
  s.append(std::to_string(it.m_actual_disk_size));
  s.append(", deletes:");
  s.append(std::to_string(it.m_entry_deletes));
  s.append(", single_deletes:");
  s.append(std::to_string(it.m_entry_single_deletes));
  s.append(", merges:");
  s.append(std::to_string(it.m_entry_merges));
  s.append(", others:");
  s.append(std::to_string(it.m_entry_others));
  s.append(", distincts per prefix: [");
  for (auto num : it.m_distinct_keys_per_prefix) {
    s.append(std::to_string(num));
    s.append(" ");
  }
  s.append("]}");
  return s;
}

/*
  Given the properties of an SST file, reads the stats from it and returns it.
*/

void SeTablePropertyCollector::read_stats_from_tbl_props(
    const std::shared_ptr<const table::TableProperties> &table_props,
    std::vector<SeIndexStats> *const out_stats_vector)
{
  assert(out_stats_vector != nullptr);
  const auto &user_properties = table_props->user_collected_properties;
  const auto it2 = user_properties.find(std::string(INDEXSTATS_KEY));
  if (it2 != user_properties.end()) {
    auto result MY_ATTRIBUTE((__unused__)) =
        SeIndexStats::unmaterialize(it2->second, out_stats_vector);
    assert(result == 0);
  }
}

/*
  Serializes an array of SeIndexStats into a network string.
*/
std::string SeIndexStats::materialize(const std::vector<SeIndexStats> &stats,
                                      const float card_adj_extra)
{
  String ret;
  se_netstr_append_uint16(&ret, INDEX_STATS_VERSION_ENTRY_TYPES);
  for (const auto &i : stats) {
    se_netstr_append_uint32(&ret, i.m_gl_index_id.cf_id);
    se_netstr_append_uint32(&ret, i.m_gl_index_id.index_id);
    assert(sizeof i.m_data_size <= 8);
    se_netstr_append_uint64(&ret, i.m_data_size);
    se_netstr_append_uint64(&ret, i.m_rows);
    se_netstr_append_uint64(&ret, i.m_actual_disk_size);
    se_netstr_append_uint64(&ret, i.m_distinct_keys_per_prefix.size());
    se_netstr_append_uint64(&ret, i.m_entry_deletes);
    se_netstr_append_uint64(&ret, i.m_entry_single_deletes);
    se_netstr_append_uint64(&ret, i.m_entry_merges);
    se_netstr_append_uint64(&ret, i.m_entry_others);
    for (const auto &num_keys : i.m_distinct_keys_per_prefix) {
      const float upd_num_keys = num_keys * card_adj_extra;
      se_netstr_append_uint64(&ret, static_cast<int64_t>(upd_num_keys));
    }
  }

  return std::string((char *)ret.ptr(), ret.length());
}

/**
  @brief
  Reads an array of SeIndexStats from a string.
  @return HA_EXIT_FAILURE if it detects any inconsistency in the input
  @return HA_EXIT_SUCCESS if completes successfully
*/
int SeIndexStats::unmaterialize(const std::string &s, std::vector<SeIndexStats> *const ret)
{
  const uchar *p = se_std_str_to_uchar_ptr(s);
  const uchar *const p2 = p + s.size();

  assert(ret != nullptr);

  if (p + 2 > p2) {
    return HA_EXIT_FAILURE;
  }

  const int version = se_netbuf_read_uint16(&p);
  SeIndexStats stats;
  // Make sure version is within supported range.
  if (version < INDEX_STATS_VERSION_INITIAL ||
      version > INDEX_STATS_VERSION_ENTRY_TYPES) {
    // NO_LINT_DEBUG
    sql_print_error("Index stats version %d was outside of supported range. "
                    "This should not happen so aborting the system.",
                    version);
    abort_with_stack_traces();
  }

  size_t needed = sizeof(stats.m_gl_index_id.cf_id) +
                  sizeof(stats.m_gl_index_id.index_id) +
                  sizeof(stats.m_data_size) + sizeof(stats.m_rows) +
                  sizeof(stats.m_actual_disk_size) + sizeof(uint64);
  if (version >= INDEX_STATS_VERSION_ENTRY_TYPES) {
    needed += sizeof(stats.m_entry_deletes) +
              sizeof(stats.m_entry_single_deletes) +
              sizeof(stats.m_entry_merges) + sizeof(stats.m_entry_others);
  }

  while (p < p2) {
    if (p + needed > p2) {
      return HA_EXIT_FAILURE;
    }
    se_netbuf_read_gl_index(&p, &stats.m_gl_index_id);
    stats.m_data_size = se_netbuf_read_uint64(&p);
    stats.m_rows = se_netbuf_read_uint64(&p);
    stats.m_actual_disk_size = se_netbuf_read_uint64(&p);
    stats.m_distinct_keys_per_prefix.resize(se_netbuf_read_uint64(&p));
    if (version >= INDEX_STATS_VERSION_ENTRY_TYPES) {
      stats.m_entry_deletes = se_netbuf_read_uint64(&p);
      stats.m_entry_single_deletes = se_netbuf_read_uint64(&p);
      stats.m_entry_merges = se_netbuf_read_uint64(&p);
      stats.m_entry_others = se_netbuf_read_uint64(&p);
    }
    if (p +
            stats.m_distinct_keys_per_prefix.size() *
                sizeof(stats.m_distinct_keys_per_prefix[0]) >
        p2) {
      return HA_EXIT_FAILURE;
    }
    for (std::size_t i = 0; i < stats.m_distinct_keys_per_prefix.size(); i++) {
      stats.m_distinct_keys_per_prefix[i] = se_netbuf_read_uint64(&p);
    }
    ret->push_back(stats);
  }
  return HA_EXIT_SUCCESS;
}

/*
  Merges one SeIndexStats into another. Can be used to come up with the stats
  for the index based on stats for each sst
*/
void SeIndexStats::merge(const SeIndexStats &s,
                         const bool &increment,
                         const int64_t &estimated_data_len)
{
  std::size_t i;

  assert(estimated_data_len >= 0);

  m_gl_index_id = s.m_gl_index_id;
  if (m_distinct_keys_per_prefix.size() < s.m_distinct_keys_per_prefix.size()) {
    m_distinct_keys_per_prefix.resize(s.m_distinct_keys_per_prefix.size());
  }
  if (increment) {
    m_rows += s.m_rows;
    m_data_size += s.m_data_size;
    /*
      The Data_length and Avg_row_length are trailing statistics, meaning
      they don't get updated for the current SST until the next SST is
      written.  So, if se reports the data_length as 0,
      we make a reasoned estimate for the data_file_length for the
      index in the current SST.
    */
    m_actual_disk_size += s.m_actual_disk_size ? s.m_actual_disk_size
                                               : estimated_data_len * s.m_rows;
    m_entry_deletes += s.m_entry_deletes;
    m_entry_single_deletes += s.m_entry_single_deletes;
    m_entry_merges += s.m_entry_merges;
    m_entry_others += s.m_entry_others;
    if (s.m_distinct_keys_per_prefix.size() > 0) {
      for (i = 0; i < s.m_distinct_keys_per_prefix.size(); i++) {
        m_distinct_keys_per_prefix[i] += s.m_distinct_keys_per_prefix[i];
      }
    } else {
      for (i = 0; i < m_distinct_keys_per_prefix.size(); i++) {
        m_distinct_keys_per_prefix[i] +=
            s.m_rows >> (m_distinct_keys_per_prefix.size() - i - 1);
      }
    }
  } else {
    m_rows -= s.m_rows;
    m_data_size -= s.m_data_size;
    m_actual_disk_size -= s.m_actual_disk_size ? s.m_actual_disk_size
                                               : estimated_data_len * s.m_rows;
    m_entry_deletes -= s.m_entry_deletes;
    m_entry_single_deletes -= s.m_entry_single_deletes;
    m_entry_merges -= s.m_entry_merges;
    m_entry_others -= s.m_entry_others;
    if (s.m_distinct_keys_per_prefix.size() > 0) {
      for (i = 0; i < s.m_distinct_keys_per_prefix.size(); i++) {
        m_distinct_keys_per_prefix[i] -= s.m_distinct_keys_per_prefix[i];
      }
    } else {
      for (i = 0; i < m_distinct_keys_per_prefix.size(); i++) {
        m_distinct_keys_per_prefix[i] -=
            s.m_rows >> (m_distinct_keys_per_prefix.size() - i - 1);
      }
    }
  }
  // avoid negative stats
  if (m_rows < 0) {
    m_rows = 0;
  }
  if (m_actual_disk_size < 0) {
    m_actual_disk_size = 0;
  }
}

void SeIndexStats::update(const SeIndexStats &s, const int64_t &estimated_data_len)
{
  std::size_t i;

  assert(estimated_data_len >= 0);

  m_gl_index_id = s.m_gl_index_id;
  if (m_distinct_keys_per_prefix.size() < s.m_distinct_keys_per_prefix.size()) {
    m_distinct_keys_per_prefix.resize(s.m_distinct_keys_per_prefix.size());
  }
  m_rows = s.m_rows;
  m_data_size = s.m_data_size;

  /*
    The Data_length and Avg_row_length are trailing statistics, meaning
    they don't get updated for the current SST until the next SST is
    written.  So, if se reports the data_length as 0,
    we make a reasoned estimate for the data_file_length for the
    index in the current SST.
  */
  m_actual_disk_size = s.m_actual_disk_size ? s.m_actual_disk_size
                                              : estimated_data_len * s.m_rows;
  m_entry_deletes = s.m_entry_deletes;
  m_entry_single_deletes = s.m_entry_single_deletes;
  m_entry_merges = s.m_entry_merges;
  m_entry_others = s.m_entry_others;
  if (s.m_distinct_keys_per_prefix.size() > 0) {
    for (i = 0; i < s.m_distinct_keys_per_prefix.size(); i++) {
      m_distinct_keys_per_prefix[i] += s.m_distinct_keys_per_prefix[i];
    }
  } else {
    for (i = 0; i < m_distinct_keys_per_prefix.size(); i++) {
      m_distinct_keys_per_prefix[i] +=
          s.m_rows >> (m_distinct_keys_per_prefix.size() - i - 1);
    }
  }
  // avoid negative stats
  if (m_rows < 0) {
    m_rows = 0;
  }
  if (m_actual_disk_size < 0) {
    m_actual_disk_size = 0;
  }
}

} //namespace smartengine
