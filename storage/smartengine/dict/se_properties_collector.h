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
#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>
#include "util/se_utils.h"
#include "smartengine/db.h"

namespace smartengine
{
class SeDdlManager;
class SeKeyDef;

extern bool se_compaction_sequential_deletes_count_sd;

//TODO: Zhao Dongsheng.This structure has been deprecated and will be removed in the future.
struct SeCompactParams {
  uint64_t m_deletes = 0, m_window = 0, m_file_size = 0;
};

struct SeIndexStats {
  enum {
    INDEX_STATS_VERSION_INITIAL = 1,
    INDEX_STATS_VERSION_ENTRY_TYPES = 2,
  };
  GL_INDEX_ID m_gl_index_id;
  int64_t m_data_size, m_rows, m_actual_disk_size;
  int64_t m_entry_deletes, m_entry_single_deletes;
  int64_t m_entry_merges, m_entry_others;
  std::vector<int64_t> m_distinct_keys_per_prefix;
  std::string m_name; // name is not persisted

  static std::string materialize(const std::vector<SeIndexStats> &stats,
                                 const float card_adj_extra);
  static int unmaterialize(const std::string &s,
                           std::vector<SeIndexStats> *const ret);

  SeIndexStats() : SeIndexStats({0, 0}) {}
  explicit SeIndexStats(GL_INDEX_ID gl_index_id)
      : m_gl_index_id(gl_index_id),
        m_data_size(0),
        m_rows(0),
        m_actual_disk_size(0),
        m_entry_deletes(0),
        m_entry_single_deletes(0),
        m_entry_merges(0),
        m_entry_others(0)
  {}

  void merge(const SeIndexStats &s,
             const bool &increment = true,
             const int64_t &estimated_data_len = 0);

  void update(const SeIndexStats &index_stats, const int64_t &estimated_data_len = 0);
  void reset()
  {
    m_data_size = 0;
    m_rows = 0;
    m_actual_disk_size = 0;
    m_entry_deletes = 0;
    m_entry_single_deletes = 0;
    m_entry_merges = 0;
    m_entry_others = 0;
  }
};

class SeTablePropertyCollector : public table::TablePropertiesCollector {
public:
  SeTablePropertyCollector(SeDdlManager *const ddl_manager,
                           const SeCompactParams &params,
                           const uint32_t &cf_id,
                           const uint8_t &table_stats_sampling_pct);

  /*
    Override parent class's virtual methods of interest.
  */
  virtual common::Status AddUserKey(const common::Slice &key,
                                    const common::Slice &value,
                                    table::EntryType type,
                                    common::SequenceNumber seq,
                                    uint64_t file_size) override;

  virtual common::Status Finish(table::UserCollectedProperties *properties) override;

  virtual const char *Name() const override { return "SeTablePropertyCollector"; }

  table::UserCollectedProperties GetReadableProperties() const override;

  bool NeedCompact() const override;
 
  virtual common::Status add_extent(bool add_flag,
                                    const common::Slice &smallest_key,
                                    const common::Slice &largest_key,
                                    int64_t file_size,
                                    int64_t num_entries,
                                    int64_t num_deletes) override;

public:
  uint64_t GetMaxDeletedRows() const { return m_max_deleted_rows; }

  static void read_stats_from_tbl_props(
      const std::shared_ptr<const table::TableProperties> &table_props,
      std::vector<SeIndexStats> *out_stats_vector);

private:
  static std::string GetReadableStats(const SeIndexStats &it);

  bool ShouldCollectStats();
  void CollectStatsForRow(const common::Slice &key,
                          const common::Slice &value,
                          const table::EntryType &type,
                          const uint64_t &file_size);

  SeIndexStats *AccessStats(const common::Slice &key);

  void AdjustDeletedRows(table::EntryType type);

private:
  uint32_t m_cf_id;
  std::shared_ptr<const SeKeyDef> m_keydef;
  SeDdlManager *m_ddl_manager;
  std::vector<SeIndexStats> m_stats;
  SeIndexStats *m_last_stats;
  static const char *INDEXSTATS_KEY;

  // last added key
  std::string m_last_key;

  // floating window to count deleted rows
  std::vector<bool> m_deleted_rows_window;
  uint64_t m_rows, m_window_pos, m_deleted_rows, m_max_deleted_rows;
  uint64_t m_file_size;
  SeCompactParams m_params;
  uint8_t m_table_stats_sampling_pct;
  unsigned int m_seed;
  float m_card_adj_extra;
};

class SeTablePropertyCollectorFactory
    : public table::TablePropertiesCollectorFactory
{
public:
  SeTablePropertyCollectorFactory(const SeTablePropertyCollectorFactory &) = delete;
  SeTablePropertyCollectorFactory &
  operator=(const SeTablePropertyCollectorFactory &) = delete;

  explicit SeTablePropertyCollectorFactory(SeDdlManager *ddl_manager)
      : m_ddl_manager(ddl_manager) {}

  /*
    Override parent class's virtual methods of interest.
  */

  virtual table::TablePropertiesCollector *CreateTablePropertiesCollector(
      table::TablePropertiesCollectorFactory::Context context) override
  {
    return new SeTablePropertyCollector(
        m_ddl_manager,
        m_params,
        context.column_family_id,
        m_table_stats_sampling_pct);
  }

  virtual const char *Name() const override
  {
    return "SeTablePropertyCollectorFactory";
  }

public:
  void SetCompactionParams(const SeCompactParams &params)
  {
    m_params = params;
  }

  void SetTableStatsSamplingPct(const uint8_t &table_stats_sampling_pct)
  {
    m_table_stats_sampling_pct = table_stats_sampling_pct;
  }

private:
  SeDdlManager *const m_ddl_manager;
  SeCompactParams m_params;
  uint8_t m_table_stats_sampling_pct;
};

} //namespace smartengine
