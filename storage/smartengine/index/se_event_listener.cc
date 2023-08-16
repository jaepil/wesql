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

#include "se_event_listener.h"
#include <string>
#include <vector>
#include <mysql/plugin.h>
#include "dict/se_properties_collector.h"

namespace smartengine
{
void SeEventListener::update_index_stats(const table::TableProperties &props)
{
  assert(m_ddl_manager != nullptr);
  const auto tbl_props =
      std::make_shared<const table::TableProperties>(props);

  std::vector<SeIndexStats> stats;
  SeTablePropertyCollector::read_stats_from_tbl_props(tbl_props, &stats);

  //m_ddl_manager->adjust_stats(stats);
}

void SeEventListener::OnCompactionCompleted(
    db::DB *db,
    const common::CompactionJobInfo &ci)
{
  assert(db != nullptr);
  assert(m_ddl_manager != nullptr);
}

void SeEventListener::OnFlushCompleted(
    db::DB *db,
    const common::FlushJobInfo &flush_job_info)
{
  assert(db != nullptr);
  update_index_stats(flush_job_info.table_properties);
}

void SeEventListener::OnExternalFileIngested(
    db::DB *db,
    const common::ExternalFileIngestionInfo &info)
{
  assert(db != nullptr);
  update_index_stats(info.table_properties);
}

} //namespace smartengine
