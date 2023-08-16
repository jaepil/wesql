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

#include "smartengine/listener.h"

namespace smartengine {

class SeDdlManager;

class SeEventListener : public common::EventListener
{
public:
  SeEventListener(const SeEventListener &) = delete;
  SeEventListener &operator=(const SeEventListener &) = delete;

  explicit SeEventListener(SeDdlManager *const ddl_manager)
      : m_ddl_manager(ddl_manager) {}

  void OnCompactionCompleted(db::DB *db,
                             const common::CompactionJobInfo &ci) override;
  void OnFlushCompleted(db::DB *db,
                        const common::FlushJobInfo &flush_job_info) override;
  void OnExternalFileIngested(
      db::DB *db,
      const common::ExternalFileIngestionInfo &ingestion_info) override;

private:
  SeDdlManager *m_ddl_manager;

  void update_index_stats(const table::TableProperties &props);
};

} //namespace smartengine
