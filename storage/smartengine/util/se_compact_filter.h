/*
   Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Portions Copyright (c) 2016-Present, Facebook, Inc.
   Portions Copyright (c) 2012, Monty Program Ab

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

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation // gcc: Class implementation
#endif

#include <string>
#include "util/se_buff.h"
#include "smartengine/compaction_filter.h"

namespace smartengine
{

class SeCompactFilter : public storage::CompactionFilter
{
public:
  SeCompactFilter(const SeCompactFilter &) = delete;
  SeCompactFilter &operator=(const SeCompactFilter &) = delete;

  explicit SeCompactFilter(uint32_t _cf_id) : m_cf_id(_cf_id) {}
  ~SeCompactFilter() {}

  // keys are passed in sorted order within the same sst.
  // V1 Filter is thread safe on our usage (creating from Factory).
  // Make sure to protect instance variables when switching to thread
  // unsafe in the future.
  virtual bool Filter(int level, const common::Slice &key,
                      const common::Slice &existing_value,
                      std::string *new_value,
                      bool *value_changed) const override
  {
    assert(key.size() >= sizeof(uint32));

    GL_INDEX_ID gl_index_id;
    gl_index_id.cf_id = m_cf_id;
    gl_index_id.index_id = se_netbuf_to_uint32((const uchar *)key.data());
    assert(gl_index_id.index_id >= 1);

    if (gl_index_id != m_prev_index) // processing new index id
    {
      if (m_num_deleted > 0) {
        m_num_deleted = 0;
      }
      m_should_delete = false;
          //se_get_dict_manager()->is_drop_index_ongoing(gl_index_id);
      m_prev_index = gl_index_id;
    }

    if (m_should_delete) {
      m_num_deleted++;
    }

    return m_should_delete;
  }

  virtual bool IgnoreSnapshots() const override { return true; }

  virtual const char *Name() const override { return "SeCompactFilter"; }

private:
  // Column family for this compaction filter
  const uint32_t m_cf_id;
  // Index id of the previous record
  mutable GL_INDEX_ID m_prev_index = {0, 0};
  // Number of rows deleted for the same index id
  mutable uint64 m_num_deleted = 0;
  // Current index id should be deleted or not (should be deleted if true)
  mutable bool m_should_delete = false;
};

class SeCompactFilterFactory : public storage::CompactionFilterFactory
{
public:
  SeCompactFilterFactory(const SeCompactFilterFactory &) = delete;
  SeCompactFilterFactory &
  operator=(const SeCompactFilterFactory &) = delete;
  SeCompactFilterFactory() {}

  ~SeCompactFilterFactory() {}

  const char *Name() const override { return "SeCompactFilterFactory"; }

  std::unique_ptr<storage::CompactionFilter> CreateCompactionFilter(
      const storage::CompactionFilter::Context &context) override {
    return std::unique_ptr<storage::CompactionFilter>(
        new SeCompactFilter(context.column_family_id));
  }
};

} //namespace smartengine
