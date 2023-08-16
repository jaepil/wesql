/*
   Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2014, SkySQL Ab

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

#include <string>
#include <unordered_map>
#include <memory>
#include "util/se_comparator.h"
#include "smartengine/table.h"
#include "smartengine/utilities/options_util.h"

namespace smartengine {

/*
  Per-column family options configs.

  Per-column family option can be set
  - Globally (the same value applies to all column families)
  - Per column family: there is a {cf_name -> value} map,
    and also there is a default value which applies to column
    families not found in the map.
*/
class SeSubtableOptions {
public:
  SeSubtableOptions(const SeSubtableOptions &) = delete;
  SeSubtableOptions &operator=(const SeSubtableOptions &) = delete;
  SeSubtableOptions() = default;

  void get(const std::string &cf_name, common::ColumnFamilyOptions *const opts);

  bool init(const table::BlockBasedTableOptions &table_options,
            std::shared_ptr<table::TablePropertiesCollectorFactory> prop_coll_factory,
            const char *const default_cf_options,
            const char *const override_cf_options);

  bool init(const table::BlockBasedTableOptions &table_options,
            std::shared_ptr<table::TablePropertiesCollectorFactory> prop_coll_factory,
            const common::ColumnFamilyOptions& default_cf_options,
            const char *const override_cf_options);

  const common::ColumnFamilyOptions &get_defaults() const { return m_default_cf_opts; }

  static const util::Comparator *get_cf_comparator(const std::string &cf_name);

  void get_cf_options(const std::string &cf_name, common::ColumnFamilyOptions *const opts);

private:
  bool set_default(const std::string &default_config);
  bool set_override(const std::string &overide_config);

  /* Helper string manipulation functions */
  static void skip_spaces(const std::string &input, size_t *const pos);
  static bool find_column_family(const std::string &input,
                                 size_t *const pos,
                                 std::string *const key);
  static bool find_options(const std::string &input,
                           size_t *const pos,
                           std::string *const options);
  static bool find_cf_options_pair(const std::string &input,
                                   size_t *const pos,
                                   std::string *const cf,
                                   std::string *const opt_str);

private:
  static SePrimaryKeyComparator s_pk_comparator;
  static SePrimaryKeyReverseComparator s_rev_pk_comparator;

  typedef std::unordered_map<std::string, std::string> Name_to_config_t;

  /* CF name -> value map */
  Name_to_config_t m_name_map;

  /* The default value (if there is only one value, it is stored here) */
  std::string m_default_config;

  common::ColumnFamilyOptions m_default_cf_opts;
};

} //namespace smartengine
