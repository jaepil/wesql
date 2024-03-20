/* Copyright (c) 2010, 2022, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef CONSENSUS_INFO_FACTORY_H
#define CONSENSUS_INFO_FACTORY_H

#include <sys/types.h>
#include <string>
#include <vector>

#include "my_bitmap.h"
#include "my_io.h"
#include "sql/rpl_info_handler.h"  // enum_return_check
#include "sql/rpl_info_factory.h"

class Consensus_info;
class Consensus_applier_info;
class Consensus_applier_worker;
class Relay_log_info;

class Consensus_info_factory : public Rpl_info_factory {
 public:
  static Consensus_info *create_consensus_info();
  static void init_consensus_repo_metadata();
  static Consensus_applier_info *create_consensus_applier_info();
  static void init_consensus_applier_repo_metadata();
  static Consensus_applier_worker *create_consensus_applier_woker(
      uint worker_id, bool on_recovery);
  static void init_consensus_applier_worker_repo_metadata();
  static bool reset_consensus_applier_workers(
      Consensus_applier_info *applier_info);

 private:
  static Rpl_info_factory::struct_table_data consensus_table_data;
  static Rpl_info_factory::struct_file_data consensus_file_data;
  static Rpl_info_factory::struct_table_data consensus_applier_table_data;
  static Rpl_info_factory::struct_file_data consensus_applier_file_data;
  static Rpl_info_factory::struct_table_data consensus_applier_worker_table_data;
  static Rpl_info_factory::struct_file_data consensus_applier_worker_file_data;
};

#endif
