/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxyEngine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxyEngine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef CONSENSUS_RECOVERY_MANAGER_INCLUDE
#define CONSENSUS_RECOVERY_MANAGER_INCLUDE

// #include "my_global.h"
#include "sql/binlog.h"
#include "sql/handler.h"  // XA_recover_txn
#include "sql/xa.h"

#include <atomic>
#include <map>
#include <vector>

class ConsensusRecoveryManager {
 public:
  ConsensusRecoveryManager() {}
  ~ConsensusRecoveryManager() { clear_all_map(); }

  // for recover
  void add_trx_to_binlog_map(my_xid xid, uint64 current_index,
                             uint64 current_term);
  void add_trx_to_binlog_xa_map(XID *xid, bool second_phase,
                                uint64 current_index, uint64 current_term);
  void clear_all_map();

  // reconstruct trx list by trx list from storage engine
  uint64 reconstruct_binlog_trx_list(Xid_commit_list &commit_list,
                                     Xa_state_list &xa_list,
                                     Xa_state_list::list &se_xa_map,
                                     Xa_state_list::list &xa_map);

  uint64 max_committed_index{0};

 private:
  uint64 recover_term{0};
  std::map<my_xid, uint64> total_trx_index_map;
  std::map<std::string, uint64>
      total_xa_trx_index_map;  //<XID+[second_phase], consensusIndex> for save
                               //relation between
                               // XID and consensus index when recovering
};

#endif
