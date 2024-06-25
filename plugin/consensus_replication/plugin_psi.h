/* Copyright (c) 2015, 2022, Oracle and/or its affiliates.

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

#ifndef CONSENSUS_PLUGIN_PSI_INCLUDED
#define CONSENSUS_PLUGIN_PSI_INCLUDED

#include "mysys/mysys_priv.h"

#ifdef HAVE_PSI_INTERFACE
/*
  Register the psi keys for mutexes, conditions, threads and rwlocks
*/
void register_all_consensus_replication_psi_keys();
#endif /* HAVE_PSI_INTERFACE */

/* clang-format off */
extern PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_index;
extern PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_term_lock;
extern PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_truncate_lock;
extern PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_apply_thread_lock;
extern PSI_mutex_key key_CONSENSUSLOG_LOCK_Consensus_stage_change;

extern PSI_mutex_key key_fifo_cache_cleaner;

extern PSI_rwlock_key key_rwlock_plugin_running;
extern PSI_rwlock_key key_rwlock_plugin_stop;
extern PSI_rwlock_key key_rwlock_ConsensusLog_status_lock;
extern PSI_rwlock_key key_rwlock_ConsensusLog_commit_lock;
extern PSI_rwlock_key key_rwlock_ConsensusLog_log_cache_lock;
extern PSI_rwlock_key key_rwlock_ConsensusLog_prefetch_channels_hash;

extern PSI_cond_key key_COND_ConsensusLog_catchup;
extern PSI_cond_key key_COND_Consensus_state_change;
extern PSI_cond_key key_COND_prefetch_reuqest;

extern PSI_thread_key key_thread_consensus_stage_change;
extern PSI_thread_key key_thread_prefetch;
extern PSI_thread_key key_thread_cleaner;

extern PSI_memory_key key_memory_ConsensusLogManager;
extern PSI_memory_key key_memory_ConsensusPreFetchManager;

/* clang-format on */

#endif /* CONSENSUS_PLUGIN_PSI_INCLUDED */
