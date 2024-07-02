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

#ifndef CONSENSUS_BINLOG_RECOVERY_INCLUDE
#define CONSENSUS_BINLOG_RECOVERY_INCLUDE

#include <string.h>
#include <sys/types.h>
#include <atomic>
#include <utility>

#include "libbinlogevents/include/binlog_event.h"  // enum_binlog_checksum_alg
#include "my_inttypes.h"
#include "sql/binlog_reader.h"  // Binlog_file_reader
#include "sql/mysqld.h"

class MYSQL_BIN_LOG;

extern int consensus_binlog_recovery(MYSQL_BIN_LOG *binlog,
                                     const char *ha_recover_end_file,
                                     my_off_t ha_recover_end_pos,
                                     char *recover_end_file,
                                     my_off_t &recover_end_pos);
extern uint64 get_applier_start_index();
extern int gtid_init_after_consensus_setup(uint64 last_index,
                                           const char *log_name = nullptr);

#endif
