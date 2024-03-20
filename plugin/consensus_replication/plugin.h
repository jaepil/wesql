/* Copyright (c) 2014, 2022, Oracle and/or its affiliates.

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

#ifndef CONSENSUS_PLUGIN_INCLUDE
#define CONSENSUS_PLUGIN_INCLUDE

#include <mysql/plugin.h>
#include <mysql/plugin_consensus_replication.h>

#define PLUGIN_AUTHOR "Apecloud Corporation"
#define PLUGIN_VERSION 0x0100

class Checkable_rwlock;

/** Flag that indicates that there are observers (for performance)*/

// Plugin global methods
SERVICE_TYPE(registry) * get_plugin_registry();

bool plugin_is_consensus_replication_enabled();

// Plugin public methods
int plugin_consensus_replication_init(MYSQL_PLUGIN plugin_info);
int plugin_consensus_replication_deinit(void *p);
int plugin_consensus_replication_stop(char **error_message = nullptr);
bool plugin_is_consensus_replication_running();

bool acquire_transaction_control_services();
bool release_transaction_control_services();

#endif /* CONSENSUS_PLUGIN_INCLUDE */
