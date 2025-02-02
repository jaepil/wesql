/*
   Portions Copyright (c) 2024, ApeCloud Inc Holding Limited

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

#ifndef CONSENSUS_OBSERVER_SERVER_STATE
#define CONSENSUS_OBSERVER_SERVER_STATE

#include "plugin.h"
#include "sql/replication.h"

/*
  DBMS lifecycle events observers.
*/
int consensus_replication_before_handle_connection(Server_state_param *param);

int consensus_replication_before_recovery(Server_state_param *param);

int consensus_replication_after_engine_recovery(Server_state_param *param);

int consensus_replication_after_recovery(Server_state_param *param);

int consensus_replication_before_server_shutdown(Server_state_param *param);

int consensus_replication_after_server_shutdown(Server_state_param *param);

extern Server_state_observer cr_server_state_observer;

#endif /* CONSENSUS_OBSERVER_SERVER_STATE */
