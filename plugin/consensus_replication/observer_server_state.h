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
