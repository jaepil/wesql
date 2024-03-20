#ifndef CONSENSUS_PLUGIN_VARIABLES_INCLUDE
#define CONSENSUS_PLUGIN_VARIABLES_INCLUDE

#include <atomic>
#include <map>

#include "mysql/components/service.h"
#include "sql/server_component/mysql_transaction_delegate_control_imp.h"

/*
  Variables that have file context on plugin.cc

  All variables declared on this structure must be initialized
  on init() function.
*/
struct plugin_consensus_local_variables {
  MYSQL_PLUGIN plugin_info_ptr;
  unsigned int plugin_version;

  Checkable_rwlock *plugin_running_lock;
  Checkable_rwlock *plugin_stop_lock;
  std::atomic<bool> consensus_replication_running;

  SERVICE_TYPE(registry) * reg_srv;
  SERVICE_TYPE_NO_CONST(mysql_before_commit_transaction_control) *
      svc_mysql_before_commit_transaction_control;
  SERVICE_TYPE_NO_CONST(
      mysql_close_connection_of_binloggable_transaction_not_reached_commit) *
      svc_mysql_close_connection_of_binloggable_transaction_not_reached_commit;

  /*
    Initialize all variables, except mutexes.
  */
  void init() {
    plugin_info_ptr = nullptr;
    plugin_version = 0;

    plugin_running_lock = nullptr;
    plugin_stop_lock = nullptr;

    reg_srv = nullptr;
    svc_mysql_before_commit_transaction_control = nullptr;
    svc_mysql_close_connection_of_binloggable_transaction_not_reached_commit = nullptr;
  }
};

#endif /* CONSENSUS_PLUGIN_VARIABLES_INCLUDE */
