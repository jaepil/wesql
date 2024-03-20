--connect (conn1,127.0.0.1,root,,test,$MYPORT_1)
--connect (conn2,127.0.0.1,root,,test,$MYPORT_1)
--connect (conn3,127.0.0.1,root,,test,$MYPORT_1)

--connect (conn,127.0.0.1,root,,test,$MYPORT_2)
--connection conn
let $master_uuid2= `SELECT @@GLOBAL.SERVER_UUID`;
--disconnect conn
--connect (conn,127.0.0.1,root,,test,$MYPORT_3)
--connection conn
let $master_uuid3= `SELECT @@GLOBAL.SERVER_UUID`;
--disconnect conn
# leader transfer at sync point $sync_point

--connection conn1
let $master_uuid1= `SELECT @@GLOBAL.SERVER_UUID`;
create table t1(id int);
--eval SET debug_sync = '$sync_point SIGNAL step WAIT_FOR continue'
--send insert into t1 values (1)

--connection conn2
SET debug_sync = 'now WAIT_FOR step';
#change consensus_leader to 2;
--replace_result $PAXOS_PORT_2 PAXOS_PORT_2
eval call dbms_consensus.change_leader("127.0.0.1:$PAXOS_PORT_2");

--connection conn3
# should fail
--send insert into t1 values(2)
--connection conn2
--let $leader_connect=
--source include/wait_leader_change.inc

--connection conn2
SET debug_sync = "now SIGNAL continue";
let $wait_condition= select count(*)=0 from information_schema.wesql_cluster_local where role='leader';
--source include/wait_condition.inc
if ($leader_connect == connection2)
{
    --connect (conn4,127.0.0.1,root,,test,$MYPORT_2)
    --connection conn4
}
if ($leader_connect == connection3)
{
    --connect (conn4,127.0.0.1,root,,test,$MYPORT_3)
    --connection conn4
}
insert into t1 values(3);
--source include/wait_follower_catchup.inc
#change consensus_leader to 1;
--replace_result $PAXOS_PORT_1 PAXOS_PORT_1
eval call dbms_consensus.change_leader("127.0.0.1:$PAXOS_PORT_1");
let $wait_condition= select count(*)=0 from information_schema.wesql_cluster_local where role='leader';
--source include/wait_condition.inc

--connect (conn5,127.0.0.1,root,,test,$MYPORT_1)
--connection conn5
let $wait_condition= select count(*)=1 from information_schema.wesql_cluster_local where role='leader';
--source include/wait_condition.inc
let $wait_condition= select SERVER_READY_FOR_RW="Yes" from information_schema.wesql_cluster_local;
--source include/wait_condition.inc
let $cnt1= `select count(*) from t1`;

--connection conn4
let $cnt2= `select count(*) from t1`;
if ($cnt1 == $cnt2)
{
    echo "data consistent!";
}
if($cnt1 != $cnt2)
{
    echo "data not consistent!";
}
--disconnect conn4

--connection conn5
drop table t1;
--disconnect conn5
--disconnect conn1
--disconnect conn2
--disconnect conn3
