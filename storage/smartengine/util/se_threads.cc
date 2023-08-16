/*
   Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Portions Copyright (c) 2015-Present, Facebook, Inc.
   Portions Copyright (c) 2012, Monty Program Ab

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

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation // gcc: Class implementation
#endif

#include "se_threads.h"
#include <sql_class.h>
#include "sql/mysqld_thd_manager.h"
#include "handler/se_hton.h"
#include "dict/se_dict_util.h"

namespace smartengine {

extern int32_t se_shrink_table_space;

extern bool opt_purge_invalid_subtable_bg;

extern uint32_t se_flush_log_at_trx_commit;

extern common::DBOptions se_db_options;

void *SeThread::thread_func(void *const thread_ptr)
{
  assert(thread_ptr != nullptr);
  //create and attach thd to current thread
  //for async commit context switch
  //backgroun thread may do async_commit for forground threads
  //so we need to create THD and set to treads attr
  my_thread_init();
  THD* thd =  new (std::nothrow) THD;
  if (nullptr != thd) {
#ifndef NDEBUG
    set_my_thread_var_id(thd->thread_id());
#endif
    thd->thread_stack=(char*)&thd;
    thd->store_globals();
  }
  SeThread *thread = static_cast<SeThread *>(thread_ptr);
  if (!thread->m_run_once.exchange(true)) {
    thread->setname();
    thread->run();
    thread->uninit();
  }
  if (nullptr != thd) 
    delete thd;
  my_thread_end();
  return nullptr;
}

void SeThread::init(
#ifdef HAVE_PSI_INTERFACE
    my_core::PSI_mutex_key stop_bg_psi_mutex_key,
    my_core::PSI_cond_key stop_bg_psi_cond_key
#endif
    )
{
  assert(!m_run_once);
  mysql_mutex_init(stop_bg_psi_mutex_key, &m_signal_mutex, MY_MUTEX_INIT_FAST);
  mysql_cond_init(stop_bg_psi_cond_key, &m_signal_cond);
}

void SeThread::uninit()
{
  mysql_mutex_destroy(&m_signal_mutex);
  mysql_cond_destroy(&m_signal_cond);
}

int SeThread::create_thread(const std::string &thread_name
#ifdef HAVE_PSI_INTERFACE
                              ,
                              PSI_thread_key background_psi_thread_key
#endif
                              )
{
  // Make a copy of the name so we can return without worrying that the
  // caller will free the memory
  m_name = thread_name;

  return mysql_thread_create(background_psi_thread_key, &m_handle, nullptr,
                             thread_func, this);
}

void SeThread::signal(const bool &stop_thread)
{
  SE_MUTEX_LOCK_CHECK(m_signal_mutex);

  if (stop_thread) {
    m_stop = true;
  }

  mysql_cond_signal(&m_signal_cond);

  SE_MUTEX_UNLOCK_CHECK(m_signal_mutex);
}

/*
  Background thread's main logic
*/
void SeBackgroundThread::run()
{
  // How many seconds to wait till flushing the WAL next time.
  const int WAKE_UP_INTERVAL = 1;

  timespec ts_next_sync;
  clock_gettime(CLOCK_REALTIME, &ts_next_sync);
  ts_next_sync.tv_sec += WAKE_UP_INTERVAL;

  for (;;) {
    // Wait until the next timeout or until we receive a signal to stop the
    // thread. Request to stop the thread should only be triggered when the
    // storage engine is being unloaded.
    SE_MUTEX_LOCK_CHECK(m_signal_mutex);
    const auto ret MY_ATTRIBUTE((__unused__)) =
        mysql_cond_timedwait(&m_signal_cond, &m_signal_mutex, &ts_next_sync);

    // Check that we receive only the expected error codes.
    assert(ret == 0 || ret == ETIMEDOUT);
    const bool local_stop = m_stop;
    const bool local_save_stats = m_save_stats;
    bool release = false;
    reset();
    // backup snapshot timer is set
    if (counter_ > 0) {
      counter_--;
      if (counter_ <= 0) {
        release = true;
        assert(0 == counter_);
      }
    }
    SE_MUTEX_UNLOCK_CHECK(m_signal_mutex);

    if (local_stop) {
      // If we're here then that's because condition variable was signaled by
      // another thread and we're shutting down. Break out the loop to make
      // sure that shutdown thread can proceed.
      break;
    }

    // This path should be taken only when the timer expired.
    assert(ret == ETIMEDOUT);

    if (local_save_stats) {
      ddl_manager.persist_stats();
    }

    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    // Flush the WAL.
    if (se_db && se_flush_log_at_trx_commit == 2) {
      assert(!se_db_options.allow_mmap_writes);
      const common::Status s = se_db->SyncWAL();
      if (!s.ok()) {
        se_handle_io_error(s, SE_IO_ERROR_BG_THREAD);
      }
    }

    // release the backup snapshot
    if (release) {
      //common::Status s = se_db->release_backup_snapshot();
      //if (!s.ok()) {
        //// FIXME if failed
        //sql_print_error("ERROR: release the backup_snapshot failed\n");
      //} else {
        //if (unlink(backup_snapshot_file_.c_str()) != 0) {
           //sql_print_error("ERROR: delete the backup snapshot file failed %s\n", backup_snapshot_file_.c_str());
           //s = common::Status::IOError();
        //}
      //}
    }

    // Set the next timestamp for mysql_cond_timedwait() (which ends up calling
    // pthread_cond_timedwait()) to wait on.
    ts_next_sync.tv_sec = ts.tv_sec + WAKE_UP_INTERVAL;
  }

  // save remaining stats which might've left unsaved
  ddl_manager.persist_stats();
}

// purge garbaged subtables due to commit-in-the-middle
static void purge_invalid_subtables()
{
  static timespec start = {0, 0};
  if (start.tv_sec == 0) {
    clock_gettime(CLOCK_REALTIME, &start);
  } else {
    timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    // scan invalid subtables per DURATION_PER_RUN seconds
    if (now.tv_sec < start.tv_sec + SeDropIndexThread::purge_schedule_interval) return;

    start = now;
  }

  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
  THD* thd = new THD;
  thd->thread_stack = (char *)&thd;
  thd->slave_thread = 0;
  thd->set_command(COM_DAEMON);
  thd->set_new_thread_id();
  thd->store_globals();
  thd_manager->add_thd(thd);
  thd_manager->inc_thread_running();

  // get subtable ids from engine first
  std::vector<int32_t> ids_from_engine = cf_manager.get_subtable_ids();

  // collect id of subtables from global data dictionary
  std::set<uint32_t> ids_from_dd;
  if (!SeDdHelper::get_se_subtable_ids(thd, purge_acquire_lock_timeout,
                                               ids_from_dd)) {
    // pre-defined subtables for internal usage
    ids_from_dd.insert(0);                           // default cf
    ids_from_dd.insert(DEFAULT_SYSTEM_SUBTABLE_ID);  // system cf

    // saved invalid subtable ids for last run
    static std::set<uint32_t> last_invalid_subtables;
    // collect invalid id of invalid subtables
    std::set<uint32_t> current_invalid_subtables;
    for (auto& id : ids_from_engine) {
      uint32_t subtable_id = id;
      GL_INDEX_ID gl_index_id = {.cf_id = subtable_id,
                                 .index_id = subtable_id};
      // filter out create/drop-ongoing index
      if (ids_from_dd.find(subtable_id) == ids_from_dd.end() &&
          ddl_manager.can_purge_subtable(thd, gl_index_id)) {
        current_invalid_subtables.insert(subtable_id);
      }
    }

    if (!last_invalid_subtables.empty() && !current_invalid_subtables.empty()) {
      // purge invalid subtables present during both last and this run
      auto wb = dict_manager.begin();
      auto write_batch = wb.get();
      for (auto& subtable_id : last_invalid_subtables) {
        auto it = current_invalid_subtables.find(subtable_id);
        if (it != current_invalid_subtables.end()) {
          XHANDLER_LOG(WARN, "SE: going to purge trashy subtable",
                       K(subtable_id));
          dict_manager.delete_index_info(write_batch,
                            {.cf_id=subtable_id, .index_id = subtable_id});
          auto cfh = cf_manager.get_cf(subtable_id);
          if (cfh != nullptr) {
            se_db->Flush(common::FlushOptions(), cfh);
            cf_manager.drop_cf(se_db, subtable_id);
          }
          current_invalid_subtables.erase(it);
        }
      }
      dict_manager.commit(write_batch);
    }
    last_invalid_subtables = current_invalid_subtables;
  } else {
    XHANDLER_LOG(WARN, "SE: failed to collect subtable id from DD!");
  }

  thd->release_resources();
  thd_manager->remove_thd(thd);
  thd_manager->dec_thread_running();
  delete thd;
}

/*
  Drop index thread's main logic
*/
void SeDropIndexThread::run()
{
  SE_MUTEX_LOCK_CHECK(m_signal_mutex);

  for (;;) {
    // The stop flag might be set by shutdown command
    // after drop_index_thread releases signal_mutex
    // (i.e. while executing expensive Seek()). To prevent drop_index_thread
    // from entering long cond_timedwait, checking if stop flag
    // is true or not is needed, with drop_index_interrupt_mutex held.
    if (m_stop) {
      break;
    }

    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += SeDropIndexThread::purge_schedule_interval;

    // enter wait
    if (m_run_) {
      m_run_ = false;
    }

    const auto ret MY_ATTRIBUTE((__unused__)) =
        mysql_cond_timedwait(&m_signal_cond, &m_signal_mutex, &ts);
    if (m_stop) {
      break;
    }
    // FIXME: shrink exent space cann't do with drop index
    if (se_shrink_table_space >= 0) {
      sql_print_information("Wait for shrink extent space finish");
      continue;
    }
    // make sure, no program error is returned
    assert(ret == 0 || ret == ETIMEDOUT);
    // may be work
    m_run_ = true;
    SE_MUTEX_UNLOCK_CHECK(m_signal_mutex);

    if (opt_purge_invalid_subtable_bg)
      purge_invalid_subtables();

    SE_MUTEX_LOCK_CHECK(m_signal_mutex);
    m_run_ = false;
  }

  SE_MUTEX_UNLOCK_CHECK(m_signal_mutex);
}

} //namespace smartengine
