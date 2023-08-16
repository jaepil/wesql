/*
   Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2014, SkySQL Ab

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

#include "se_cf_manager.h"

namespace smartengine
{
/* Check if ColumnFamily name says it's a reverse-ordered CF */
bool SeSubtableManager::is_cf_name_reverse(const char *const name)
{
  /* nullptr means the default CF is used.. (TODO: can the default CF be
   * reverse?) */
  if (name && !strncmp(name, "rev:", 4))
    return true;
  else
    return false;
}

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key ex_key_cfm;
#endif

void SeSubtableManager::init(SeSubtableOptions *const cf_options,
                             std::vector<db::ColumnFamilyHandle *> *const handles,
                             SeDdlLogManager *ddl_log_manager)
{
  mysql_mutex_init(ex_key_cfm, &m_mutex, MY_MUTEX_INIT_FAST);

  assert(cf_options != nullptr);
  assert(handles != nullptr);
  assert(ddl_log_manager != nullptr);
  assert(handles->size() > 0);

  m_cf_options = cf_options;
  m_ddl_log_manager = ddl_log_manager;

  for (auto cfh : *handles) {
    assert(cfh != nullptr);
    //m_cf_name_map[cfh->GetName()] = cfh;
    m_subtable_id_map[cfh->GetID()] = cfh;
  }
}

void SeSubtableManager::cleanup()
{
  for (auto it : m_subtable_id_map) {
    MOD_DELETE_OBJECT(ColumnFamilyHandle, it.second);
  }
  mysql_mutex_destroy(&m_mutex);
}

/**
  Generate Column Family name for per-index column families

  @param res  OUT  Column Family name
*/

void SeSubtableManager::get_per_index_cf_name(const std::string &db_table_name,
                                              const char *const index_name,
                                              std::string *const res)
{
  assert(index_name != nullptr);
  assert(res != nullptr);

  *res = db_table_name + "." + index_name;
}


/**
  create subtable physically, for rollback, we need write ddl_log.
  @param[in] se_db, se DB object
  @param[in] xa_batch, all modify data cache in user transaction, be part of xa transaction, for create system subtable, no need write log and xa_batch is nullptr.
  @param[in, thread_id, session thread_id
  @param[in], subtable_name
  @param[in/out], cf_handle, handler to operate subtable
*/
bool SeSubtableManager::create_subtable(db::DB *const se_db,
                                        db::WriteBatch *const xa_batch,
                                        ulong thread_id,
                                        uint index_number,
                                        const common::ColumnFamilyOptions &cf_options,
                                        const char *subtable_name,
                                        bool create_table_space,
                                        int64_t &table_space_id,
                                        db::ColumnFamilyHandle **cf_handle)
{
  /** create subtable physically */
  const std::string subtable_name_str(subtable_name);
  common::ColumnFamilyOptions opts;
  m_cf_options->get_cf_options(subtable_name_str, &opts);

  sql_print_information(
      "SE: creating subtable: index_number(%d), subtable_name(%s)",
      index_number, subtable_name_str.c_str());

  /** write subtable log */
  if (xa_batch != nullptr) {
    if (m_ddl_log_manager->write_drop_subtable_log(
            xa_batch, index_number, thread_id, false)) {
      sql_print_error("SE: write drop_subtable_log error");
      return true;
    }
  }

  DBUG_EXECUTE_IF("ddl_log_crash_after_drop_subtable_log", DBUG_SUICIDE(););
  struct db::CreateSubTableArgs args(index_number, cf_options, create_table_space, table_space_id);
  const common::Status s =
      se_db->CreateColumnFamily(args, cf_handle);
  if (s.ok()) {
    m_subtable_id_map[(*cf_handle)->GetID()] = *cf_handle;
    if (create_table_space) {
      table_space_id = (reinterpret_cast<db::ColumnFamilyHandleImpl *>(*cf_handle))->cfd()->get_table_space_id();
    }
  }


  sql_print_information("SE: creating subtable successfully. "
                        "index_number(%d), subtable_id(%d), subtable_name(%s)",
                        index_number, (*cf_handle)->GetID(), subtable_name_str.c_str());
  return false;
}

/*
  @brief
  Find column family by name. If it doesn't exist, create it

  @detail
    See SeSubtableManager::get_cf
*/
db::ColumnFamilyHandle *SeSubtableManager::get_or_create_cf(
    db::DB *const se_db,
    db::WriteBatch *write_batch,
    ulong thread_id,
    uint subtable_id,
    const char *cf_name,
    const std::string &db_table_name,
    const char *const index_name,
    bool *const is_automatic,
    const common::ColumnFamilyOptions &cf_options,
    bool create_table_space,
    int64_t &table_space_id)
{

  assert(se_db != nullptr);

  SE_MUTEX_LOCK_CHECK(m_mutex);

  db::ColumnFamilyHandle *cf_handle = nullptr;
  const auto it = m_subtable_id_map.find(subtable_id);
  if (it != m_subtable_id_map.end())
    cf_handle = it->second;
  else {
    if (!create_subtable(se_db,
                         write_batch,
                         thread_id,
                         subtable_id,
                         cf_options,
                         cf_name,
                         create_table_space,
                         table_space_id,
                         &cf_handle)) {
      sql_print_information("SE: create subtable successfully, thread_id(%ld), "
                      "subtable_id(%d), cf_name(%s)",
                      thread_id, cf_handle->GetID(), cf_name);
    } else {
      cf_handle = nullptr;
      sql_print_error(
          "SE: create subtable failed, %ld, %d, %s", thread_id,
          subtable_id, cf_name);
    }
  }

  SE_MUTEX_UNLOCK_CHECK(m_mutex);

  return cf_handle;
}

/*
 * drop cf specified by cf_id
 */
void SeSubtableManager::drop_cf(db::DB *const se_db, const uint32_t cf_id)
{
  assert(se_db != nullptr);
  db::ColumnFamilyHandle *cf_handle;

  SE_MUTEX_LOCK_CHECK(m_mutex);

  const auto id_it = m_subtable_id_map.find(cf_id);
  if (id_it != m_subtable_id_map.end()) {
    cf_handle = id_it->second;
    const common::Status s = se_db->DropColumnFamily(cf_handle);
    if (!s.ok()) {
      sql_print_information("SE: drop subtable failed %s", s.getState());
    }
    // remove it from the maps
    m_subtable_id_map.erase(id_it);
    MOD_DELETE_OBJECT(ColumnFamilyHandle, cf_handle);
  } else {
    sql_print_information("SE: can't find the subtable(%d) to drop", cf_id);
  }

  SE_MUTEX_UNLOCK_CHECK(m_mutex);
}

db::ColumnFamilyHandle *SeSubtableManager::get_cf(const uint32_t &id) const
{
  db::ColumnFamilyHandle *cf_handle = nullptr;

  SE_MUTEX_LOCK_CHECK(m_mutex);
  const auto it = m_subtable_id_map.find(id);
  if (it != m_subtable_id_map.end())
    cf_handle = it->second;
  SE_MUTEX_UNLOCK_CHECK(m_mutex);

  return cf_handle;
}

std::vector<int32_t> SeSubtableManager::get_subtable_ids() const
{
  std::vector<int32_t> subtable_ids;

  SE_MUTEX_LOCK_CHECK(m_mutex);
  for (auto it : m_subtable_id_map) {
    subtable_ids.push_back(it.first);
  }
  SE_MUTEX_UNLOCK_CHECK(m_mutex);

  return subtable_ids;
}

std::vector<db::ColumnFamilyHandle *> SeSubtableManager::get_all_cf() const
{
  std::vector<db::ColumnFamilyHandle *> list;
  db::ColumnFamilyHandleImpl *handle_impl = nullptr;
  SE_MUTEX_LOCK_CHECK(m_mutex);

  for (auto it : m_subtable_id_map) {
    handle_impl = reinterpret_cast<db::ColumnFamilyHandleImpl *>(it.second);
    // ref the cfd avoid delete and the caller need delete it
    list.push_back(new db::ColumnFamilyHandleImpl(
                      handle_impl->cfd(), handle_impl->db(), handle_impl->mutex()));
  }

  SE_MUTEX_UNLOCK_CHECK(m_mutex);

  return list;
}

} //namespace smartengine
