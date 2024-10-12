/*
   Copyright (c) 2024, ApeCloud Inc Holding Limited.

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

#include <chrono>
#include <string>
#include <string_view>
#include "objstore.h"
namespace smartengine {
namespace objstore {

extern const std::chrono::milliseconds single_data_node_lock_renewal_interval;

using ObjectStore = ::objstore::ObjectStore;

bool is_lease_lock_owner_node();

// NOTICE: since we don't support multiple data nodes at now, we use a lock file
// to prevent multiple data nodes running at the same time. but this is not
// enough, since object store like s3 don't support IO fence feature, so there
// maybe are inflight IOs to the object store even if the lease time is expired.
// so this is just a temporary solution.
int try_single_data_node_lease_lock(ObjectStore *objstore,
                                    const std::string_view bucket_dir,
                                    std::string &err_msg,
                                    std::string &important_msg,
                                    bool &need_abort);

#ifndef NDEBUG

extern const char *single_data_node_lease_lock;
extern const char *single_data_node_lock_version_file_prefix;

extern const std::chrono::milliseconds single_data_node_lock_lease_timeout;
extern const std::chrono::milliseconds single_data_node_lock_renewal_timeout;

int get_single_data_node_lease_lock_expire_time(ObjectStore *objstore,
                                                const std::string_view bucket_dir,
                                                std::string &err_msg,
                                                std::chrono::milliseconds &expire_time);

int try_single_data_node_lease_lock_if_expired(ObjectStore *objstore,
                                               const std::string_view bucket_dir,
                                               std::string &err_msg,
                                               std::chrono::milliseconds &new_lease_time);

int renewal_single_data_node_lease_lock(ObjectStore *objstore,
                                        const std::string_view bucket_dir,
                                        std::chrono::milliseconds &new_lease_time,
                                        std::string &err_msg);

void TEST_unset_lease_lock_owner();

#endif

} // namespace objstore
} // namespace smartengine