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

#include "objstore.h"

#include "mysys/objstore/local.h"
#include "mysys/objstore/s3.h"
#include "mysys/objstore/aliyun_oss.h"

#include <filesystem>

namespace objstore {

const char *backup_snapshot_releasing = "releasing";
const char *backup_snapshot_recovering = "recovering";
const char *backup_snapshot_status_prefix = "se_snapshot_status_";

namespace fs = std::filesystem;

std::string remove_prefix(const std::string &str, const std::string &prefix) {
  int pos = str.find(prefix);
  if (pos == 0) {
    return str.substr(prefix.size());
  }
  return str;
}

/*
this function check whether a key is frist-level sub key in a s3/oss/...
directory (like `aws s3 ls ...`).

if prefix is "a", the following are first-level sub keys:
====
"a"
"ab"
"a/"
"aa"
"aa/"
"ab/"
====

if prefix is "a/", the following are first level sub keys:
====
"a/"
"a/b"
"a/b/"
====
and the following are not:
====
"aa/"
"a/b/c"
====
*/
bool is_first_level_sub_key(const std::string_view &key,
                            const std::string_view &prefix) {
  if (key.size() >= prefix.size()) {
    if (key.find('/', prefix.size()) == std::string::npos ||
        key.find('/', prefix.size()) == key.size() - 1) {
      return true;
    }
  }
  return false;
}

int mkdir_p(std::string_view path) {
  std::error_code errcode;
  fs::create_directories(path, errcode);
  // bool created = fs::create_directories(path, errcode);
  //  assert(created);
  return errcode.value();
}

int rm_f(std::string_view path) {
  std::error_code errcode;
  fs::remove_all(path, errcode);
  // bool deleted = fs::remove_all(path, errcode);
  // assert(deleted);
  return errcode.value();
}

void init_objstore_provider(const std::string_view &provider) {
  if (provider == "aws") {
    init_aws_api();
  } else if (provider == "aliyun") {
    init_aliyun_api();
  } else if (provider == "local") {
    // do nothing
  }
}

void cleanup_objstore_provider(ObjectStore *objstore) {
  if (objstore != nullptr) {
    std::string_view provider = objstore->get_provider();
    if (provider == "aws") {
      shutdown_aws_api();
    } else if (provider == "aliyun") {
      shutdown_aliyun_api();
    } else if (provider == "local") {
      // do nothing
    }
  }
}

Status ObjectStore::delete_directory(const std::string_view &bucket,
                                     const std::string_view &prefix) {
  std::string dir_prefix(prefix);
  if (!prefix.empty() && prefix.back() != '/') {
    dir_prefix.append("/");
  }
  std::string start_after;
  bool finished = false;
  std::vector<ObjectMeta> objects;
  while (!finished) {
    Status s =
        list_object(bucket, dir_prefix, true, start_after, finished, objects);
    if (!s.is_succ()) {
      return s;
    }
    if (!objects.empty()) {
      std::vector<std::string_view> object_keys;
      for (const ObjectMeta &object_meta : objects) {
        object_keys.emplace_back(object_meta.key);
      }
      s = delete_objects(bucket, object_keys);
      if (!s.is_succ()) {
        return s;
      }
      objects.clear();
    }
  }
  return Status();
}

Status ObjectStore::put_objects_from_dir(const std::string_view &src_dir,
                                         const std::string_view &dst_objstore_bucket,
                                         const std::string_view &dst_objstore_dir) {
  fs::path src_dir_path = fs::path(src_dir).lexically_normal();
  std::string dst_objstore_dir_path(dst_objstore_dir);
  if (!dst_objstore_dir_path.empty() && dst_objstore_dir_path.back() != '/') {
    dst_objstore_dir_path.append("/");
  }
  // check if local dir path exists
  if (!fs::exists(src_dir_path)) {
    std::string err_msg = src_dir_path.native() + " does not exist";
    return Status(Errors::SE_INVALID, EINVAL, err_msg.c_str());
  }
  // check if local dir path is a directory
  if (!fs::is_directory(src_dir_path)) {
    std::string err_msg = src_dir_path.native() + " is not a directory";
    return Status(Errors::SE_INVALID, ENOTDIR, err_msg.c_str());
  }

  if (!dst_objstore_dir_path.empty()) {
    Status s = put_object(dst_objstore_bucket, dst_objstore_dir_path, "");
    if (!s.is_succ()) {
      return s;
    }
  }
  for (const fs::directory_entry &entry :
       fs::recursive_directory_iterator(src_dir_path)) {
    std::string key = fs::relative(entry.path(), src_dir_path);
    if (!dst_objstore_dir_path.empty()) {
      key = dst_objstore_dir_path + key;
    }

    if (entry.is_directory()) {
      if (key.back() != '/') {
        key.append("/");
      }
      Status s = put_object(dst_objstore_bucket, key, "");
      if (!s.is_succ()) {
        return s;
      }
    } else if (entry.is_regular_file()) {
      std::string abs_src_file_path = fs::absolute(entry.path());
      Status s =
          put_object_from_file(dst_objstore_bucket, key, abs_src_file_path);
      if (!s.is_succ()) {
        return s;
      }
    }
  }
  return Status();
}

Status ObjectStore::get_objects_to_dir(const std::string_view &src_objstore_bucket,
                                       const std::string_view &src_objstore_dir,
                                       const std::string_view &dst_dir) {
  fs::path dst_dir_path = fs::path(dst_dir).lexically_normal();
  std::string src_objstore_dir_path(src_objstore_dir);
  if (!src_objstore_dir_path.empty() && src_objstore_dir_path.back() != '/') {
    src_objstore_dir_path.append("/");
  }
  // check if the dst directory exists, if not, create it.
  if (!fs::exists(dst_dir_path)) {
    int ret = mkdir_p(dst_dir_path.native());
    if (ret != 0) {
      return Status(Errors::SE_IO_ERROR, ret, std::generic_category().message(ret));
    }
  }
  if (!fs::is_directory(dst_dir_path)) {
    std::string err_msg = dst_dir_path.native() + " is not a directory";
    return Status(Errors::SE_INVALID, ENOTDIR, err_msg.c_str());
  }

  bool finished = false;
  std::vector<ObjectMeta> object_metas;
  Status s;
  std::string start_after;
  fs::path last_parent_path;
  while (!finished) {
    s = list_object(src_objstore_bucket, src_objstore_dir_path, true,
                    start_after, finished, object_metas);
    if (!s.is_succ()) {
      return s;
    }
    fs::path abs_dst_dir_path = fs::absolute(dst_dir_path);
    for (const ObjectMeta &obj : object_metas) {
      std::string key = obj.key;
      if (!src_objstore_dir_path.empty()) {
        key = remove_prefix(key, src_objstore_dir_path);
      }
      fs::path dst_file_path = abs_dst_dir_path / key;

      if (obj.key.back() == '/') {
        // this is a sub directory of object store bucket
        if (!fs::exists(dst_file_path)) {
          int ret = mkdir_p(dst_file_path.native());
          if (ret != 0) {
            return Status(Errors::SE_IO_ERROR, ret,
                          std::generic_category().message(ret));
          }
        }
      } else {
        if (dst_file_path.parent_path() != last_parent_path &&
            !fs::exists(dst_file_path.parent_path())) {
          int ret = mkdir_p(dst_file_path.parent_path().native());
          if (ret != 0) {
            return Status(Errors::SE_IO_ERROR, ret,
                          std::generic_category().message(ret));
          }
          last_parent_path = dst_file_path.parent_path();
        }
        s = get_object_to_file(src_objstore_bucket, obj.key,
                               dst_file_path.native());
        if (!s.is_succ()) {
          return s;
        }
      }
    }
    object_metas.clear();
  }
  return Status();
}

ObjectStore *create_object_store(const std::string_view &provider,
                                 const std::string_view region,
                                 const std::string_view *endpoint,
                                 bool use_https, std::string &err_msg) {
  if (provider == "aws") {
    return create_s3_objstore(region, endpoint, use_https, err_msg);
  } else if (provider == "aliyun") {
    return create_aliyun_oss_objstore(region, endpoint, err_msg);
  } else if (provider == "local") {
    return create_local_objstore(region, endpoint, use_https);
  } else {
    return nullptr;
  }
}

ObjectStore *create_object_store_for_test(const std::string_view &provider,
                                          const std::string_view region,
                                          const std::string_view *endpoint,
                                          bool use_https,
                                          const std::string_view bucket_dir,
                                          std::string &err_msg) {
  if (provider == "aws") {
    return create_s3_objstore_for_test(region, endpoint, use_https, bucket_dir,
                                       err_msg);
  } else if (provider == "local") {
    return create_local_objstore(region, endpoint, use_https);
  } else if (provider == "aliyun") {
    return create_aliyun_oss_objstore_for_test(region, endpoint, bucket_dir,
                                               err_msg);
  } else {
    return nullptr;
  }
}

void destroy_object_store(ObjectStore *obj_store) {
  // provide a register mechanism to create/destroy the object store
  delete obj_store;
}

int init_object_store(const std::string_view &provider,
                      const std::string_view &region,
                      const std::string_view &bucket_dir, std::string &err_msg,
                      ObjectStore *&objstore) {
  Status status;
  int ret = 0;
  init_objstore_provider(provider);
  objstore = create_object_store(provider, region, nullptr, false, err_msg);
  if (objstore == nullptr) {
    ret = 1;
    cleanup_objstore_provider(objstore);
    return ret;
  }
  return 0;
}

void cleanup_object_store(ObjectStore *&objstore) {
  cleanup_objstore_provider(objstore);
  delete objstore;
}

int tryObjectStoreFileLock(ObjectStore *objstore,
                           const std::string_view bucket_dir,
                           std::string &err_msg, std::string &key,
                           std::string &data,
                           const char *backup_snapshot_status) {
  int ret = 0;
  if (objstore == nullptr) {
    err_msg = "object store is not initialized";
    ret = Errors::SE_INVALID;
  } else {
    bool forbid_overwrite = true;
    Status status =
        objstore->put_object(bucket_dir, key, data, forbid_overwrite);
    if (status.error_code() == Errors::SE_OBJECT_FORBID_OVERWRITE) {
      // the lock file already exists
      std::string current_status;
      status = objstore->get_object(bucket_dir, key, current_status);
      if (status.error_code() == Errors::SE_NO_SUCH_KEY) {
        err_msg = "lock file may be deleted by another node";
        ret = Errors::SE_MULTI_DATA_NODE_DETECTED;
      } else if (!status.is_succ()) {
        err_msg = status.error_message();
        ret = status.error_code();
      } else {
        if (current_status == backup_snapshot_status) {
          // lock file exists and with the same status, this may happen when
          // a data node was killed/crashed after the lock file written. since
          // we don't support muliple data nodes at now, we treat this as an
          // normal case.
        } else {
          err_msg = std::string() + "lock file exists but in " +
                    current_status + " status";
          ret = 1;
        }
      }
    } else if (!status.is_succ()) {
      err_msg = status.error_message();
      ret = status.error_code();
    } else {
      // lock file is created successfully
    }
  }
  return ret;
}

int tryBackupStatusLock(uint64_t auto_increment_id, ObjectStore *objstore,
                        const std::string_view bucket_dir, std::string &err_msg,
                        const char *backup_snapshot_status) {
  std::string key =
      backup_snapshot_status_prefix + std::to_string(auto_increment_id);
  std::string data = backup_snapshot_status;
  return tryObjectStoreFileLock(objstore, bucket_dir, err_msg, key, data,
                                backup_snapshot_status);
}

int tryBackupRecoveringLock(uint64_t auto_increment_id, ObjectStore *objstore,
                            const std::string_view bucket_dir,
                            std::string &err_msg) {
  return tryBackupStatusLock(auto_increment_id, objstore, bucket_dir, err_msg,
                             backup_snapshot_recovering);
}

// backup releasing lock is for the case that mulitple instances of
// smartengine are running, and one instance is doing recovering based on a
// snapshot X, and the other instance is doing release snapshot X. we use this
// lock to prevent a snapshot being released when it is being used.
int tryBackupReleasingLock(uint64_t auto_increment_id, ObjectStore *objstore,
                           const std::string_view bucket_dir,
                           std::string &err_msg) {
  return tryBackupStatusLock(auto_increment_id, objstore, bucket_dir, err_msg,
                             backup_snapshot_releasing);
}

int updateBackupStautsLockFile(uint64_t auto_increment_id,
                               ObjectStore *objstore,
                               const std::string_view bucket_dir,
                               const std::string_view expected_status,
                               const std::string_view new_status,
                               std::string &err_msg) {
  Status status;
  int ret = 0;
  if (objstore == nullptr) {
    err_msg = "object store is not initialized";
    return Errors::SE_INVALID;
  }
  std::string key =
      backup_snapshot_status_prefix + std::to_string(auto_increment_id);
  std::string value;
  status = objstore->get_object(bucket_dir, key, value);
  if (!status.is_succ()) {
    err_msg = status.error_message();
    ret = status.error_code();
  } else if (value != expected_status) {
    err_msg = std::string("snapshot is not in ") + expected_status.data() +
              " status, actual status is " + value;
    ret = 1;
  } else {
    status = objstore->put_object(bucket_dir, key, new_status, false);
    if (!status.is_succ()) {
      err_msg = status.error_message();
      ret = status.error_code();
    }
  }
  return ret;
}

int removeBackupStatusLockFile(uint64_t auto_increment_id,
                               ObjectStore *objstore,
                               const std::string_view bucket_dir,
                               const std::string_view expected_status,
                               std::string &err_msg) {
  Status status;
  int ret = 0;
  if (objstore == nullptr) {
    err_msg = "object store is not initialized";
    ret = Errors::SE_INVALID;
    return ret;
  }

  std::string key =
      backup_snapshot_status_prefix + std::to_string(auto_increment_id);
  std::string value;
  status = objstore->get_object(bucket_dir, key, value);
  if (!status.is_succ()) {
    err_msg = status.error_message();
    ret = status.error_code();
  } else if (value != expected_status) {
    err_msg = std::string("snapshot is not in ") + expected_status.data() +
              " status, actual status is " + value;
    ret = 1;
  } else {
    status = objstore->delete_object(bucket_dir, key);
    if (!status.is_succ()) {
      err_msg = status.error_message();
      ret = status.error_code();
    }
  }
  return ret;
}

int removeObsoletedBackupStatusLockFiles(ObjectStore *objstore,
                                         const std::string_view bucket_dir,
                                         uint64_t auto_increment_id_in_use,
                                         std::string &err_msg) {
  Status status;
  int ret = 0;
  if (objstore == nullptr) {
    ret = 1;
    err_msg = "object store is not initialized";
    return ret;
  }

  std::string start_after;
  bool finished = false;
  std::vector<ObjectMeta> objects;
  std::string lock_file_to_exclude =
      backup_snapshot_status_prefix + std::to_string(auto_increment_id_in_use);
  do {
    status = objstore->list_object(bucket_dir, backup_snapshot_status_prefix,
                                   false, start_after, finished, objects);
    if (!status.is_succ()) {
      ret = status.error_code();
      err_msg = status.error_message();
    } else {
      std::string value;
      std::vector<std::string_view> object_keys;
      for (const auto &object : objects) {
        if (object.key == lock_file_to_exclude) {
          continue;
        }
        object_keys.emplace_back(object.key);
      }

      status = objstore->delete_objects(bucket_dir, object_keys);
      if (!status.is_succ()) {
        ret = status.error_code();
        err_msg = status.error_message();
      }
    }
  } while (!finished && ret == 0);

  return ret;
}

}  // namespace objstore