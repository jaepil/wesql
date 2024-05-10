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

#ifndef OBJSTORE_OBJSTORE_H_INCLUDED
#define OBJSTORE_OBJSTORE_H_INCLUDED

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace objstore {

// interfaces to manipulate object like aws S3.
// this interfaces will shield the differences between different object storages
// provider, such as aws S3, aliyun OSS, MinIO, etc.

enum Errors {
  SE_SUCCESS = 0,
  // smartengine internal error codes
  SE_IO_ERROR = 1,
  SE_INVALID,

  // object store errors, for s3, they are all not retryable errors:
  SE_BUCKET_ALREADY_EXISTS = 101,
  SE_BUCKET_ALREADY_OWNED_BY_YOU,
  SE_INVALID_OBJECT_STATE,
  SE_NO_SUCH_BUCKET,
  SE_NO_SUCH_KEY,
  SE_NO_SUCH_UPLOAD,
  SE_OBJECT_ALREADY_IN_ACTIVE_TIER,
  SE_OBJECT_NOT_IN_ACTIVE_TIER,

  // not-retryable generic errors, for s3, like:
  // INCOMPLETE_SIGNATURE
  // INVALID_ACTION
  // INVALID_CLIENT_TOKEN_ID
  // INVALID_PARAMETER_COMBINATION
  // INVALID_PARAMETER_VALUE
  // ...
  CLOUD_PROVIDER_UNRECOVERABLE_ERROR = 201,

  // retryable generic errors,  for s3, they are:
  // INTERNAL_FAILURE
  // REQUEST_EXPIRED
  //  SERVICE_UNAVAILABLE
  // THROTTLING
  // SLOW_DOWN
  // REQUEST_TIME_TOO_SKEWED
  // REQUEST_TIMEOUT
  CLOUD_PROVIDER_ERROR_RETRY_LIMIT_EXCEEDED,
};

class Status {
 public:
  Status() {}
  Status(Errors error_code, int cloud_provider_err_code,
         std::string_view error_msg)
      : error_code_(error_code),
        cloud_provider_err_code_(cloud_provider_err_code),
        error_msg_(error_msg) {}
  ~Status() = default;

  bool is_succ() const { return error_code_ == 0; }

  void set_error_code(Errors error_code) { error_code_ = error_code; }
  Errors error_code() const { return error_code_; }

  void set_cloud_provider_err_code(int cloud_provider_err_code) {
    cloud_provider_err_code_ = cloud_provider_err_code;
  }
  int cloud_provider_err_code() const { return cloud_provider_err_code_; }

  void set_error_msg(std::string_view error_msg) { error_msg_ = error_msg; }
  std::string_view error_message() const { return error_msg_; }

 private:
  Errors error_code_{SE_SUCCESS};
  int cloud_provider_err_code_{0};
  std::string error_msg_;
};

struct ObjectMeta {
  std::string key;
  int64_t last_modified; // timestamp in milliseconds since epoch.
  long long size;        // body size
};

class ObjectStore {
 public:
  virtual ~ObjectStore() = default;

  virtual Status create_bucket(const std::string_view &bucket) = 0;

  virtual Status delete_bucket(const std::string_view &bucket) = 0;

  virtual Status put_object_from_file(
      const std::string_view &bucket, const std::string_view &key,
      const std::string_view &data_file_path) = 0;
  virtual Status get_object_to_file(
      const std::string_view &bucket, const std::string_view &key,
      const std::string_view &output_file_path) = 0;

  virtual Status put_object(const std::string_view &bucket,
                            const std::string_view &key,
                            const std::string_view &data) = 0;
  virtual Status get_object(const std::string_view &bucket,
                            const std::string_view &key, std::string &body) = 0;
  virtual Status get_object(const std::string_view &bucket,
                            const std::string_view &key, size_t off, size_t len,
                            std::string &body) = 0;
  virtual Status get_object_meta(const std::string_view &bucket,
                                 const std::string_view &key,
                                 ObjectMeta &meta) = 0;

  virtual Status list_object(const std::string_view &bucket,
                             const std::string_view &prefix,
                             std::vector<ObjectMeta> &objects) = 0;

  virtual Status delete_object(const std::string_view &bucket,
                               const std::string_view &key) = 0;
};

// create ObjectStore based credentials in credentials dir or environment
// variables.
ObjectStore *create_object_store(const std::string_view &provider,
                                 const std::string_view region,
                                 const std::string_view *endpoint,
                                 bool use_https = true);

void destroy_object_store(ObjectStore *obj_store);

}  // namespace objstore

#endif  // OBJSTORE_OBJSTORE_H_INCLUDED
