/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cstdio>
#include <cstdlib>
#include <chrono>

#include "objstore.h"
#include "storage/storage_common.h"
#include "util/filename.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace smartengine {
using namespace common;
using namespace util;
namespace obj {

const std::string ALIYUN_PROVIDER = "aliyun";
const std::string S3_PROVIDER = "aws";
const std::string ALIYUN_ENDPOINT = "oss-cn-hangzhou.aliyuncs.com";

class ObjstoreTest : public testing::TestWithParam<std::string>
{
public:
  void SetUp() override
  {
    env_ = Env::Default();
    env_options_ = EnvOptions();

    objstore::ObjectStore *obs = nullptr;

    provider_ = GetParam();

    if (provider_ == ALIYUN_PROVIDER) {
      // aliyun
      bucket_ = bucket_aliyun_;
      endpoint_ = new std::string_view(ALIYUN_ENDPOINT);
    } else if (provider_ == S3_PROVIDER) {
      // s3
      bucket_ = bucket_s3_;
    }

    auto s = env_->InitObjectStore(provider_, region_, endpoint_, false, bucket_, "");
    ASSERT_OK(s);
    s = env_->GetObjectStore(obs);
    ASSERT_OK(s);
    obj_store_ = obs;

    objstore::Status ss;
    ss = obs->delete_bucket(bucket_);
    ASSERT_TRUE(ss.is_succ() || ss.error_code() == objstore::SE_NO_SUCH_BUCKET);
    if (provider_ == ALIYUN_PROVIDER) {
      // In Alibaba Cloud OSS, once a bucket is deleted, 
      // it cannot be immediately recreated with the same name for a period of time. 
      // Therefore, each time a bucket is created, it should have a unique name.
      bucket_ = bucket_aliyun_ + std::to_string(getCurrentTimeMillis());
    }
    ss = obs->create_bucket(bucket_);
    if (!ss.is_succ()) {
      std::cout << "set up:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    ASSERT_TRUE(ss.is_succ());
  }

  objstore::ObjectStore *create_objstore_client()
  {
    objstore::init_objstore_provider(provider_);
    return objstore::create_object_store(provider_, region_, endpoint_, false);
  }

  void release_objstore_client(objstore::ObjectStore *client)
  {
    objstore::destroy_object_store(client);
    objstore::cleanup_objstore_provider(obj_store_);
  }

  void TearDown() override
  {
    objstore::Status ss;
    ss = obj_store_->delete_bucket(bucket_);
    if (!ss.is_succ()) {
      std::cout << "tear down:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    ASSERT_TRUE(ss.is_succ());

    env_->DestroyObjectStore();
    delete endpoint_;
  }
  objstore::Status create_bucket()
  {
    if (provider_ == ALIYUN_PROVIDER) {
      bucket_ = bucket_aliyun_ + std::to_string(getCurrentTimeMillis());
    }
    objstore::Status ss = obj_store_->create_bucket(bucket_);
    if (!ss.is_succ()) {
      std::cout << "create bucket:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }
  objstore::Status create_same_bucket_for_aliyun()
  {
    objstore::Status ss = obj_store_->create_bucket(bucket_);
    if (!ss.is_succ()) {
      std::cout << "create bucket:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }
  objstore::Status delete_bucket()
  {
    objstore::Status ss = obj_store_->delete_bucket(bucket_);
    if (!ss.is_succ()) {
      std::cout << "delete bucket:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }
  objstore::Status put_object(const std::string &key, const std::string &data)
  {
    objstore::Status ss = obj_store_->put_object(bucket_, key, data);
    if (!ss.is_succ()) {
      std::cout << "put object:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }
  objstore::Status get_object(const std::string &key, std::string &data)
  {
    objstore::Status ss = obj_store_->get_object(bucket_, key, data);
    if (!ss.is_succ()) {
      std::cout << "get object:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }
  objstore::Status get_object_range(const std::string &key, std::string &data, size_t off, size_t len)
  {
    objstore::Status ss = obj_store_->get_object(bucket_, key, off, len, data);
    if (!ss.is_succ()) {
      std::cout << "get object range:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }
  objstore::Status delete_object(const std::string &key)
  {
    objstore::Status ss = obj_store_->delete_object(bucket_, key);
    if (!ss.is_succ()) {
      std::cout << "delete object:" << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }
  objstore::Status list_object(const std::string &prefix,
                               std::string_view &start_after,
                               bool &finished,
                               std::vector<objstore::ObjectMeta> &objects)
  {
    objstore::Status ss = obj_store_->list_object(bucket_, prefix, start_after, finished, objects);
    if (!ss.is_succ()) {
      std::cout << "list object: " << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }
  objstore::Status put_object_from_file(const std::string &key, const std::string &data_file_path)
  {
    objstore::Status ss = obj_store_->put_object_from_file(bucket_, key, data_file_path);
    if (!ss.is_succ()) {
      std::cout << "put object from file: " << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }
  objstore::Status get_object_to_file(const std::string &key, const std::string &output_file_path)
  {
    objstore::Status ss = obj_store_->get_object_to_file(bucket_, key, output_file_path);
    if (!ss.is_succ()) {
      std::cout << "get object to file: " << ss.error_code() << " " << ss.error_message() << std::endl;
    }
    return ss;
  }

  static int64_t getCurrentTimeMillis() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
  }

protected:
  Env *env_;
  EnvOptions env_options_;
  objstore::ObjectStore *obj_store_;

  std::string provider_;
  std::string bucket_;
  std::string bucket_s3_ = "wesql-s3-ut-test";
  std::string bucket_aliyun_ = "wesql-aliyun-ut-test";
  std::string region_ = "cn-northwest-1";
  std::string_view *endpoint_ = nullptr;
};

INSTANTIATE_TEST_CASE_P(cloudProviders,
                        ObjstoreTest,
                        testing::Values(
                                        "aliyun",
                                        "aws"
                                        ));

TEST_P(ObjstoreTest, reinitObjStoreApi)
{
  objstore::ObjectStore *obs = nullptr;

  objstore::init_objstore_provider(provider_);
  objstore::init_objstore_provider(provider_);
  objstore::init_objstore_provider(provider_);
  objstore::cleanup_objstore_provider(obj_store_);
  objstore::cleanup_objstore_provider(obj_store_);
  objstore::cleanup_objstore_provider(obj_store_);

  env_->GetObjectStore(obs);
  ASSERT_TRUE(obs != nullptr);

  objstore::cleanup_objstore_provider(obj_store_);
  objstore::init_objstore_provider(provider_);

  env_->DestroyObjectStore();

  env_->GetObjectStore(obs);
  ASSERT_TRUE(obs == nullptr);

  auto s = env_->InitObjectStore(provider_, region_, endpoint_, false, bucket_, "");
  ASSERT_OK(s);
  s = env_->GetObjectStore(obs);
  ASSERT_OK(s);
  obj_store_ = obs;
}

TEST_P(ObjstoreTest, noBucket)
{
  objstore::Status ss = delete_bucket();
  ASSERT_TRUE(ss.is_succ());
  std::string data;

  ss = get_object("x", data);
  ASSERT_EQ(ss.error_code(), objstore::SE_NO_SUCH_BUCKET);

  ss = put_object("x", "x");
  ASSERT_EQ(ss.error_code(), objstore::SE_NO_SUCH_BUCKET);

  ss = delete_object("x");
  ASSERT_EQ(ss.error_code(), objstore::SE_NO_SUCH_BUCKET);

  bool finished = false;
  std::string_view start_after = "";
  std::vector<objstore::ObjectMeta> objects;
  ss = list_object("x", start_after, finished, objects);
  ASSERT_EQ(ss.error_code(), objstore::SE_NO_SUCH_BUCKET);

  ss = create_bucket();
  ASSERT_TRUE(ss.is_succ());
}

TEST_P(ObjstoreTest, createBucket)
{
  objstore::Status ss = delete_bucket();
  ASSERT_TRUE(ss.is_succ());
  std::string data;

  ss = create_bucket();
  ASSERT_TRUE(ss.is_succ());

  if (provider_ == S3_PROVIDER) {
    ss = create_bucket();
    ASSERT_EQ(ss.error_code(), objstore::SE_BUCKET_ALREADY_OWNED_BY_YOU);
  } else if (provider_ == ALIYUN_PROVIDER) {
    ss = create_same_bucket_for_aliyun();
    ASSERT_EQ(ss.error_code(), objstore::SE_BUCKET_ALREADY_EXISTS);
  }

  objstore::ObjectStore *new_client = create_objstore_client();
  ASSERT_TRUE(new_client != nullptr);

  ss = new_client->create_bucket(bucket_);
  if (provider_ == S3_PROVIDER) {
    ASSERT_EQ(ss.error_code(), objstore::SE_BUCKET_ALREADY_OWNED_BY_YOU);
  } else if (provider_ == ALIYUN_PROVIDER) {
    ASSERT_EQ(ss.error_code(), objstore::SE_BUCKET_ALREADY_EXISTS);
  }

  release_objstore_client(new_client);
}

TEST_P(ObjstoreTest, operateObject)
{
  std::string key = "test_put_object";
  std::string raw_data = "test_put_object_data";
  std::string data;
  objstore::Status ss = put_object(key, raw_data);
  ASSERT_TRUE(ss.is_succ());

  ss = get_object(key, data);
  ASSERT_TRUE(ss.is_succ());
  ASSERT_EQ(data, data);

  ss = get_object_range(key, data, 1, 3);
  ASSERT_TRUE(ss.is_succ());
  ASSERT_EQ(data, "est");

  ss = get_object_range(key, data, 1, raw_data.size() - 1);
  ASSERT_TRUE(ss.is_succ());
  ASSERT_EQ(data, "est_put_object_data");

  ss = get_object_range(key, data, 1, 300);
  ASSERT_TRUE(ss.is_succ());
  ASSERT_EQ(data, "est_put_object_data");

  ss = get_object_range(key, data, 30, 10);
  ASSERT_TRUE(!ss.is_succ());

  ss = delete_object(key);
  ASSERT_TRUE(ss.is_succ());

  ss = get_object(key, data);
  ASSERT_EQ(ss.error_code(), objstore::SE_NO_SUCH_KEY);
}

TEST_P(ObjstoreTest, listObject)
{
  objstore::Status ss;
  std::vector<objstore::ObjectMeta> objects;
  bool finished = false;
  std::string_view start_after = "";
  // put 1001 objects with the same prefix
  for (int i = 0; i < 1001; i++) {
    std::string key = "test_list_object_" + std::to_string(i);
    std::string data = "test_list_object_data_" + std::to_string(i);
    ss = put_object(key, data);
    ASSERT_TRUE(ss.is_succ());
  }

  ss = list_object("test_list_object_", start_after, finished, objects);
  // s3 list object api returns 1000 objects at most
  ASSERT_EQ(objects.size(), 1000);
  // list object is ordered in lexicographical order, so the last(1001-th) is "test_list_object_999", but not
  // "test_list_object_1000", and the 1000-th object is "test_list_object_998"
  ASSERT_EQ(objects[999].key, "test_list_object_998");
  ASSERT_EQ(start_after, "test_list_object_998");
  ASSERT_FALSE(finished);

  // check the results are ordered by key in lexicographical order
  for (int i = 0; i < 999; i++) {
    ASSERT_TRUE(objects[i].key < objects[i + 1].key);
  }
  objects.clear();

  ss = list_object("test_list_object_", start_after, finished, objects);
  // the `start_after` key("test_list_object_998") is not included.
  ASSERT_EQ(objects.size(), 1);
  ASSERT_EQ(objects[0].key, "test_list_object_999");
  ASSERT_TRUE(finished);
  ASSERT_EQ(start_after, "");
  objects.clear();

  for (int i = 0; i < 1001; i++) {
    std::string key = "test_list_object_" + std::to_string(i);
    ss = delete_object(key);
    ASSERT_TRUE(ss.is_succ());
  }
}

TEST_P(ObjstoreTest, operateObjectFromFile)
{
  std::string key = "test_put_object_from_file";
  std::string data = "test_put_object_from_file_data";
  std::string data_file_path = "/tmp/test_put_object_from_file_data";
  FILE *fp = fopen(data_file_path.c_str(), "w");
  ASSERT_TRUE(fp != nullptr);
  fwrite(data.c_str(), 1, data.size(), fp);
  fclose(fp);

  objstore::Status ss = put_object_from_file(key, data_file_path);
  ASSERT_TRUE(ss.is_succ());

  std::string output_file_path = "/tmp/test_get_object_to_file_data";
  ss = get_object_to_file(key, output_file_path);
  ASSERT_TRUE(ss.is_succ());

  std::string output_data;
  fp = fopen(output_file_path.c_str(), "r");
  ASSERT_TRUE(fp != nullptr);
  char buf[1024];
  size_t read_size = fread(buf, 1, 1024, fp);
  output_data.append(buf, read_size);
  fclose(fp);
  ASSERT_EQ(data, output_data);

  ss = delete_object(key);
  ASSERT_TRUE(ss.is_succ());

  // remove the temp file
  remove(data_file_path.c_str());
  remove(output_file_path.c_str());
}

TEST_P(ObjstoreTest, noSuckKey) {
  std::string key = "test_put_object_key";
  std::string data = "test-data";
  objstore::Status ss = put_object(key, data);
  ASSERT_TRUE(ss.is_succ());

  ss = delete_object(key);
  ASSERT_TRUE(ss.is_succ());
  ss = get_object(key, data);
  ASSERT_TRUE(!ss.is_succ());
  ASSERT_EQ(ss.error_code(), objstore::SE_NO_SUCH_KEY);
}

} // namespace obj

} // namespace smartengine

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
