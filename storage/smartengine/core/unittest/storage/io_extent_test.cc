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

#include "util/testharness.h"
#include "util/testutil.h"
#include "storage/io_extent.h"

static const std::string test_dir = smartengine::util::test::TmpDir() + "/io_extent_test";
namespace smartengine
{
using namespace common;
using namespace util;
namespace storage
{
class IOExtentTest : public testing::Test
{
public:
  IOExtentTest() : io_info_(), data_() {}
  ~IOExtentTest() {}

  virtual void SetUp()
  {
    int ret = 0;
    // Simulate read and write operations on extent(0, 1).
    io_info_.extent_id_ = ExtentId(0, 1);
    io_info_.extent_size_ = MAX_EXTENT_SIZE;
    io_info_.block_size_ = 16 * 1024;
    io_info_.unique_id_ = 1;

    // open file
    std::string file_name = test_dir + TEST_FILE_NAME;

    if (0 > (io_info_.fd_ = ::open(file_name.c_str(), O_RDWR | O_DIRECT | O_CREAT))) {
      fprintf(stderr, "Failed to open file: errno = %d (%s)\n", errno, strerror(errno));
    }
    assert(io_info_.fd_ > 0);

    // fallocate space for extents (0, 0),(0,1)
    ret = ::fallocate(io_info_.fd_, FALLOC_FL_KEEP_SIZE, 0, 2 * MAX_EXTENT_SIZE);
    assert(-1 != ret);

    // generate data
    data_ = generate_random_string(MAX_EXTENT_SIZE);

    // allocate aligned buf
    aligned_buf_ = reinterpret_cast<char *>(memory::base_memalign(DIOHelper::DIO_ALIGN_SIZE, ALIGNED_BUF_SIZE));
  }

    virtual void TearDown()
    {
      // remove file
      std::string file_name = test_dir + TEST_FILE_NAME;
      ::close(io_info_.fd_);
      ::unlink(file_name.c_str());

      io_info_.reset();
      data_.clear();
      memory::base_memalign_free(aligned_buf_);
    }

protected:
  std::string generate_random_string(int64_t length)
  {
    // use current time as seed
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937_64 generator(seed);
    std::uniform_int_distribution<int> distribution(32, 126); // ASCII visible range

    // generate
    std::string result;
    result.reserve(length);
    for (int64_t i = 0; i < length; ++i) {
        result.push_back(static_cast<char>(distribution(generator)));
    }

    return result;
  }

  void prepare_data()
  {
    int ret = Status::kOk;
    WritableExtent writable_extent;
    ret = writable_extent.init(io_info_);
    ASSERT_EQ(Status::kOk, ret);
    memcpy(aligned_buf_, data_.data(), MAX_EXTENT_SIZE);
    ret = writable_extent.append(Slice(aligned_buf_, MAX_EXTENT_SIZE));
    ASSERT_EQ(Status::kOk, ret);   
  }

  void check_read(ReadableExtent * readable_extent,
                  const int64_t offset,
                  const int64_t size,
                  const int64_t aligned_buf_offset,
                  util::AIOHandle *aio_handle)
  {
    int ret = Status::kOk;
    Slice resut;
    memset(aligned_buf_, 0, 2 * MAX_EXTENT_SIZE);

    ret = readable_extent->read(offset,
                                size,
                                aligned_buf_ + aligned_buf_offset,
                                aio_handle,
                                resut);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(size, resut.size());
    ret = memcmp(data_.data() + offset, resut.data(), size);
    ASSERT_EQ(0, ret);
  }

protected:
  static const int64_t ALIGNED_BUF_SIZE = 2 * MAX_EXTENT_SIZE;
  static const char *TEST_FILE_NAME;

protected:
  ExtentIOInfo io_info_;
  std::string data_;
  char *aligned_buf_;
};
const char *IOExtentTest::TEST_FILE_NAME = "/io_extent.sst";

TEST_F(IOExtentTest, init)
{
  int ret = Status::kOk;
  ExtentIOInfo invalid_io_info;

  WritableExtent writable_extent;
  ret = writable_extent.init(invalid_io_info);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = writable_extent.init(io_info_);
  ASSERT_EQ(Status::kOk, ret);
  
  ReadableExtent readable_extent;
  ret = readable_extent.init(invalid_io_info);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = readable_extent.init(io_info_);
  ASSERT_EQ(Status::kOk, ret);

  FullPrefetchExtent prefetch_extent;
  ret = prefetch_extent.init(invalid_io_info);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = prefetch_extent.init(io_info_);
  ASSERT_EQ(Status::kOk, ret);
}

TEST_F(IOExtentTest, append)
{
  int ret = Status::kOk;
  WritableExtent writable_extent;

  // Not init
  ret = writable_extent.append(Slice(data_.data(), data_.size()));
  ASSERT_EQ(Status::kNotInit, ret);

  // Init
  ret = writable_extent.init(io_info_);
  ASSERT_EQ(Status::kOk, ret);

  // Invalid data
  ret = writable_extent.append(Slice(nullptr, MAX_EXTENT_SIZE));
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // Invalid data size
  ret = writable_extent.append(Slice(data_.data(), 0));
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // OverLimit
  ret = writable_extent.append(Slice(data_.data(), 2 * MAX_EXTENT_SIZE));
  ASSERT_EQ(Status::kOverLimit, ret);

  // Not aligned data
  char *not_aligned_addr = (aligned_buf_ + DIOHelper::DIO_ALIGN_SIZE / 2);
  memcpy(not_aligned_addr, data_.data(), MAX_EXTENT_SIZE);
  ret = writable_extent.append(Slice(not_aligned_addr, MAX_EXTENT_SIZE));
  ASSERT_EQ(Status::kErrorUnexpected, ret);

  // successful single append
  memcpy(aligned_buf_, data_.data(), MAX_EXTENT_SIZE);
  ret = writable_extent.append(Slice(aligned_buf_, MAX_EXTENT_SIZE));
  ASSERT_EQ(Status::kOk, ret);
  // Check data
  ReadableExtent readable_extent;
  ret = readable_extent.init(io_info_);
  ASSERT_EQ(Status::kOk, ret);
  check_read(&readable_extent, 0, MAX_EXTENT_SIZE, 0, nullptr);

  // successful multiple append
  writable_extent.reset();
  ret = writable_extent.init(io_info_);
  ASSERT_EQ(Status::kOk, ret);
  memcpy(aligned_buf_, data_.data(), MAX_EXTENT_SIZE);
  for (int32_t i = 0; i < MAX_EXTENT_SIZE / DIOHelper::DIO_ALIGN_SIZE; ++i) {
    ret = writable_extent.append(Slice(aligned_buf_ + i * DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE));
    ASSERT_EQ(Status::kOk, ret);
  }
  check_read(&readable_extent, 0, MAX_EXTENT_SIZE, 0, nullptr);
  ret = writable_extent.append(Slice(aligned_buf_, DIOHelper::DIO_ALIGN_SIZE));
  ASSERT_EQ(Status::kOverLimit, ret);
}

TEST_F(IOExtentTest, sync_read)
{
  int ret = Status::kOk;
  ReadableExtent readable_extent;
  Slice result;

  // Prepare data
  prepare_data();

  // Not init
  ret = readable_extent.read(0, MAX_EXTENT_SIZE, aligned_buf_, nullptr /*aio_handle*/,  result);
  ASSERT_EQ(Status::kNotInit, ret);

  // Init
  ret = readable_extent.init(io_info_);
  ASSERT_EQ(Status::kOk, ret);

  // Invalid offset
  ret = readable_extent.read(-1, MAX_EXTENT_SIZE, aligned_buf_, nullptr /*aio_handle*/,  result);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // Invalid size
  ret = readable_extent.read(0, 0, aligned_buf_, nullptr /*aio_handle*/,  result);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // Invalid buffer
  ret = readable_extent.read(0, MAX_EXTENT_SIZE, nullptr /*buf=*/, nullptr /*aio_handle=*/, result);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  // OverLimit extent size
  ret = readable_extent.read(0, 2 * MAX_EXTENT_SIZE, aligned_buf_, nullptr /*aio_handle*/,  result);
  ASSERT_EQ(Status::kOverLimit, ret);

  // Aligned offset, size, buf
  check_read(&readable_extent, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE, 0, nullptr);

  check_read(&readable_extent, DIOHelper::DIO_ALIGN_SIZE * 2, DIOHelper::DIO_ALIGN_SIZE * 10, 0, nullptr);

  check_read(&readable_extent, 0, MAX_EXTENT_SIZE, 0, nullptr);

  // Not aligned offset, and aligned size, buf
  check_read(&readable_extent, DIOHelper::DIO_ALIGN_SIZE / 2, DIOHelper::DIO_ALIGN_SIZE, 0, nullptr);

  check_read(&readable_extent,
            DIOHelper::DIO_ALIGN_SIZE * 10 + DIOHelper::DIO_ALIGN_SIZE / 10,
            DIOHelper::DIO_ALIGN_SIZE * 10,
            0,
            nullptr);

  // Not aligned size, and aligned offset, buf
  check_read(&readable_extent, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE / 2, 0, nullptr);

  check_read(&readable_extent,
             DIOHelper::DIO_ALIGN_SIZE * 10,
             DIOHelper::DIO_ALIGN_SIZE * 10 + DIOHelper::DIO_ALIGN_SIZE / 10,
             0,
             nullptr);
  
  // Not aligned buf, and aligned offset, size
  check_read(&readable_extent, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE, DIOHelper::DIO_ALIGN_SIZE / 2, nullptr);

  check_read(&readable_extent,
             DIOHelper::DIO_ALIGN_SIZE * 10,
             DIOHelper::DIO_ALIGN_SIZE * 10,
             DIOHelper::DIO_ALIGN_SIZE * 5 + DIOHelper::DIO_ALIGN_SIZE / 5,
             nullptr);

  // Random offset, size, buf
  test::RandomInt64Generator generator(0, 1024 * 1024);
  for (int32_t i = 0; i < 10000; ++i) {
    check_read(&readable_extent, generator.generate(), generator.generate(), generator.generate(), nullptr);
  }

}

TEST_F(IOExtentTest, async_read)
{
  int ret = Status::kOk;
  ReadableExtent readable_extent;

  // Prepare data
  prepare_data();

  // Init
  ret = readable_extent.init(io_info_);
  ASSERT_EQ(Status::kOk, ret);

  // Random async read
  test::RandomInt64Generator generator(0, 1024 * 1024);
  for (int32_t i = 0; i < 10000; ++i) {
    util::AIOHandle aio_handle;
    aio_handle.aio_req_.reset(new util::AIOReq());
    int64_t offset = generator.generate();
    int64_t size = generator.generate();
    int64_t aligned_buf_offset = generator.generate(); 
    ret = readable_extent.prefetch(offset, size, &aio_handle);
    ASSERT_EQ(Status::kOk, ret);
    check_read(&readable_extent, offset, size, aligned_buf_offset, &aio_handle);
  }

  // Read range is not consistent with prefetch range
  for (int32_t i = 0; i < 10000; ++i) {
    util::AIOHandle aio_handle;
    aio_handle.aio_req_.reset(new util::AIOReq());
    ret = readable_extent.prefetch(generator.generate(), generator.generate(), &aio_handle);
    ASSERT_EQ(Status::kOk, ret);
    check_read(&readable_extent, generator.generate(), generator.generate(), generator.generate(), &aio_handle);
  }

  // Prefetch overlimit range
  {
    util::AIOHandle aio_handle;
    aio_handle.aio_req_.reset(new util::AIOReq());
    ret = readable_extent.prefetch(MAX_EXTENT_SIZE / 2, MAX_EXTENT_SIZE, &aio_handle);
    ASSERT_EQ(Status::kOk, ret);
  }

}

TEST_F(IOExtentTest, FullPrefetchExtent)
{
  int ret = Status::kOk;
  FullPrefetchExtent pfefetch_extent;
  Slice result;

  // Prepare data
  prepare_data();

  // Init
  ret = pfefetch_extent.init(io_info_);
  ASSERT_EQ(Status::kOk, ret);

  // Prefetch extent
  ret = pfefetch_extent.full_prefetch();

  // Random read
  test::RandomInt64Generator generator(0, 1024 * 1024);
  for (int32_t i = 0; i < 10000; ++i) {
    check_read(&pfefetch_extent, generator.generate(), generator.generate(), generator.generate(), nullptr);
  }
}

} // namespace storage
} // namespace smartengine

int main(int argc, char **argv)
{
  smartengine::util::test::remove_dir(test_dir.c_str());
  smartengine::util::Env::Default()->CreateDir(test_dir);
  std::string log_path = smartengine::util::test::TmpDir() + "/io_extent_test.log";
  smartengine::logger::Logger::get_log().init(log_path.c_str(), smartengine::logger::DEBUG_LEVEL);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
