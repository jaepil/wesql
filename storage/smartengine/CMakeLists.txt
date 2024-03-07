# TODO: Copyrights

#IF (NOT EXISTS "${CMAKE_SOURCE_DIR}/storage/smartengine/smartengine/Makefile")
#  MESSAGE(SEND_ERROR "Missing Makefile in smartengine directory. Try \"git submodule update\".")
#ENDIF()

# suppress warning of new FixedQueue<T>, since using C++17
IF (NOT WITH_SMARTENGINE)
  MESSAGE (STATUS "Not building with smartengine, you must specify -DWITH_SMARTENGINE=1|ON to cmake in order to build with smartengine.")
  RETURN ()
ELSE ()
  MESSAGE (STATUS "Building with smartengine storage engine = " ${WITH_SMARTENGINE})
ENDIF()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-compare -Wno-unused-parameter -Wno-unused-variable -Wno-vla -Wno-overloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers -Wno-missing-format-attribute -Wno-format-security -Wno-non-virtual-dtor -faligned-new=32 -Wno-cast-qual")
set(SE_SOURCE_DIR "${CMAKE_SOURCE_DIR}/storage/smartengine/core")
CHECK_FUNCTION_EXISTS(sched_getcpu  HAVE_SCHED_GETCPU)
IF(HAVE_SCHED_GETCPU)
  ADD_DEFINITIONS(-DHAVE_SCHED_GETCPU=1)
ENDIF()

# get a list of smartengine library source files
# run with env -i to avoid passing variables
EXECUTE_PROCESS(
  COMMAND env -i ${CMAKE_SOURCE_DIR}/storage/smartengine/get_smartengine_files.sh
  OUTPUT_VARIABLE SCRIPT_OUTPUT
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)
# split the list into lines
STRING(REGEX MATCHALL "[^\n]+" SE_LIB_SOURCES ${SCRIPT_OUTPUT})



INCLUDE_DIRECTORIES(
  ${CMAKE_SOURCE_DIR}/sql
  ${CMAKE_SOURCE_DIR}/sql-common
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/storage/innobase
  ${CMAKE_SOURCE_DIR}/storage/innobase/include
  ${CMAKE_SOURCE_DIR}/storage/smartengine
  ${CMAKE_SOURCE_DIR}/storage/smartengine/handler
  ${CMAKE_SOURCE_DIR}/storage/smartengine/util
  ${CMAKE_SOURCE_DIR}/storage/smartengine/api
  ${CMAKE_SOURCE_DIR}/storage/smartengine/dict
  ${CMAKE_SOURCE_DIR}/storage/smartengine/index
  ${CMAKE_SOURCE_DIR}/storage/smartengine/plugin
  ${CMAKE_SOURCE_DIR}/storage/smartengine/transaction
  ${CMAKE_SOURCE_DIR}/storage/smartengine/core
  ${CMAKE_SOURCE_DIR}/storage/smartengine/core/db
)

ADD_DEFINITIONS(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX -DHAVE_ZLIB)

# need xstream_extent
#SET(XSTREAM_SOURCES
#  xstream/ds_buffer.cc
#  xstream/ds_local.cc
#  xstream/ds_stdout.cc
#  xstream/datasink.cc
#  xstream/xstream_read.cc
#  xstream/xstream_write.cc
#  xstream/xcrypt_write.cc
#  )

SET(SE_SOURCES
  handler/ha_smartengine.cc 
  handler/ha_smartengine.h 
  handler/handler_alter.cc
  handler/handler_alter.h
  handler/handler_select.cc
  handler/se_hton.cc
  handler/se_hton.h
  api/se_api.cc
  api/se_api.h
  dict/se_dd_operations.cc 
  dict/se_dd_operations.h
  dict/se_cf_options.cc 
  dict/se_cf_options.h
  dict/se_cf_manager.cc 
  dict/se_cf_manager.h
  dict/se_binlog_manager.cc
  dict/se_binlog_manager.h
  dict/se_charset_info.cc
  dict/se_charset_info.h
  dict/se_ddl_manager.cc
  dict/se_ddl_manager.h
  dict/se_dict_manager.cc
  dict/se_dict_manager.h
  dict/se_dict_struct.cc
  dict/se_dict_struct.h
  dict/se_dict_util.cc
  dict/se_dict_util.h
  dict/se_field_make_unpack_info.cc
  dict/se_field_make_unpack_info.h
  dict/se_field_pack.cc
  dict/se_field_pack.h
  dict/se_field_pack_func.cc
  dict/se_field_pack_func.h
  dict/se_field_unpack_func.cc
  dict/se_field_unpack_func.h
  dict/se_field_skip_func.cc
  dict/se_field_skip_func.h
  dict/se_index.cc
  dict/se_index.h
  dict/se_table.cc
  dict/se_table.h
  dict/se_log_ddl.cc 
  dict/se_log_ddl.h
  dict/se_sst_info.cc 
  dict/se_sst_info.h
  dict/se_index_stats.cc 
  dict/se_index_stats.h
  index/se_index_merge.cc 
  index/se_index_merge.h
  index/se_parallel_ddl_ctx.cc
  index/se_parallel_ddl_ctx.h
  plugin/se_i_s.cc 
  plugin/se_i_s.h
  plugin/se_plugin.cc
  plugin/se_status_vars.cc
  plugin/se_status_vars.h
  plugin/se_system_vars.cc
  plugin/se_system_vars.h
  transaction/se_async_commit_callback.h
  transaction/se_transaction.cc
  transaction/se_transaction.h
  transaction/se_transaction_factory.cc
  transaction/se_transaction_factory.h
  transaction/se_transaction_impl.cc
  transaction/se_transaction_impl.h
  transaction/se_transaction_list_walker.cc
  transaction/se_transaction_list_walker.h
  transaction/se_write_batch_impl.cc
  transaction/se_write_batch_impl.h
  util/se_logger.cc
  util/se_logger.h
  util/se_mutex_wrapper.cc 
  util/se_mutex_wrapper.h
  util/se_utils.cc 
  util/se_utils.h 
  util/se_buff.cc
  util/se_buff.h
  util/se_threads.cc 
  util/se_threads.h
  util/se_counter.h
  util/se_comparator.h
  util/se_atomic_stat.h
  ${SE_LIB_SOURCES}
  #${XSTREAM_SOURCES}
)

IF(WITH_FB_TSAN)
  SET(PIC_EXT "_pic")
ELSE()
  SET(PIC_EXT "")
ENDIF()

#ADD_SUBDIRECTORY(${CMAKE_SOURCE_DIR}/storage/smartengine/core/IS ${CMAKE_BINARY_DIR}/IS.build)
SET(smartengine_static_libs )
IF (NOT "$ENV{WITH_SNAPPY}" STREQUAL "")
  SET(smartengine_static_libs ${smartengine_static_libs}
  $ENV{WITH_SNAPPY}/lib/libsnappy${PIC_EXT}.a)
  ADD_DEFINITIONS(-DHAVE_SNAPPY)
ENDIF()

IF (NOT "$ENV{WITH_LZ4}" STREQUAL "")
  SET(smartengine_static_libs ${smartengine_static_libs}
  $ENV{WITH_LZ4}/lib/liblz4${PIC_EXT}.a)
  ADD_DEFINITIONS(-DHAVE_LZ4)
ENDIF()

# link ZSTD by default
SET(WITH_ZSTD 1)
IF (WITH_ZSTD)
#  if (EXISTS ${CMAKE_SOURCE_DIR}/extra/zstd/lib/libzstd${PIC_EXT}.a)
#    MESSAGE("Use existing zstd library ${CMAKE_SOURCE_DIR}/extra/zstd/lib/libzstd${PIC_EXT}.a")
#  else()
#    MESSAGE("compile bundled zstd library")
#    EXECUTE_PROCESS(COMMAND tar zxvf zstd.tar.gz 
#                    WORKING_DIRECTORY  ${CMAKE_SOURCE_DIR}/extra/)
#    EXECUTE_PROCESS(COMMAND make -j
#                    WORKING_DIRECTORY  ${CMAKE_SOURCE_DIR}/extra/zstd/lib)
#  endif()
  SET(smartengine_static_libs ${smartengine_static_libs} ${ZSTD_LIBRARY})
  ADD_DEFINITIONS(-DHAVE_ZSTD)
ENDIF()

#SET(IS_LIB ${CMAKE_BINARY_DIR}/IS.build/libis_all.a)
#SET(smartengine_static_libs ${smartengine_static_libs} ${ZLIB_LIBRARY} "-lrt" "-laio" ${IS_LIB})
SET(smartengine_static_libs ${smartengine_static_libs} ${ZLIB_LIBRARY} "-lrt" "-laio")

if (WITH_TBB)
  include_directories(${CMAKE_SOURCE_DIR}/storage/smartengine/core/third-party/tbb/include)
  SET(smartengine_static_libs ${smartengine_static_libs} ${TBB_LIBRARIES})
endif()

MYSQL_ADD_PLUGIN(smartengine ${SE_SOURCES} STORAGE_ENGINE DEFAULT STATIC_ONLY
  LINK_LIBRARIES ${smartengine_static_libs}
  #  DEPENDENCIES libis_all tbb
)

IF(WITH_EMBEDDED_SERVER)
  ADD_SUBDIRECTORY(unittest)
ENDIF()

# TODO: read this file list from src.mk:TOOL_SOURCES
SET(SE_TOOL_SOURCES
  ${CMAKE_SOURCE_DIR}/storage/smartengine/core/tools/ldb_tool.cc
  ${CMAKE_SOURCE_DIR}/storage/smartengine/core/tools/ldb_cmd.cc
  ${CMAKE_SOURCE_DIR}/storage/smartengine/core/tools/sst_dump_tool.cc
  ${CMAKE_SOURCE_DIR}/storage/smartengine/core/tools/backup_check_tool.cc 
)
MYSQL_ADD_EXECUTABLE(sst_dump ${CMAKE_SOURCE_DIR}/storage/smartengine/core/tools/sst_dump.cc ${SE_TOOL_SOURCES})
TARGET_LINK_LIBRARIES(sst_dump smartengine)

MYSQL_ADD_EXECUTABLE(ldb ${CMAKE_SOURCE_DIR}/storage/smartengine/core/tools/ldb.cc ${SE_TOOL_SOURCES})
TARGET_LINK_LIBRARIES(ldb smartengine)

MYSQL_ADD_EXECUTABLE(mysql_ldb ${CMAKE_SOURCE_DIR}/storage/smartengine/tools/mysql_ldb.cc ${SE_TOOL_SOURCES})
TARGET_LINK_LIBRARIES(mysql_ldb smartengine)

MYSQL_ADD_EXECUTABLE(backup_check ${CMAKE_SOURCE_DIR}/storage/smartengine/core/tools/backup_check.cc ${SE_TOOL_SOURCES})
TARGET_LINK_LIBRARIES(backup_check smartengine)
