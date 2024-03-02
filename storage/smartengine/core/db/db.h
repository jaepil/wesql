/*
 * Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_ROCKSDB_INCLUDE_DB_H_
#define STORAGE_ROCKSDB_INCLUDE_DB_H_

#include <stdint.h>
#include <stdio.h>
#include <map>
#include <memory>
#include <string>
#include <list>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <mutex>
#include "db/async_callback.h"
#include "options/options.h"
#include "table/iterator.h"
#include "transactions/transaction_log.h"

#ifdef _WIN32
// Windows API macro interference
#undef DeleteFile
#endif

namespace smartengine {
namespace common {
struct Options;
struct DBOptions;
struct ColumnFamilyOptions;
struct ReadOptions;
struct WriteOptions;
struct FlushOptions;
}

namespace table {
struct TableProperties;
struct ExternalSstFileInfo;
}

namespace util {
class Env;
}

namespace storage {
class ExtentSpaceManager;
struct CompactionJobStatsInfo;
class StorageLogger;
struct DataFileStatistics;
}

namespace db {
class WriteBatch;
struct MiniTables;
struct CreateSubTableArgs;
class ColumnFamilyData;
struct SuperVersion;

using std::unique_ptr;
#ifdef WITH_STRESS_CHECK
extern thread_local std::unordered_map<std::string, std::string>
    *STRESS_CHECK_RECORDS;
#define STRESS_CHECK_SAVE(name, value)                          \
    (*smartengine::db::STRESS_CHECK_RECORDS)[std::string(#name)] =  \
        std::to_string(value);

#define STRESS_CHECK_APPEND(name, value)                              \
    (*smartengine::db::STRESS_CHECK_RECORDS)[std::string(#name)].append(  \
        std::to_string(value) + " ");

#define STRESS_CHECK_PRINT()                                            \
    for (auto it : *(smartengine::db::STRESS_CHECK_RECORDS)) {              \
      fprintf(stderr, "%s, %s\n", it.first.data(), it.second.data()); \
    }

#define STRESS_CHECK_CLEAR()                                    \
    smartengine::db::STRESS_CHECK_RECORDS->clear();
#else
#define STRESS_CHECK_SAVE(name, value)
#define STRESS_CHECK_APPEND(name, value)
#define STRESS_CHECK_PRINT()
#define STRESS_CHECK_CLEAR()
#endif

extern const std::string kDefaultColumnFamilyName;
struct ColumnFamilyDescriptor {
  std::string name;
  common::ColumnFamilyOptions options;
  ColumnFamilyDescriptor()
      : name(kDefaultColumnFamilyName),
        options(common::ColumnFamilyOptions()) {}
  ColumnFamilyDescriptor(const std::string& _name,
                         const common::ColumnFamilyOptions& _options)
      : name(_name), options(_options) {}
};

struct CreateSubTableArgs
{
  int64_t index_id_;
  common::ColumnFamilyOptions cf_options_;
  bool create_table_space_;
  int64_t table_space_id_;

  CreateSubTableArgs() : index_id_(-1), cf_options_() {}
  CreateSubTableArgs(int64_t index_id, common::ColumnFamilyOptions cf_options)
      : index_id_(index_id),
        cf_options_(cf_options),
        create_table_space_(false),
        table_space_id_(0)
  {
  }
  CreateSubTableArgs(int64_t index_id, common::ColumnFamilyOptions cf_options, bool create_table_space, int64_t table_space_id)
      : index_id_(index_id),
        cf_options_(cf_options),
        create_table_space_(create_table_space),
        table_space_id_(table_space_id)
  {
  }

  ~CreateSubTableArgs() {}
  bool is_valid() const
  {
    return index_id_ >= 0;
  }
  DECLARE_AND_DEFINE_TO_STRING(KV_(index_id), KV_(create_table_space), KV_(table_space_id))
};

class ColumnFamilyHandle {
 public:
  virtual ~ColumnFamilyHandle() {}
  // Returns the name of the column family associated with the current handle.
  virtual const std::string& GetName() const = 0;
  // Returns the ID of the column family associated with the current handle.
  virtual uint32_t GetID() const = 0;
  // Fills "*desc" with the up-to-date descriptor of the column family
  // associated with this handle. Since it fills "*desc" with the up-to-date
  // information, this call might internally lock and release DB mutex to
  // access the up-to-date CF options.  In addition, all the pointer-typed
  // options cannot be referenced any longer than the original options exist.
  //
  // Note that this function is not supported in RocksDBLite.
  virtual common::Status GetDescriptor(ColumnFamilyDescriptor* desc) = 0;
  // Returns the comparator of the column family associated with the
  // current handle.
  virtual const util::Comparator* GetComparator() const = 0;
};
// A range of keys
struct Range {
  common::Slice start;  // Included in the range
  common::Slice limit;  // Not included in the range

  Range() {}
  Range(const common::Slice& s, const common::Slice& l) : start(s), limit(l) {}
};

// A collections of table properties objects, where
//  key: is the table's file name.
//  value: the table properties object of the given table.
typedef std::unordered_map<std::string,
                           std::shared_ptr<const table::TableProperties>>
    TablePropertiesCollection;

// for hotbackup
typedef std::unordered_map<db::ColumnFamilyData *, const db::Snapshot *> MetaSnapshotMap;

// A DB is a persistent ordered map from keys to values.
// A DB is safe for concurrent access from multiple threads without
// any external synchronization.
class DB {
 public:
  // Open the database with the specified "name".
  // Stores a pointer to a heap-allocated database in *dbptr and returns
  // OK on success.
  // Stores nullptr in *dbptr and returns a non-OK status on error.
  // Caller should delete *dbptr when it is no longer needed.
  static common::Status Open(const common::Options& options,
                             const std::string& name, DB** dbptr);

  // Open DB with column families.
  // db_options specify database specific options
  // column_families is the vector of all column families in the database,
  // containing column family name and options. You need to open ALL column
  // families in the database. To get the list of column families, you can use
  // ListColumnFamilies(). Also, you can open only a subset of column families
  // for read-only access.
  // The default column family name is 'default' and it's stored
  // in smartengine::db::kDefaultColumnFamilyName.
  // If everything is OK, handles will on return be the same size
  // as column_families --- handles[i] will be a handle that you
  // will use to operate on column family column_family[i].
  // Before delete DB, you have to close All column families by calling
  // DestroyColumnFamilyHandle() with all the handles.
  static common::Status Open(
      const common::DBOptions& db_options, const std::string& name,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles, DB** dbptr);

  // ListColumnFamilies will open the DB specified by argument name
  // and return the list of all column families in that DB
  // through column_families argument. The ordering of
  // column families in column_families is unspecified.
  static common::Status ListColumnFamilies(
      const common::DBOptions& db_options, const std::string& name,
      std::vector<std::string>* column_families);

  DB() {}
  virtual ~DB();

  // Create a column_family and return the handle of column family
  // through the argument handle.
  virtual common::Status CreateColumnFamily(CreateSubTableArgs &args, ColumnFamilyHandle** handle);

  // Drop a column family specified by column_family handle. This call
  // only records a drop record in the manifest and prevents the column
  // family from flushing and compacting.
  virtual common::Status DropColumnFamily(ColumnFamilyHandle* column_family);
  // Close a column family specified by column_family handle and destroy
  // the column family handle specified to avoid double deletion. This call
  // deletes the column family handle by default. Use this method to
  // close column family instead of deleting column family handle directly
  virtual common::Status DestroyColumnFamilyHandle(
      ColumnFamilyHandle* column_family);

  // Set the database entry for "key" to "value".
  // If "key" already exists, it will be overwritten.
  // Returns OK on success, and a non-OK status on error.
  // Note: consider setting options.sync = true.
  virtual common::Status Put(const common::WriteOptions& options,
                             ColumnFamilyHandle* column_family,
                             const common::Slice& key,
                             const common::Slice& value) = 0;
  virtual common::Status Put(const common::WriteOptions& options,
                             const common::Slice& key,
                             const common::Slice& value) {
    return Put(options, DefaultColumnFamily(), key, value);
  }

  // Remove the database entry (if any) for "key".  Returns OK on
  // success, and a non-OK status on error.  It is not an error if "key"
  // did not exist in the database.
  // Note: consider setting options.sync = true.
  virtual common::Status Delete(const common::WriteOptions& options,
                                ColumnFamilyHandle* column_family,
                                const common::Slice& key) = 0;
  virtual common::Status Delete(const common::WriteOptions& options,
                                const common::Slice& key) {
    return Delete(options, DefaultColumnFamily(), key);
  }

  // Remove the database entry for "key". Requires that the key exists
  // and was not overwritten. Returns OK on success, and a non-OK status
  // on error.  It is not an error if "key" did not exist in the database.
  //
  // If a key is overwritten (by calling Put() multiple times), then the result
  // of calling SingleDelete() on this key is undefined.  SingleDelete() only
  // behaves correctly if there has been only one Put() for this key since the
  // previous call to SingleDelete() for this key.
  //
  // This feature is currently an experimental performance optimization
  // for a very specific workload.  It is up to the caller to ensure that
  // SingleDelete is only used for a key that is not deleted using Delete().
  // Mixing SingleDelete operations with Deletes can result in undefined behavior.
  //
  // Note: consider setting options.sync = true.
  virtual common::Status SingleDelete(const common::WriteOptions& options,
                                      ColumnFamilyHandle* column_family,
                                      const common::Slice& key) = 0;
  virtual common::Status SingleDelete(const common::WriteOptions& options,
                                      const common::Slice& key) {
    return SingleDelete(options, DefaultColumnFamily(), key);
  }

  // Apply the specified updates to the database.
  // If `updates` contains no update, WAL will still be synced if
  // options.sync=true.
  // Returns OK on success, non-OK on failure.
  // Note: consider setting options.sync = true.
  virtual common::Status Write(const common::WriteOptions& options,
                               WriteBatch* updates) = 0;

  virtual common::Status WriteAsync(const common::WriteOptions& options,
                                    WriteBatch* updates,
                                    common::AsyncCallback* call_back) {
    return common::Status::NotSupported(
        "This type of db do not support WriteAsync");
  }
  // If the database contains an entry for "key" store the
  // corresponding value in *value and return OK.
  //
  // If there is no entry for "key" leave *value unchanged and return
  // a status for which common::Status::IsNotFound() returns true.
  //
  // May return some other common::Status on an error.
  virtual inline common::Status Get(const common::ReadOptions& options,
                                    ColumnFamilyHandle* column_family,
                                    const common::Slice& key,
                                    std::string* value) {
    assert(value != nullptr);
    common::PinnableSlice pinnable_val(value);
    assert(!pinnable_val.IsPinned());
    auto s = Get(options, column_family, key, &pinnable_val);
    if (s.ok() && pinnable_val.IsPinned()) {
      value->assign(pinnable_val.data(), pinnable_val.size());
    }  // else value is already assigned
    return s;
  }
  virtual common::Status Get(const common::ReadOptions& options,
                             ColumnFamilyHandle* column_family,
                             const common::Slice& key,
                             common::PinnableSlice* value) = 0;
  virtual common::Status Get(const common::ReadOptions& options,
                             const common::Slice& key, std::string* value) {
    return Get(options, DefaultColumnFamily(), key, value);
  }

  // If keys[i] does not exist in the database, then the i'th returned
  // status will be one for which common::Status::IsNotFound() is true, and
  // (*values)[i] will be set to some arbitrary value (often ""). Otherwise,
  // the i'th returned status will have common::Status::ok() true, and
  // (*values)[i]
  // will store the value associated with keys[i].
  //
  // (*values) will always be resized to be the same size as (keys).
  // Similarly, the number of returned statuses will be the number of keys.
  // Note: keys will not be "de-duplicated". Duplicate keys will return
  // duplicate values in order.
  virtual std::vector<common::Status> MultiGet(
      const common::ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<common::Slice>& keys,
      std::vector<std::string>* values) = 0;
  virtual std::vector<common::Status> MultiGet(
      const common::ReadOptions& options,
      const std::vector<common::Slice>& keys,
      std::vector<std::string>* values) {
    return MultiGet(options, std::vector<ColumnFamilyHandle*>(
                                 keys.size(), DefaultColumnFamily()),
                    keys, values);
  }

  // If the key definitely does not exist in the database, then this method
  // returns false, else true. If the caller wants to obtain value when the key
  // is found in memory, a bool for 'value_found' must be passed. 'value_found'
  // will be true on return if value has been set properly.
  // This check is potentially lighter-weight than invoking DB::Get(). One way
  // to make this lighter weight is to avoid doing any IOs.
  // Default implementation here returns true and sets 'value_found' to false
  virtual bool KeyMayExist(const common::ReadOptions& /*options*/,
                           ColumnFamilyHandle* /*column_family*/,
                           const common::Slice& /*key*/, std::string* /*value*/,
                           bool* value_found = nullptr) {
    if (value_found != nullptr) {
      *value_found = false;
    }
    return true;
  }
  virtual bool KeyMayExist(const common::ReadOptions& options,
                           const common::Slice& key, std::string* value,
                           bool* value_found = nullptr) {
    return KeyMayExist(options, DefaultColumnFamily(), key, value, value_found);
  }

  // Return a heap-allocated iterator over the contents of the database.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  //
  // Caller should delete the iterator when it is no longer needed.
  // The returned iterator should be deleted before this db is deleted.
  virtual Iterator* NewIterator(const common::ReadOptions& options,
                                ColumnFamilyHandle* column_family) = 0;
  virtual Iterator* NewIterator(const common::ReadOptions& options) {
    return NewIterator(options, DefaultColumnFamily());
  }

  // Return a handle to the current DB state.  Iterators created with
  // this handle will all observe a stable snapshot of the current DB
  // state.  The caller must call ReleaseSnapshot(result) when the
  // snapshot is no longer needed.
  //
  // nullptr will be returned if the DB fails to take a snapshot or does
  // not support snapshot.
  virtual const Snapshot* GetSnapshot() = 0;

  // Release a previously acquired snapshot.  The caller must not
  // use "snapshot" after this call.
  virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;

#ifndef ROCKSDB_LITE
  // Contains all valid property arguments for GetProperty().
  //
  // NOTE: Property names cannot end in numbers since those are interpreted as
  //       arguments, e.g., see kNumFilesAtLevelPrefix.
  struct Properties {
    //  "smartengine.num-files-at-level<N>" - returns string containing the number
    //      of files at level <N>, where <N> is an ASCII representation of a
    //      level number (e.g., "0").
    static const std::string kNumFilesAtLevelPrefix;

    //  "smartengine.compression-ratio-at-level<N>" - returns string containing the
    //      compression ratio of data at level <N>, where <N> is an ASCII
    //      representation of a level number (e.g., "0"). Here, compression
    //      ratio is defined as uncompressed data size / compressed file size.
    //      Returns "-1.0" if no open files at level <N>.
    static const std::string kCompressionRatioAtLevelPrefix;

    //  "smartengine.stats" - returns a multi-line string containing the data
    //      described by kCFStats followed by the data described by kDBStats.
    static const std::string kStats;

    //  "smartengine.sstables" - returns a multi-line string summarizing current
    //      SST files.
    static const std::string kSSTables;

    //  "smartengine.cfstats" - Both of "smartengine.cfstats-no-file-histogram" and
    //      "smartengine.cf-file-histogram" together. See below for description
    //      of the two.
    static const std::string kCFStats;

    //  "smartengine.cfstats-no-file-histogram" - returns a multi-line string with
    //      general columm family stats per-level over db's lifetime ("L<n>"),
    //      aggregated over db's lifetime ("Sum"), and aggregated over the
    //      interval since the last retrieval ("Int").
    //  It could also be used to return the stats in the format of the map.
    //  In this case there will a pair of string to array of double for
    //  each level as well as for "Sum". "Int" stats will not be affected
    //  when this form of stats are retrived.
    static const std::string kCFStatsNoFileHistogram;

    //  "smartengine.cf-file-histogram" - print out how many file reads to every
    //      level, as well as the histogram of latency of single requests.
    static const std::string kCFFileHistogram;

    //  "smartengine.dbstats" - returns a multi-line string with general database
    //      stats, both cumulative (over the db's lifetime) and interval (since
    //      the last retrieval of kDBStats).
    static const std::string kDBStats;

    //  "smartengine.compactions" - returns a multi-line string with compaction stats
    //      which mainly contain L0 and L1 information
    static const std::string kCompactionStats;

    //  "smartengine.meta" - returns a multi-line string with storage manager meta
    static const std::string kMeta;

    //  "smartengine.levelstats" - returns multi-line string containing the number
    //      of files per level and total size of each level (MB).
    static const std::string kLevelStats;

    //  "smartengine.num-immutable-mem-table" - returns number of immutable
    //      memtables that have not yet been flushed.
    static const std::string kNumImmutableMemTable;

    //  "smartengine.num-immutable-mem-table-flushed" - returns number of immutable
    //      memtables that have already been flushed.
    static const std::string kNumImmutableMemTableFlushed;

    //  "smartengine.mem-table-flush-pending" - returns 1 if a memtable flush is
    //      pending; otherwise, returns 0.
    static const std::string kMemTableFlushPending;

    //  "smartengine.num-running-flushes" - returns the number of currently running
    //      flushes.
    static const std::string kNumRunningFlushes;

    //  "smartengine.compaction-pending" - returns 1 if at least one compaction is
    //      pending; otherwise, returns 0.
    static const std::string kCompactionPending;

    //  "smartengine.num-running-compactions" - returns the number of currently
    //      running compactions.
    static const std::string kNumRunningCompactions;

    //  "smartengine.background-errors" - returns accumulated number of background
    //      errors.
    static const std::string kBackgroundErrors;

    //  "smartengine.cur-size-active-mem-table" - returns approximate size of active
    //      memtable (bytes).
    static const std::string kCurSizeActiveMemTable;

    //  "smartengine.cur-size-all-mem-tables" - returns approximate size of active
    //      and unflushed immutable memtables (bytes).
    static const std::string kCurSizeAllMemTables;

    //  "smartengine.size-all-mem-tables" - returns approximate size of active,
    //      unflushed immutable, and pinned immutable memtables (bytes).
    static const std::string kSizeAllMemTables;

    //  "smartengine.num-entries-active-mem-table" - returns total number of entries
    //      in the active memtable.
    static const std::string kNumEntriesActiveMemTable;

    //  "smartengine.num-entries-imm-mem-tables" - returns total number of entries
    //      in the unflushed immutable memtables.
    static const std::string kNumEntriesImmMemTables;

    //  "smartengine.num-deletes-active-mem-table" - returns total number of delete
    //      entries in the active memtable.
    static const std::string kNumDeletesActiveMemTable;

    //  "smartengine.num-deletes-imm-mem-tables" - returns total number of delete
    //      entries in the unflushed immutable memtables.
    static const std::string kNumDeletesImmMemTables;

    //  "smartengine.estimate-num-keys" - returns estimated number of total keys in
    //      the active and unflushed immutable memtables and storage.
    static const std::string kEstimateNumKeys;

    //  "smartengine.estimate-table-readers-mem" - returns estimated memory used for
    //      reading SST tables, excluding memory used in block cache (e.g.,
    //      filter and index blocks).
    static const std::string kEstimateTableReadersMem;

    //  "smartengine.is-file-deletions-enabled" - returns 0 if deletion of obsolete
    //      files is enabled; otherwise, returns a non-zero number.
    static const std::string kIsFileDeletionsEnabled;

    //  "smartengine.num-snapshots" - returns number of unreleased snapshots of the
    //      database.
    static const std::string kNumSnapshots;

    //  "smartengine.oldest-snapshot-time" - returns number representing unix
    //      timestamp of oldest unreleased snapshot.
    static const std::string kOldestSnapshotTime;

    //  "smartengine.num-live-versions" - returns number of live versions. `Version`
    //      is an internal data structure. See version_set.h for details. More
    //      live versions often mean more SST files are held from being deleted,
    //      by iterators or unfinished compactions.
    static const std::string kNumLiveVersions;

    //  "smartengine.current-super-version-number" - returns number of curent LSM
    //  version. It is a uint64_t integer number, incremented after there is
    //  any change to the LSM tree. The number is not preserved after restarting
    //  the DB. After DB restart, it will start from 0 again.
    static const std::string kCurrentSuperVersionNumber;

    //  "smartengine.estimate-live-data-size" - returns an estimate of the amount of
    //      live data in bytes.
    static const std::string kEstimateLiveDataSize;

    //  "smartengine.min-log-number-to-keep" - return the minmum log number of the
    //      log files that should be kept.
    static const std::string kMinLogNumberToKeep;

    //  "smartengine.total-sst-files-size" - returns total size (bytes) of all SST
    //      files.
    //  WARNING: may slow down online queries if there are too many files.
    static const std::string kTotalSstFilesSize;

    //  "smartengine.base-level" - returns number of level to which L0 data will be
    //      compacted.
    static const std::string kBaseLevel;

    //  "smartengine.estimate-pending-compaction-bytes" - returns estimated total
    //      number of bytes compaction needs to rewrite to get all levels down
    //      to under target size. Not valid for other compactions than level-
    //      based.
    static const std::string kEstimatePendingCompactionBytes;

    //  "smartengine.aggregated-table-properties" - returns a string representation
    //      of the aggregated table properties of the target column family.
    static const std::string kAggregatedTableProperties;

    //  "smartengine.aggregated-table-properties-at-level<N>", same as the previous
    //      one but only returns the aggregated table properties of the
    //      specified level "N" at the target column family.
    static const std::string kAggregatedTablePropertiesAtLevel;

    //  "smartengine.actual-delayed-write-rate" - returns the current actual delayed
    //      write rate. 0 means no delay.
    static const std::string kActualDelayedWriteRate;

    //  "smartengine.is-write-stopped" - Return 1 if write has been stopped.
    static const std::string kIsWriteStopped;

    static const std::string kDBMemoryStats;

    static const std::string kActiveMemTableTotalNumber;

    static const std::string kActiveMemTableTotalMemoryAllocated;

    static const std::string kActiveMemTableTotalMemoryUsed;

    static const std::string kUnflushedImmTableTotalNumber;

    static const std::string kUnflushedImmTableTotalMemoryAllocated;

    static const std::string kUnflushedImmTableTotalMemoryUsed;

    static const std::string kTableReaderTotalNumber;

    static const std::string kTableReaderTotalMemoryUsed;

    static const std::string kBlockCacheTotalPinnedMemory;

    static const std::string kBlockCacheTotalMemoryUsed;

    static const std::string kActiveWALTotalNumber;

    static const std::string kActiveWALTotalBufferSize;

    static const std::string kDBTotalMemoryAllocated;
  };
#endif /* ROCKSDB_LITE */

  // DB implementations can export properties about their state via this method.
  // If "property" is a valid property understood by this DB implementation (see
  // Properties struct above for valid options), fills "*value" with its current
  // value and returns true.  Otherwise, returns false.
  virtual bool GetProperty(ColumnFamilyHandle* column_family,
                           const common::Slice& property,
                           std::string* value) = 0;
  virtual bool GetProperty(const common::Slice& property, std::string* value) {
    return GetProperty(DefaultColumnFamily(), property, value);
  }
  virtual bool GetMapProperty(ColumnFamilyHandle* column_family,
                              const common::Slice& property,
                              std::map<std::string, double>* value) = 0;
  virtual bool GetMapProperty(const common::Slice& property,
                              std::map<std::string, double>* value) {
    return GetMapProperty(DefaultColumnFamily(), property, value);
  }

  // Similar to GetProperty(), but only works for a subset of properties whose
  // return value is an integer. Return the value by integer. Supported
  // properties:
  //  "smartengine.num-immutable-mem-table"
  //  "smartengine.mem-table-flush-pending"
  //  "smartengine.compaction-pending"
  //  "smartengine.background-errors"
  //  "smartengine.cur-size-active-mem-table"
  //  "smartengine.cur-size-all-mem-tables"
  //  "smartengine.size-all-mem-tables"
  //  "smartengine.num-entries-active-mem-table"
  //  "smartengine.num-entries-imm-mem-tables"
  //  "smartengine.num-deletes-active-mem-table"
  //  "smartengine.num-deletes-imm-mem-tables"
  //  "smartengine.estimate-num-keys"
  //  "smartengine.estimate-table-readers-mem"
  //  "smartengine.is-file-deletions-enabled"
  //  "smartengine.num-snapshots"
  //  "smartengine.oldest-snapshot-time"
  //  "smartengine.num-live-versions"
  //  "smartengine.current-super-version-number"
  //  "smartengine.estimate-live-data-size"
  //  "smartengine.min-log-number-to-keep"
  //  "smartengine.total-sst-files-size"
  //  "smartengine.base-level"
  //  "smartengine.estimate-pending-compaction-bytes"
  //  "smartengine.num-running-compactions"
  //  "smartengine.num-running-flushes"
  //  "smartengine.actual-delayed-write-rate"
  //  "smartengine.is-write-stopped"
  virtual bool GetIntProperty(ColumnFamilyHandle* column_family,
                              const common::Slice& property,
                              uint64_t* value) = 0;
  virtual bool GetIntProperty(const common::Slice& property, uint64_t* value) {
    return GetIntProperty(DefaultColumnFamily(), property, value);
  }

  // Reset internal stats for DB and all column families.
  // Note this doesn't reset options.statistics as it is not owned by
  // DB.
  virtual common::Status ResetStats() {
    return common::Status::NotSupported("Not implemented");
  }

  // Same as GetIntProperty(), but this one returns the aggregated int
  // property from all column families.
  virtual bool GetAggregatedIntProperty(const common::Slice& property,
                                        uint64_t* value) = 0;

  // Flags for DB::GetSizeApproximation that specify whether memtable
  // stats should be included, or file stats approximation or both
  enum SizeApproximationFlags : uint8_t {
    NONE = 0,
    INCLUDE_MEMTABLES = 1,
    INCLUDE_FILES = 1 << 1
  };

  // For each i in [0,n-1], store in "sizes[i]", the approximate
  // file system space used by keys in "[range[i].start .. range[i].limit)".
  //
  // Note that the returned sizes measure file system space usage, so
  // if the user data compresses by a factor of ten, the returned
  // sizes will be one-tenth the size of the corresponding user data size.
  //
  // If include_flags defines whether the returned size should include
  // the recently written data in the mem-tables (if
  // the mem-table type supports it), data serialized to disk, or both.
  // include_flags should be of type DB::SizeApproximationFlags
  virtual void GetApproximateSizes(ColumnFamilyHandle* column_family,
                                   const Range* range, int n, uint64_t* sizes,
                                   uint8_t include_flags = INCLUDE_FILES) = 0;
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes,
                                   uint8_t include_flags = INCLUDE_FILES) {
    GetApproximateSizes(DefaultColumnFamily(), range, n, sizes, include_flags);
  }

  // The method is similar to GetApproximateSizes, except it
  // returns approximate number of records in memtables.
  virtual void GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                           const Range& range,
                                           uint64_t* const count,
                                           uint64_t* const size) = 0;
  virtual void GetApproximateMemTableStats(const Range& range,
                                           uint64_t* const count,
                                           uint64_t* const size) {
    GetApproximateMemTableStats(DefaultColumnFamily(), range, count, size);
  }

  // Compact the underlying storage for the key range [*begin,*end].
  // The actual compaction interval might be superset of [*begin, *end].
  // In particular, deleted and overwritten versions are discarded,
  // and the data is rearranged to reduce the cost of operations
  // needed to access the data.  This operation should typically only
  // be invoked by users who understand the underlying implementation.
  //
  // begin==nullptr is treated as a key before all keys in the database.
  // end==nullptr is treated as a key after all keys in the database.
  // Therefore the following call will compact the entire database:
  //    db->CompactRange(options, nullptr, nullptr);
  // Note that after the entire database is compacted, all data are pushed
  // down to the last level containing any data. If the total data size after
  // compaction is reduced, that level might not be appropriate for hosting all
  // the files. In this case, client could set options.change_level to true, to
  // move the files back to the minimum level capable of holding the data set
  // or a given level (specified by non-negative options.target_level).
  virtual common::Status CompactRange(ColumnFamilyHandle* column_family,
                                      const uint32_t manual_compact_type) = 0;
  //TODO:Zhao Dongsheng, this interface unused?
  virtual common::Status CompactRange(const uint32_t manual_compact_type)
  {
    return CompactRange(DefaultColumnFamily(), manual_compact_type);
  }

  virtual int reset_pending_shrink(const uint64_t subtable_id) = 0;

  virtual common::Status SetOptions(
      ColumnFamilyHandle* /*column_family*/,
      const std::unordered_map<std::string, std::string>& /*new_options*/) {
    return common::Status::NotSupported("Not implemented");
  }
  virtual common::Status SetOptions(
      const std::unordered_map<std::string, std::string>& new_options) {
    return SetOptions(DefaultColumnFamily(), new_options);
  }

  virtual common::Status SetDBOptions(
      const std::unordered_map<std::string, std::string>& new_options) = 0;

  // This function will wait until all currently running background processes
  // finish. After it returns, no background process will be run until
  // UnblockBackgroundWork is called
  virtual common::Status PauseBackgroundWork() = 0;
  virtual common::Status ContinueBackgroundWork() = 0;
  virtual void CancelAllBackgroundWork(bool wait) = 0;

  // This function will enable automatic compactions for the given column
  // families if they were previously disabled. The function will first set the
  // disable_auto_compactions option for each column family to 'false', after
  // which it will schedule a flush/compaction.
  //
  // NOTE: Setting disable_auto_compactions to 'false' through SetOptions() API
  // does NOT schedule a flush/compaction afterwards, and only changes the
  // parameter itself within the column family option.
  //
  virtual common::Status EnableAutoCompaction(
      const std::vector<ColumnFamilyHandle*>& column_family_handles) = 0;

  // Get DB name -- the exact same name that was provided as an argument to
  // DB::Open()
  virtual const std::string& GetName() const = 0;

  // Get Env object from the DB
  virtual util::Env* GetEnv() const = 0;

  // Get DB Options that we use.  During the process of opening the
  // column family, the options provided when calling DB::Open() or
  // DB::CreateColumnFamily() will have been "sanitized" and transformed
  // in an implementation-defined manner.
  virtual common::Options GetOptions(
      ColumnFamilyHandle* column_family) const = 0;
  virtual common::Options GetOptions() const {
    return GetOptions(DefaultColumnFamily());
  }

  virtual common::DBOptions GetDBOptions() const = 0;

  // Flush all mem-table data.
  virtual common::Status Flush(const common::FlushOptions& options,
                               ColumnFamilyHandle* column_family) = 0;
  virtual common::Status Flush(const common::FlushOptions& options) {
    return Flush(options, DefaultColumnFamily());
  }

  // Sync the wal. Note that Write() followed by SyncWAL() is not exactly the
  // same as Write() with sync=true: in the latter case the changes won't be
  // visible until the sync is done.
  // Currently only works if allow_mmap_writes = false in Options.
  virtual common::Status SyncWAL() = 0;

  // The sequence number of the most recent transaction.
  virtual common::SequenceNumber GetLatestSequenceNumber() const = 0;

#ifndef ROCKSDB_LITE

  // Prevent file deletions. Compactions will continue to occur,
  // but no obsolete files will be deleted. Calling this multiple
  // times have the same effect as calling it once.
  virtual common::Status DisableFileDeletions() = 0;

  // Allow compactions to delete obsolete files.
  // If force == true, the call to EnableFileDeletions() will guarantee that
  // file deletions are enabled after the call, even if DisableFileDeletions()
  // was called multiple times before.
  // If force == false, EnableFileDeletions will only enable file deletion
  // after it's been called at least as many times as DisableFileDeletions(),
  // enabling the two methods to be called by two threads concurrently without
  // synchronization -- i.e., file deletions will be enabled only after both
  // threads call EnableFileDeletions()
  virtual common::Status EnableFileDeletions(bool force = true) = 0;

  // GetLiveFiles followed by GetSortedWalFiles can generate a lossless backup

  // Retrieve the list of all files in the database. The files are
  // relative to the dbname and are not absolute paths. The valid size of the
  // manifest file is returned in manifest_file_size. The manifest file is an
  // ever growing file, but only the portion specified by manifest_file_size is
  // valid for this snapshot.
  // Setting flush_memtable to true does Flush before recording the live files.
  // Setting flush_memtable to false is useful when we don't want to wait for
  // flush which may have to wait for compaction to complete taking an
  // indeterminate time.
  //
  // In case you have multiple column families, even if flush_memtable is true,
  // you still need to call GetSortedWalFiles after GetLiveFiles to compensate
  // for new data that arrived to already-flushed column families while other
  // column families were flushing
  virtual common::Status GetLiveFiles(std::vector<std::string>&,
                                      uint64_t* manifest_file_size,
                                      bool flush_memtable = true) = 0;

  // Retrieve the sorted list of all wal files with earliest file first
  virtual common::Status GetSortedWalFiles(VectorLogPtr& files) = 0;

  // for hotbackup
  virtual int create_backup_snapshot(MetaSnapshotMap &meta_snapshot,
                                     int32_t &last_manifest_file_num,
                                     uint64_t &last_manifest_file_size,
                                     uint64_t &last_wal_file_num)
  {
    return common::Status::kNotSupported;
  }
  virtual int record_incremental_extent_ids(const int32_t first_manifest_file_num,
                                            const int32_t last_manifest_file_num,
                                            const uint64_t last_manifest_file_size)
  {
    return common::Status::kNotSupported;
  }

  virtual int64_t get_last_wal_file_size() const { return -1; }

  virtual int release_backup_snapshot(MetaSnapshotMap &meta_snapshot)
  {
    return common::Status::kNotSupported;
  }

  virtual int64_t backup_manifest_file_size() const { return -1; }

  virtual int shrink_table_space(int32_t table_space_id) = 0;

  // information schema
  virtual int get_all_subtable(std::vector<smartengine::db::ColumnFamilyHandle*>
                               &subtables) const = 0;

  virtual int return_all_subtable(std::vector<smartengine::db::ColumnFamilyHandle*> &subtables) = 0;

  virtual db::SuperVersion *GetAndRefSuperVersion(db::ColumnFamilyData* cfd) = 0;

  virtual void ReturnAndCleanupSuperVersion(db::ColumnFamilyData *cfd, db::SuperVersion *sv) = 0;

  virtual int get_data_file_stats(std::vector<storage::DataFileStatistics> &data_file_stats) = 0;

  // information schema
  virtual std::list<storage::CompactionJobStatsInfo*> &get_compaction_history(std::mutex **mu,
          storage::CompactionJobStatsInfo **sum) = 0;
  // Sets iter to an iterator that is positioned at a write-batch containing
  // seq_number. If the sequence number is non existent, it returns an iterator
  // at the first available seq_no after the requested seq_no
  // Returns common::Status::OK if iterator is valid
  // Must set WAL_ttl_seconds or WAL_size_limit_MB to large values to
  // use this api, else the WAL files will get
  // cleared aggressively and the iterator might keep getting invalid before
  // an update is read.
  virtual common::Status GetUpdatesSince(
      common::SequenceNumber seq_number,
      unique_ptr<TransactionLogIterator>* iter,
      const TransactionLogIterator::ReadOptions& read_options =
          TransactionLogIterator::ReadOptions()) = 0;

// Windows API macro interference
#undef DeleteFile
  // Delete the file name from the db directory and update the internal state to
  // reflect that. Supports deletion of sst and log files only. 'name' must be
  // path relative to the db directory. eg. 000001.sst, /archive/000003.log
  virtual common::Status DeleteFile(std::string name) = 0;

  /*
   * for bulkload
   */
  virtual common::Status InstallSstExternal(ColumnFamilyHandle* column_family,
                                            db::MiniTables* mtables) {
    return common::Status::OK();
  }
  virtual storage::StorageLogger * GetStorageLogger() {
    return nullptr;
  }

#endif  // ROCKSDB_LITE

  // Sets the globally unique ID created at database creation time by invoking
  // Env::GenerateUniqueId(), in identity. Returns common::Status::OK if
  // identity could
  // be set properly
  virtual common::Status GetDbIdentity(std::string& identity) const = 0;

  // Returns default column family handle
  virtual ColumnFamilyHandle* DefaultColumnFamily() const = 0;

#ifndef ROCKSDB_LITE
  virtual common::Status GetPropertiesOfAllTables(
      ColumnFamilyHandle* column_family, TablePropertiesCollection* props) = 0;
  virtual common::Status GetPropertiesOfAllTables(
      TablePropertiesCollection* props) {
    return GetPropertiesOfAllTables(DefaultColumnFamily(), props);
  }
  virtual common::Status GetPropertiesOfTablesInRange(
      ColumnFamilyHandle* column_family, const Range* range, std::size_t n,
      TablePropertiesCollection* props) = 0;
#endif  // ROCKSDB_LITE

  // Needed for StackableDB
  virtual DB* GetRootDB() { return this; }

  // Used to switch on/off MajorCompaction (L1->L2),
  // turn on when flag is true;
  virtual common::Status switch_major_compaction(
      const std::vector<ColumnFamilyHandle*>& column_family_handles, bool flag) {
    return common::Status::OK();
  }

  virtual common::Status disable_backgroud_merge(const std::vector<ColumnFamilyHandle*>& column_family_handles) {
    return common::Status::OK();
  }

  virtual common::Status enable_backgroud_merge(const std::vector<ColumnFamilyHandle*>& column_family_handles) {
    return common::Status::OK();
  }

  // hot backup
  virtual int do_manual_checkpoint(int32_t &manifest_file_num)
  {
    return common::Status::kNotSupported;
  }
  virtual int stream_log_extents(
              std::function<int(const char*, int, int64_t, int)> *stream_extent,
              int64_t start, int64_t end, int dest_fd) {
    return 0;
  }

  virtual bool get_columnfamily_stats(ColumnFamilyHandle* column_family, int64_t &data_size,
                                      int64_t &num_entries, int64_t &num_deletes, int64_t &disk_size) { return false; }

 private:
  // No copying allowed
  DB(const DB&);
  void operator=(const DB&);
};

// Destroy the contents of the specified database.
// Be very careful using this method.
common::Status DestroyDB(const std::string& name,
                         const common::Options& options);

#ifndef ROCKSDB_LITE
// If a DB cannot be opened, you may attempt to call this method to
// resurrect as much of the contents of the database as possible.
// Some data may be lost, so be careful when calling this function
// on a database that contains important information.
//
// With this API, we will warn and skip data associated with column families not
// specified in column_families.
//
// @param column_families Descriptors for known column families
common::Status RepairDB(
    const std::string& dbname, const common::DBOptions& db_options,
    const std::vector<ColumnFamilyDescriptor>& column_families);

// @param unknown_cf_opts Options for column families encountered during the
//                        repair that were not specified in column_families.
common::Status RepairDB(
    const std::string& dbname, const common::DBOptions& db_options,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    const common::ColumnFamilyOptions& unknown_cf_opts);

// @param options These options will be used for the database and for ALL column
//                families encountered during the repair
common::Status RepairDB(const std::string& dbname,
                        const common::Options& options);

#endif

}  // namespace db
}  // namespace smartengine

#endif  // STORAGE_ROCKSDB_INCLUDE_DB_H_
