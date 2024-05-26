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

#pragma once

#include "util/serialization.h"
#include "util/to_string.h"
#include <cstdint>

namespace smartengine
{
namespace table
{

/** smartengine record storage format:
+----------+----------+----------+------------------+-----------+----------------+----------------+-----------+------------+
|memcmp key| rec hdr  |field num |null-bitmap length|null-bitmap|unpack data flag|unpack data size|unpack data|non-pk field|
| (varlen) | (1 byte) |(2 bytes) |    (2 bytes)     | (varlen)  |    (1 byte)    |   (2 bytes)    | (varlen)  |  (varlen)  |
|          |          |(optional)|    (optional)    | (optional)|   (optional)   |   (optional)   | (optional)|            |
+----------+----------+----------+------------------+-----------+----------------+----------------+-----------+------------+
mandatory part: memcmp key(comparable primary fields), record header, non-primary key fields.
optional part(instant ddl): field number, null-bitmap length, null-bitmap.
optional part(unpack data): unpack data flag, unpack data size, unpack data.
*/
struct RecordFormat
{
  /**byte size of record header*/
  static const int64_t RECORD_HEADER_SIZE = 1;
  /**byte size of field number*/
  static const int64_t RECORD_FIELD_NUMBER_SIZE = 2;
  /**byte size of nullable field bitmap*/
  static const int64_t RECORD_NULL_BITMAP_SIZE_BYTES = 2;
  /**instant ddl flag*/
  static const uint8_t RECORD_INSTANT_DDL_FLAG = 0x80;
  /**unpack data flag*/
  static const uint8_t RECORD_UNPACK_DATA_FLAG = 0x02;
  /**unpack data flag size*/
  static const int64_t RECORD_UNPACK_DATA_FLAG_SIZE = 1;
  /**byte size of unpack data size*/
  static const int64_t RECORD_UNPACK_DATA_SIZE_BYTES = 2;
  /**byte size of unpack header, include "unpack data flag" and "unpack data size"*/
  static const int64_t RECORD_UNPACK_HEADER_SIZE = RECORD_UNPACK_DATA_FLAG_SIZE + RECORD_UNPACK_DATA_SIZE_BYTES;
};

enum ColumnType
{
  INVALID_COLUMN_TYPE = 0,
  PRIMARY_COLUMN = 1,
  RECORD_HEADER = 2,
  NULL_BITMAP = 3,
  UNPACK_INFO = 4,
  BLOB_COLUMN = 5,
  VARCHAR_COLUMN = 6,
  FIXED_COLUMN = 7,
  MAX_COLUMN_TYPE,
};

extern bool is_data_column(uint8_t col_type);

struct ColumnSchema
{
  static const int64_t COLUMN_SCHEMA_VERSION = 1;
public:
  ColumnSchema();
  ColumnSchema(const ColumnSchema &column_schema);
  ColumnSchema &operator=(const ColumnSchema &column_schema);
  ~ColumnSchema();
  void reset();
  bool is_valid() const;

  void set_nullable() { attr_ |= FLAG_MASK_NULLABLE; }
  void unset_nullable() { attr_ &= (~FLAG_MASK_NULLABLE); }
  void set_fixed() { attr_ |= FLAG_MASK_FIXED; }
  void unset_fixed() { attr_ &= (~FLAG_MASK_FIXED); }
  void set_instant() {attr_ |= FLAG_MASK_INSTANT; }
  void unset_instant() { attr_ &= (~FLAG_MASK_INSTANT); }
  bool is_nullable() const { return 0 != (attr_ & FLAG_MASK_NULLABLE); }
  bool is_fixed() const { return 0 != (attr_ & FLAG_MASK_FIXED); }
  bool is_instant() const { return 0 != (attr_ & FLAG_MASK_INSTANT); }
  bool is_primary() const { return PRIMARY_COLUMN == type_; }

  DECLARE_COMPACTIPLE_SERIALIZATION(COLUMN_SCHEMA_VERSION)
  DECLARE_TO_STRING()
public:
  uint8_t type_;

  /**column attributes flag:
  +----------+------------------------------------+
  | nullable |  fixed  | instant  | reserved bits |
  |  (bit 0) | (bit 1) |  (bit 2) |  (bit 3~7)    |
  +----------+---------+--------------------------+
  */
  uint8_t attr_;

  /**Specially for record header*/
  uint32_t column_count_;
  uint32_t null_bitmap_size_;
  uint32_t original_column_count_;
  uint32_t original_null_bitmap_size_;

  /**Specially for data column*/
  uint8_t null_mask_;
  uint32_t null_offset_;
  // data size can be zero, like binary(0).
  union {
    uint32_t data_size_;
    uint32_t data_size_bytes_;
  };

private:
  static const uint8_t FLAG_MASK_NULLABLE = 0x01;
  static const uint8_t FLAG_MASK_FIXED = 0x02;
  static const uint8_t FLAG_MASK_INSTANT = 0x04;
};
typedef std::vector<ColumnSchema> ColumnSchemaArray;

struct TableSchema
{
  static const int64_t TABLE_SCHEMA_VERSION = 1;
public:
  TableSchema();
  TableSchema(const TableSchema &table_schema);
  TableSchema &operator=(const TableSchema &table_schema);
  ~TableSchema();


  void reset();
  bool is_valid() const;
  void set_unpack() { attr_ |= FLAG_MASK_UNPACK; }
  void set_column_format() { attr_ |= FLAG_MASK_COLUMN_FORMAT; }
  void unset_unpack() { attr_ &= (~FLAG_MASK_UNPACK); }
  void unset_column_format() { attr_ &= (~FLAG_MASK_COLUMN_FORMAT); }
  bool has_unpack() const { return 0 != (attr_ & FLAG_MASK_UNPACK); }
  bool use_column_format() const { return 0 != (attr_ & FLAG_MASK_COLUMN_FORMAT); }

  DECLARE_COMPACTIPLE_SERIALIZATION(TABLE_SCHEMA_VERSION)
  DECLARE_TO_STRING()

public:
  int64_t primary_index_id_;
  /**table attributes flag:
  +-----------------+--------------------------+----------------+
  | has unpack info | has instant added column |  reserved bits |
  |     (bit 0)     |        (bit 1)           |   (bit 2~7)    |
  +-----------------+--------------------------+----------------+
  */
  int8_t attr_;
  /**The count of columns which have been packed to memory comparison format.
  The primary key needs to be assembled in a format suitable for memory comparison,
  which may include unpack information or not.Therefore, the parsing of the primary
  key requires the use of unpack function and unpack information, making it distinct
  from non-primary columns.*/
  uint32_t packed_column_count_;
  /**The table schema typically includes at least the following types of columns:
  1.PRIMARY_COLUMN, which represents all columns in primary key.
  2.RECORD_HEADER
  And optionaly includes following types of columns:
  3. NULL_BITMAP, if there has nullable column.
  4. UNPACK_INFO, for some charset.
  5. BLOB_COLUMN, VARCHAR_COLUMN, FIXED_COLUMN.*/
  ColumnSchemaArray column_schemas_;

private:
  static const uint8_t FLAG_MASK_UNPACK = 0x01;
  static const uint8_t FLAG_MASK_COLUMN_FORMAT = 0x02;
};

} // namespace table
} // namespace smartengine