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

#include "table/schema_struct.h"

namespace smartengine
{
namespace table
{
bool is_data_column(uint8_t col_type)
{
  return PRIMARY_COLUMN == col_type || BLOB_COLUMN == col_type
      || VARCHAR_COLUMN == col_type || FIXED_COLUMN == col_type;
}

ColumnSchema::ColumnSchema()
    : type_(ColumnType::INVALID_COLUMN_TYPE),
      attr_(0),
      column_count_(0),
      null_bitmap_size_(0),
      original_column_count_(0),
      original_null_bitmap_size_(0),
      null_mask_(0),
      null_offset_(0),
      data_size_(0)
{}

ColumnSchema::ColumnSchema(const ColumnSchema &column_schema)
    : type_(column_schema.type_),
      attr_(column_schema.attr_),
      column_count_(column_schema.column_count_),
      null_bitmap_size_(column_schema.null_bitmap_size_),
      original_column_count_(column_schema.original_column_count_),
      original_null_bitmap_size_(column_schema.original_null_bitmap_size_),
      null_mask_(column_schema.null_mask_),
      null_offset_(column_schema.null_offset_),
      data_size_(column_schema.data_size_)
{}

ColumnSchema &ColumnSchema::operator=(const ColumnSchema &column_schema)
{
  type_ = column_schema.type_;
  attr_ = column_schema.attr_;
  column_count_ = column_schema.column_count_;
  null_bitmap_size_ = column_schema.null_bitmap_size_;
  original_column_count_ = column_schema.original_column_count_;
  original_null_bitmap_size_ = column_schema.original_null_bitmap_size_;
  null_mask_ = column_schema.null_mask_;
  null_offset_ = column_schema.null_offset_;
  data_size_ = column_schema.data_size_;

  return *this;
}

ColumnSchema::~ColumnSchema()
{}

void ColumnSchema::reset()
{
  type_ = INVALID_COLUMN_TYPE;
  attr_ = 0;
  column_count_ = 0;
  null_bitmap_size_ = 0;
  original_column_count_ = 0;
  original_null_bitmap_size_ = 0;
  null_mask_ = 0;
  null_offset_ = 0;
  data_size_ = 0;
}

bool ColumnSchema::is_valid() const
{
  bool res = (type_ > INVALID_COLUMN_TYPE);

  if (res && is_data_column(type_)) {
    res = res && ((is_nullable() && null_mask_ > 0) ||
        (!is_nullable() && (0 == null_offset_) && (0 == null_mask_)));
  }

  if (res && (RECORD_HEADER == type_)) {
    res = res && (column_count_ > 0);
    res = res && ((is_nullable() && (null_bitmap_size_ > 0)) || !is_nullable());
    res = res && ((is_instant() && original_column_count_ > 0) || !is_instant());
  }

  return res;
}

DEFINE_COMPACTIPLE_SERIALIZATION(ColumnSchema, type_, attr_, column_count_,
    null_bitmap_size_, original_column_count_, original_null_bitmap_size_,
    null_mask_, null_offset_, data_size_)

DEFINE_TO_STRING(ColumnSchema, KV_(type), KV_(attr), KV_(column_count),
    KV_(null_bitmap_size), KV_(original_null_bitmap_size), KV_(original_column_count),
    KV_(original_null_bitmap_size), KV_(null_mask), KV_(null_offset), KV_(data_size))

TableSchema::TableSchema()
    : primary_index_id_(0),
      attr_(0),
      packed_column_count_(0),
      column_schemas_()
{}

TableSchema::TableSchema(const TableSchema &table_schema)
    : primary_index_id_(table_schema.primary_index_id_),
      attr_(table_schema.attr_),
      packed_column_count_(table_schema.packed_column_count_),
      column_schemas_(table_schema.column_schemas_)
{}

TableSchema &TableSchema::operator=(const TableSchema &table_schema)
{
  primary_index_id_ = table_schema.primary_index_id_;
  attr_ = table_schema.attr_;
  packed_column_count_ = table_schema.packed_column_count_;
  column_schemas_ = table_schema.column_schemas_;

  return *this;
}

TableSchema::~TableSchema()
{}

void TableSchema::reset()
{
  primary_index_id_ = 0;
  attr_ = 0;
  packed_column_count_ = 0;
  column_schemas_.clear();
}

bool TableSchema::is_valid() const
{
  bool res = true;
  //TODO:Zhao Dongsheng, the hard code "1" should optimize
  //Check table schema's validation except system and default subtable
  if (primary_index_id_ > 1) {
    res = column_schemas_.size() >= 2;
    std::for_each(column_schemas_.begin(), column_schemas_.end(),
        [&](const ColumnSchema &schema) {
          res = res && schema.is_valid();
        });
  }

  return res;
}

DEFINE_TO_STRING(TableSchema, KV_(primary_index_id), KV_(attr), KV_(packed_column_count), KV_(column_schemas))

DEFINE_COMPACTIPLE_SERIALIZATION(TableSchema, primary_index_id_, attr_, packed_column_count_, column_schemas_)

} // namespace table
} // namespace smartengine