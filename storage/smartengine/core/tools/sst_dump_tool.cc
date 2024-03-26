//  Portions Copyright (c) 2023, ApeCloud Inc Holding Limited
//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef ROCKSDB_LITE

#include "tools/sst_dump_tool_imp.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <vector>

#include "db/db.h"
#include "db/version_set.h"
#include "memtable/memtable.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "tools/ldb_cmd.h"
#include "storage/extent_space_manager.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "table/extent_table_builder.h"
#include "table/extent_table_factory.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/table_reader.h"
#include "util/compression.h"
#include "util/random.h"
#include "write_batch/write_batch_internal.h"

namespace smartengine {
using namespace table;
using namespace common;
using namespace util;
using namespace db;
using namespace memory;
using namespace logger;

namespace tools {

using std::dynamic_pointer_cast;

SstFileReader::SstFileReader(const std::string& file_path, bool verify_checksum,
                             bool output_hex, size_t extent_offset,
                             Options options)
    : file_name_(file_path),
      read_num_(0),
      verify_checksum_(verify_checksum),
      output_hex_(output_hex),
      options_(options),
      ioptions_(options_),
      internal_comparator_(BytewiseComparator()) {
  init_result_ = GetTableReader(file_name_, extent_offset);  // initialization
}

const char* testFileName = "test_file_name";

Status SstFileReader::GetTableReader(const std::string& file_path,
                                     size_t extent_offset) {
  // Warning about 'magic_number' being uninitialized shows up only in UBsan
  // builds. Though access is guarded by 's.ok()' checks, fix the issue to
  // avoid any warnings.
  uint64_t magic_number = Footer::kInvalidTableMagicNumber;

  // read table magic number
  Footer footer;

  // file descriptor
  int fd = open(file_path.c_str(), O_RDONLY, 0644);

  // extract file number form file path
  size_t found = file_path.rfind('/');  // the last '/'
  std::string file_name = std::string(file_path, found + 1);
  found = file_name.rfind('.');
  file_name = file_name.substr(0, found);  // erease ".sst"
  std::istringstream iss(file_name);
  Status s;
  int file_number = 0;
  iss >> file_number;
  if (iss.fail()) {
    s = Status::NotSupported(
        file_name +
        ".sst is not a correct sst file name which must be numeric");
    return s;
  }

  storage::ExtentId eid(file_number, extent_offset);
  uint64_t file_size = 0;
  if (s.ok()) {
    s = options_.env->GetFileSize(file_path, &file_size);
  }

  storage::RandomAccessExtent *extent = MOD_NEW_OBJECT(ModId::kDefaultMod, storage::RandomAccessExtent);
  storage::ExtentSpaceManager* fake_space = reinterpret_cast<storage::ExtentSpaceManager*>(1);
  storage::ExtentIOInfo io_info(fd, eid, storage::MAX_EXTENT_SIZE, storage::DATA_BLOCK_SIZE, 1);
  extent->init(io_info, fake_space);
  file_.reset(MOD_NEW_OBJECT(ModId::kDefaultMod,
      RandomAccessFileReader, extent, options_.env, nullptr, 0, nullptr, &ioptions_, EnvOptions()));

  if (s.ok()) {
    s = ReadFooterFromFile(file_.get(), file_size, &footer);
  }

  if (s.ok()) {
    magic_number = footer.table_magic_number();
    assert(kExtentBasedTableMagicNumber == magic_number);
  }

  if (s.ok()) {
    options_.comparator = &internal_comparator_;
    // For old sst format, ReadTableProperties might fail but file can be read
    if (ReadTableProperties(magic_number, file_.get(), file_size).ok()) {
      SetTableOptionsByMagicNumber(magic_number);
    }
  }

  if (s.ok()) {
    table::TableReader *table_reader = nullptr;
    s = NewTableReader(ioptions_, soptions_, internal_comparator_, file_size, table_reader);
    table_reader_.reset(table_reader);
  }
  return s;
}

Status SstFileReader::NewTableReader(
    const ImmutableCFOptions& ioptions, const EnvOptions& soptions,
    const InternalKeyComparator& internal_comparator, uint64_t file_size,
    TableReader *&table_reader) {

  // Not sure if this is 100% necessary, but it seems not a bad idea to have
  // the same logic as block based table.
  shared_ptr<ExtentBasedTableFactory> extent_based_table_factory =
      dynamic_pointer_cast<ExtentBasedTableFactory>(options_.table_factory);

  if (extent_based_table_factory) {
    return extent_based_table_factory->NewTableReader(
        TableReaderOptions(ioptions_, soptions_, internal_comparator_,
                           nullptr /* fd */, nullptr /* read hist */,
                           false /* skip_filters */),
                           file_.release(), file_size,
                           table_reader, /*enable_prefetch=*/false);
  }

  assert(!extent_based_table_factory);

  // For all other factory implementation
  Status s = options_.table_factory->NewTableReader(
      TableReaderOptions(ioptions_, soptions_, internal_comparator_),
      file_.release(), file_size, table_reader);
  return s;
}

Status SstFileReader::DumpTable(const std::string& out_filename) {
  unique_ptr<WritableFile, ptr_destruct_delete<WritableFile>> out_file_ptr;
  WritableFile *out_file = nullptr;
  Env* env = Env::Default();
  env->NewWritableFile(out_filename, out_file, soptions_);
  out_file_ptr.reset(out_file);
  Status s = table_reader_->DumpTable(out_file);
  out_file->Close();
  return s;
}


Status SstFileReader::ReadTableProperties(uint64_t table_magic_number,
                                          RandomAccessFileReader* file,
                                          uint64_t file_size) {
  TableProperties* table_properties = nullptr;
  Status s = table::ReadTableProperties(file, file_size, table_magic_number,
                                        ioptions_, &table_properties);
  if (s.ok()) {
    table_properties_.reset(table_properties);
  } else {
    fprintf(stdout, "Not able to read table properties\n");
  }
  return s;
}

Status SstFileReader::SetTableOptionsByMagicNumber(
    uint64_t table_magic_number) {
  assert(table_properties_);
  if (table_magic_number == kExtentBasedTableMagicNumber) {
    options_.table_factory = std::make_shared<ExtentBasedTableFactory>();
  } else {
    char error_msg_buffer[80];
    snprintf(error_msg_buffer, sizeof(error_msg_buffer) - 1,
             "Unsupported table magic number --- %lx",
             (long)table_magic_number);
    return Status::InvalidArgument(error_msg_buffer);
  }

  return Status::OK();
}


Status SstFileReader::ReadSequential(bool print_kv, uint64_t read_num,
                                     bool has_from, const std::string& from_key,
                                     bool has_to, const std::string& to_key,
                                     bool use_from_as_prefix) {
  if (!table_reader_) {
    return init_result_;
  }

  InternalIterator* iter =
      table_reader_->NewIterator(ReadOptions(verify_checksum_, false));
  uint64_t i = 0;
  if (has_from) {
    InternalKey ikey;
    ikey.SetMaxPossibleForUserKey(from_key);
    iter->Seek(ikey.Encode());
  } else {
    iter->SeekToFirst();
  }
  for (; iter->Valid(); iter->Next()) {
    Slice key = iter->key();
    Slice value = iter->value();
    ++i;
    if (read_num > 0 && i > read_num) break;

    ParsedInternalKey ikey;
    if (!ParseInternalKey(key, &ikey)) {
      std::cerr << "Internal Key [" << key.ToString(true /* in hex*/)
                << "] parse error!\n";
      continue;
    }

    // the key returned is not prefixed with out 'from' key
    if (use_from_as_prefix && !ikey.user_key.starts_with(from_key)) {
      break;
    }

    // If end marker was specified, we stop before it
    if (has_to && BytewiseComparator()->Compare(ikey.user_key, to_key) >= 0) {
      break;
    }

    if (print_kv) {
      fprintf(stdout, "%s => %s\n", ikey.DebugString(output_hex_).c_str(),
              value.ToString(output_hex_).c_str());
    }
  }

  read_num_ += i;

  Status ret = iter->status();
  //delete iter;
  return ret;
}

Status SstFileReader::ReadTableProperties(
    std::shared_ptr<const TableProperties>* table_properties) {
  if (!table_reader_) {
    return init_result_;
  }

  *table_properties = table_reader_->GetTableProperties();
  return init_result_;
}

namespace {

void print_help() {
  fprintf(stderr,
          R"(sst_dump --file=<data_dir_OR_sst_file> [--command=check|scan|raw]
    --file=<data_dir_OR_sst_file>
      Path to SST file or directory containing SST files

    --extent=extent_offset
      the specific extent offset

    --command=check|scan|raw
        check: Iterate over entries in files but dont print anything except if an error is encounterd (default command)
        scan: Iterate over entries in files and print them to screen
        raw: Dump all the table contents to <file_name>_dump.txt

    --output_hex
      Can be combined with scan command to print the keys and values in Hex

    --from=<user_key>
      Key to start reading from when executing check|scan

    --to=<user_key>
      Key to stop reading at when executing check|scan

    --prefix=<user_key>
      Returns all keys with this prefix when executing check|scan
      Cannot be used in conjunction with --from

    --read_num=<num>
      Maximum number of entries to read when executing check|scan

    --verify_checksum
      Verify file checksum when executing check|scan

    --input_key_hex
      Can be combined with --from and --to to indicate that these values are encoded in Hex

    --show_properties
      Print table properties after iterating over the file

    --set_block_size=<block_size>
      Can be combined with --show_compression_sizes to set the block size that will be used
      when trying different compression algorithms

    --parse_internal_key=<0xKEY>
      Convenience option to parse an internal key on the command line. Dumps the
      internal key in hex format {'key' @ SN: type}
)");
}

}  // namespace

int SSTDumpTool::Run(int argc, char** argv) {
  const char* dir_or_file = nullptr;
  uint64_t read_num = -1;  // limit the number of entries to print
  //'raw' will genarate a txt file while 'scan' will print entries
  std::string command;

  char junk;
  uint64_t n;
  bool verify_checksum = false;
  bool output_hex = false;     // hex format
  bool input_key_hex = false;  // the from and to keys' format is hex
  bool has_from = false;
  bool has_to = false;
  bool use_from_as_prefix = false;
  bool show_properties = false;         // print metainfo of the sst file
  bool show_summary = false;
  bool set_block_size = false;  // set the block size when using show_compresion_size
  bool set_extent_offset = false;
  std::string extent_str;
  std::string from_key;  //
  std::string to_key;    // from 'keym' to 'keyn' when 'scan'
  std::string block_size_str;
  size_t block_size;
  size_t extent_offset;
  uint64_t total_num_files = 0;
  uint64_t total_num_data_blocks = 0;
  uint64_t total_data_block_size = 0;
  uint64_t total_index_block_size = 0;
  uint64_t total_filter_block_size = 0;
  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--file=", 7) == 0) {
      dir_or_file = argv[i] + 7;
    } else if (strncmp(argv[i], "--extent=", 9) == 0) {
      extent_str = argv[i] + 9;
      std::istringstream iss(extent_str);
      if (iss.fail()) {
        fprintf(stderr, "extent must be numeric");
        exit(1);
      }
      iss >> extent_offset;
      set_extent_offset = true;
    } else if (strcmp(argv[i], "--output_hex") == 0) {
      output_hex = true;
    } else if (strcmp(argv[i], "--input_key_hex") == 0) {
      input_key_hex = true;
    } else if (sscanf(argv[i], "--read_num=%lu%c", (unsigned long*)&n, &junk) ==
               1) {
      read_num = n;
    } else if (strcmp(argv[i], "--verify_checksum") == 0) {
      verify_checksum = true;
    } else if (strncmp(argv[i], "--command=", 10) == 0) {
      command = argv[i] + 10;
    } else if (strncmp(argv[i], "--from=", 7) == 0) {
      from_key = argv[i] + 7;
      has_from = true;
    } else if (strncmp(argv[i], "--to=", 5) == 0) {
      to_key = argv[i] + 5;
      has_to = true;
    } else if (strncmp(argv[i], "--prefix=", 9) == 0) {
      from_key = argv[i] + 9;
      use_from_as_prefix = true;
    } else if (strcmp(argv[i], "--show_properties") == 0) {
      show_properties = true;
    } else if (strcmp(argv[i], "--show_summary") == 0) {
      show_summary = true;
    } else if (strncmp(argv[i], "--set_block_size=", 17) == 0) {
      set_block_size = true;
      block_size_str = argv[i] + 17;
      std::istringstream iss(block_size_str);
      if (iss.fail()) {
        fprintf(stderr, "block size must be numeric");
        exit(1);
      }
      iss >> block_size;
      UNUSED(set_block_size);
    } else if (strncmp(argv[i], "--parse_internal_key=", 21) == 0) {
      std::string in_key(argv[i] + 21);
      try {
        in_key = LDBCommand::HexToString(in_key);
      } catch (...) {
        std::cerr << "ERROR: Invalid key input '" << in_key
                  << "' Use 0x{hex representation of internal smartengine key}"
                  << std::endl;
        return -1;
      }
      Slice sl_key = Slice(in_key);
      ParsedInternalKey ikey;
      int retc = 0;
      if (!ParseInternalKey(sl_key, &ikey)) {
        std::cerr << "Internal Key [" << sl_key.ToString(true /* in hex*/)
                  << "] parse error!\n";
        retc = -1;
      }
      fprintf(stdout, "key=%s\n", ikey.DebugString(true).c_str());
      return retc;
    } else {
      fprintf(stderr, "Unrecognized argument '%s'\n\n", argv[i]);
      print_help();
      exit(1);
    }
  }  //

  if (use_from_as_prefix && has_from) {
    fprintf(stderr, "Cannot specify --prefix and --from\n\n");
    exit(1);
  }

  if (input_key_hex) {
    if (has_from || use_from_as_prefix) {
      from_key = LDBCommand::HexToString(from_key);
    }
    if (has_to) {
      to_key = LDBCommand::HexToString(to_key);
    }
  }

  if (dir_or_file == nullptr) {
    fprintf(stderr, "file or directory must be specified.\n\n");
    print_help();
    exit(1);
  }

  if (!set_extent_offset) {
    fprintf(stderr, "extent must be specified.\n\n");
    print_help();
    exit(1);
  }

  std::vector<std::string>
      filenames;  // if dir_or_file is a dir, filenames stores all files' names
  Env* env = Env::Default();
  Status st = env->GetChildren(dir_or_file, &filenames);
  bool dir = true;  // dir_or_file is a dir
  if (!st.ok()) {
    filenames.clear();
    filenames.push_back(dir_or_file);
    dir = false;  // dir_or_file is a file
  }

  fprintf(stdout, "from [%s] to [%s]\n", Slice(from_key).ToString(true).c_str(),
          Slice(to_key).ToString(true).c_str());

  uint64_t total_read = 0;
  for (size_t i = 0; i < filenames.size(); i++) {
    std::string filename = filenames.at(i);
    if (filename.length() <= 4 ||
        filename.rfind(".sst") != filename.length() - 4) {
      // ignore
      continue;
    }
    // for the next extents of the same logical file if any,
    // assume in the same directory
    std::string dirname;
    if (dir) {
      dirname = std::string(dir_or_file);
      filename = std::string(dir_or_file) + "/" + filename;
    } else {
      size_t found = std::string(dir_or_file).rfind('/');  // the last '/'
      if (found != std::string::npos) {
        dirname = std::string(dir_or_file, found);
      } else {
        char buf[1024];
        ssize_t count =
            readlink("/proc/self/cwd", buf,
                     sizeof(buf));  // read the linkpath of the given path
        dirname = std::string(buf, (count > 0) ? count : 0);
      }
    }

    Options options;
    options.db_paths.emplace_back(dirname,
                                  std::numeric_limits<uint64_t>::max());
    SstFileReader reader(filename, verify_checksum, output_hex, extent_offset,
                         options);
    if (!reader.getStatus().ok()) {  // reader returns some wrong messages
      fprintf(stderr, "%s: %s\n", filename.c_str(),
              reader.getStatus().ToString().c_str());
      continue;
    }

    if (command == "raw") {
      std::string out_filename = filename.substr(0, filename.length() - 4);
      out_filename.append("_" + extent_str);
      out_filename.append("_dump.txt");

      st = reader.DumpTable(out_filename);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(), st.ToString().c_str());
        exit(1);
      } else {
        fprintf(stdout, "raw dump written to file %s\n", &out_filename[0]);
      }
      continue;
    }

    // scan all files in give file path.
    if (command == "" || command == "scan" || command == "check") {
      st = reader.ReadSequential(
          command == "scan", read_num > 0 ? (read_num - total_read) : read_num,
          has_from || use_from_as_prefix, from_key, has_to, to_key,
          use_from_as_prefix);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(), st.ToString().c_str());
      }
      total_read += reader.GetReadNumber();
      if (read_num > 0 && total_read > read_num) {
        break;
      }
    }

    if (show_properties || show_summary) {
      const TableProperties* table_properties;

      std::shared_ptr<const TableProperties> table_properties_from_reader;
      st = reader.ReadTableProperties(&table_properties_from_reader);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(), st.ToString().c_str());
        fprintf(stderr, "Try to use initial table properties\n");
        table_properties = reader.GetInitTableProperties();
      } else {
        table_properties = table_properties_from_reader.get();
      }
      if (table_properties != nullptr) {
        if (show_properties) {
          fprintf(stdout,
                  "Table Properties:\n"
                  "------------------------------\n"
                  "  %s",
                  table_properties->ToString("\n  ", ": ").c_str());
        }
        total_num_files += 1;
        total_num_data_blocks += table_properties->num_data_blocks;
        total_data_block_size += table_properties->data_size;
        total_index_block_size += table_properties->index_size;
        total_filter_block_size += table_properties->filter_size;
      }
    }
  }
  if (show_summary) {
    fprintf(stdout, "total number of files: %" PRIu64 "\n", total_num_files);
    fprintf(stdout, "total number of data blocks: %" PRIu64 "\n",
            total_num_data_blocks);
    fprintf(stdout, "total data block size: %" PRIu64 "\n",
            total_data_block_size);
    fprintf(stdout, "total index block size: %" PRIu64 "\n",
            total_index_block_size);
    fprintf(stdout, "total filter block size: %" PRIu64 "\n",
            total_filter_block_size);
  }
  return 0;
}
}  // namespace tools
}  // namespace smartengine

#endif  // ROCKSDB_LITE
