//
//  core_workload.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include "uniform_generator.h"
#include "zipfian_generator.h"
#include "scrambled_zipfian_generator.h"
#include "skewed_latest_generator.h"
#include "const_generator.h"
#include "core_workload.h"
#include "random_byte_generator.h"
#include "utils/utils.h"

#include <algorithm>
#include <random>
#include <string>
#include <iostream>
#include <json/json.h>
using std::string;
using ycsbc::CoreWorkload;

const char *ycsbc::kOperationString[ycsbc::MAXOPTYPE] = {
    "INSERT",
    "READ",
    "UPDATE",
    "SCAN",
    "READMODIFYWRITE",
    "DELETE",
    "RANDOM_INSERT",
    "INSERT_BATCH",
    "QUEUE",
    "INSERT-FAILED",
    "READ-FAILED",
    "UPDATE-FAILED",
    "SCAN-FAILED",
    "READMODIFYWRITE-FAILED",
    "DELETE-FAILED",
    "INSERT_BATCH-FAILED"};

const string CoreWorkload::TABLENAME_PROPERTY = "table";
const string CoreWorkload::TABLENAME_DEFAULT = "usertable";

const string CoreWorkload::FIELD_COUNT_PROPERTY = "fieldcount";
const string CoreWorkload::FIELD_COUNT_DEFAULT = "10";

const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_PROPERTY = "field_len_dist";
const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_DEFAULT = "constant";

const string CoreWorkload::FIELD_LENGTH_PROPERTY = "fieldlength";
const string CoreWorkload::FIELD_LENGTH_DEFAULT = "100";

const string CoreWorkload::READ_ALL_FIELDS_PROPERTY = "readallfields";
const string CoreWorkload::READ_ALL_FIELDS_DEFAULT = "true";

const string CoreWorkload::WRITE_ALL_FIELDS_PROPERTY = "writeallfields";
const string CoreWorkload::WRITE_ALL_FIELDS_DEFAULT = "false";

const string CoreWorkload::READ_PROPORTION_PROPERTY = "readproportion";
const string CoreWorkload::READ_PROPORTION_DEFAULT = "0.95";

const string CoreWorkload::UPDATE_PROPORTION_PROPERTY = "updateproportion";
const string CoreWorkload::UPDATE_PROPORTION_DEFAULT = "0.05";

const string CoreWorkload::INSERT_PROPORTION_PROPERTY = "insertproportion";
const string CoreWorkload::INSERT_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::SCAN_PROPORTION_PROPERTY = "scanproportion";
const string CoreWorkload::SCAN_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";
const string CoreWorkload::READMODIFYWRITE_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::RANDOM_INSERT_PROPORTION_PROPERTY = "randominsertproportion";
const string CoreWorkload::RANDOM_INSERT_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::INSERT_BATCH_PROPORTION_PROPERTY = "insertbatchproportion";
const string CoreWorkload::INSERT_BATCH_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";
const string CoreWorkload::REQUEST_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::ZERO_PADDING_PROPERTY = "zeropadding";
const string CoreWorkload::ZERO_PADDING_DEFAULT = "1";

const string CoreWorkload::MIN_SCAN_LENGTH_PROPERTY = "minscanlength";
const string CoreWorkload::MIN_SCAN_LENGTH_DEFAULT = "1";

const string CoreWorkload::MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
const string CoreWorkload::MAX_SCAN_LENGTH_DEFAULT = "1000";

const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";
const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::INSERT_ORDER_PROPERTY = "insertorder";
const string CoreWorkload::INSERT_ORDER_DEFAULT = "ordered";

const string CoreWorkload::INSERT_START_PROPERTY = "insertstart";
const string CoreWorkload::INSERT_START_DEFAULT = "0";

const string CoreWorkload::RECORD_COUNT_PROPERTY = "recordcount";
const string CoreWorkload::OPERATION_COUNT_PROPERTY = "operationcount";

const std::string CoreWorkload::FIELD_NAME_PREFIX = "fieldnameprefix";
const std::string CoreWorkload::FIELD_NAME_PREFIX_DEFAULT = "field";

const std::string CoreWorkload::ZIPFIAN_CONST_PROPERTY = "zipfian_const";

const std::string CoreWorkload::OP_MODE_PROPERTY = "real_op_mode";
const std::string CoreWorkload::OP_MODE_DEFAULT = "true";

namespace ycsbc
{

  std::vector<std::string> Prop2vector(const utils::Properties &props, const std::string &prop, const std::string &default_val)
  {
    std::string vals = props.GetProperty(prop, default_val);
    std::vector<std::string> output;
    std::string val;

    std::istringstream stream(vals);
    while (std::getline(stream, val, ','))
    {
      output.push_back(val);
    }
    return output;
  }

  void CoreWorkload::Init(const utils::Properties &p)
  {
    table_name_ = p.GetProperty(TABLENAME_PROPERTY, TABLENAME_DEFAULT);

    field_count_ = std::stoi(p.GetProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_DEFAULT));
    field_prefix_ = p.GetProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);
    field_len_generator_ = GetFieldLenGenerator(p);

    std::string request_dist = p.GetProperty(REQUEST_DISTRIBUTION_PROPERTY,
                                             REQUEST_DISTRIBUTION_DEFAULT);
    int min_scan_len = std::stoi(p.GetProperty(MIN_SCAN_LENGTH_PROPERTY, MIN_SCAN_LENGTH_DEFAULT));
    int max_scan_len = std::stoi(p.GetProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_DEFAULT));
    std::string scan_len_dist = p.GetProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,
                                              SCAN_LENGTH_DISTRIBUTION_DEFAULT);

    zero_padding_ = std::stoi(p.GetProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_DEFAULT));

    read_all_fields_ = utils::StrToBool(p.GetProperty(READ_ALL_FIELDS_PROPERTY,
                                                      READ_ALL_FIELDS_DEFAULT));
    write_all_fields_ = utils::StrToBool(p.GetProperty(WRITE_ALL_FIELDS_PROPERTY,
                                                       WRITE_ALL_FIELDS_DEFAULT));

    // if (p.GetProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_DEFAULT) == "hashed") {
    //   ordered_inserts_ = false;
    // } else {
    //   ordered_inserts_ = true;
    // }

    // No hashing.
    ordered_inserts_ = true;

    field_chooser_ = new UniformGenerator(0, field_count_ - 1);

    if (scan_len_dist == "uniform")
    {
      scan_len_chooser_ = new UniformGenerator(min_scan_len, max_scan_len);
    }
    else if (scan_len_dist == "zipfian")
    {
      scan_len_chooser_ = new ZipfianGenerator(min_scan_len, max_scan_len);
    }
    else
    {
      throw utils::Exception("Distribution not allowed for scan length: " + scan_len_dist);
    }
  }

  ycsbc::Generator<uint64_t> *CoreWorkload::GetFieldLenGenerator(
      const utils::Properties &p)
  {
    string field_len_dist = p.GetProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
                                          FIELD_LENGTH_DISTRIBUTION_DEFAULT);
    int field_len = std::stoi(p.GetProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_DEFAULT));
    if (field_len_dist == "constant")
    {
      return new ConstGenerator(field_len);
    }
    else if (field_len_dist == "uniform")
    {
      return new UniformGenerator(1, field_len);
    }
    else if (field_len_dist == "zipfian")
    {
      return new ZipfianGenerator(1, field_len);
    }
    else
    {
      throw utils::Exception("Unknown field length distribution: " + field_len_dist);
    }
  }

  std::string CoreWorkload::BuildKeyName(uint64_t key_num)
  {
    // if (!ordered_inserts_) {
    //   key_num = utils::Hash(key_num);
    // }
    std::string prekey = "user";
    std::string value = std::to_string(key_num);
    // int fill = std::max(0, zero_padding_ - static_cast<int>(value.size()));
    // return prekey.append(fill, '0').append(value);
    return prekey.append(value);
  }

  void CoreWorkload::BuildValues(std::vector<ycsbc::DB::Field> &values)
  {
    for (int i = 0; i < field_count_; ++i)
    {
      values.push_back(DB::Field());
      ycsbc::DB::Field &field = values.back();
      field.name.append(field_prefix_).append(std::to_string(i));
      uint64_t len = field_len_generator_->Next();
      field.value.reserve(len);
      RandomByteGenerator byte_generator;
      std::generate_n(std::back_inserter(field.value), len, [&]()
                      { return byte_generator.Next(); });
    }
  }

  void CoreWorkload::BuildSingleValue(std::vector<ycsbc::DB::Field> &values)
  {
    values.push_back(DB::Field());
    ycsbc::DB::Field &field = values.back();
    field.name.append(NextFieldName());
    uint64_t len = field_len_generator_->Next();
    field.value.reserve(len);
    RandomByteGenerator byte_generator;
    std::generate_n(std::back_inserter(field.value), len, [&]()
                    { return byte_generator.Next(); });
  }

  uint64_t CoreWorkload::NextTransactionKeyNum(ClientConfig *config)
  {
    uint64_t key_num;
    do
    {
      key_num = config->key_chooser_->Next();
    } while (key_num > config->transaction_insert_key_sequence_->Last());
    return key_num;
  }

  std::string CoreWorkload::NextFieldName()
  {
    return std::string(field_prefix_).append(std::to_string(field_chooser_->Next()));
  }

  bool CoreWorkload::DoInsert(DB &db, ClientConfig *config)
  {
    const std::string key = BuildKeyName(config->insert_key_sequence_->Next());
    std::vector<DB::Field> fields;
    BuildValues(fields);
    return db.Insert(table_name_, key, fields) == DB::kOK;
  }

  bool CoreWorkload::DoTransaction(DB &db, ClientConfig *config)
  {
    DB::Status status;
    auto op_choice = config->op_chooser_->Next();
    switch (op_choice)
    {
    case READ:
      status = TransactionRead(db, config);
      break;
    case UPDATE:
      status = TransactionUpdate(db, config);
      break;
    case INSERT:
      status = TransactionInsert(db, config);
      break;
    case SCAN:
      status = TransactionScan(db, config);
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite(db, config);
      break;
    case RANDOM_INSERT:
      status = TransactionRandomInsert(db, config);
      break;
    case INSERT_BATCH:
      status = TransactionInsertBatch(db, config);
      break;
    default:
      std::cout << "[TGRIGGS_LOG] Unknown op: " << op_choice << std::endl;
      throw utils::Exception("Operation request is not recognized!");
    }

    return (status == DB::kOK);
  }

  DB::Status CoreWorkload::TransactionRead(DB &db, ClientConfig *config)
  {
    uint64_t key_num = NextTransactionKeyNum(config);
    std::string table_name = config->cf;
    int client_id = config->client_id;
    uint64_t client_key_num = key_num;
    // uint64_t client_key_num = key_num + (client_id%2) * (6250000 / 4);

    const std::string key = BuildKeyName(client_key_num);
    std::vector<DB::Field> result;
    if (!read_all_fields())
    {
      std::vector<std::string> fields;
      fields.push_back(NextFieldName());
      return db.Read(table_name, key, &fields, result, client_id);
    }
    else
    {
      return db.Read(table_name, key, NULL, result, client_id);
    }
  }

  DB::Status CoreWorkload::TransactionReadModifyWrite(DB &db, ClientConfig *config)
  {
    uint64_t key_num = NextTransactionKeyNum(config);
    const std::string key = BuildKeyName(key_num);
    std::vector<DB::Field> result;

    if (!read_all_fields())
    {
      std::vector<std::string> fields;
      fields.push_back(NextFieldName());
      db.Read(table_name_, key, &fields, result);
    }
    else
    {
      db.Read(table_name_, key, NULL, result);
    }

    std::vector<DB::Field> values;
    if (write_all_fields())
    {
      BuildValues(values);
    }
    else
    {
      BuildSingleValue(values);
    }
    return db.Update(table_name_, key, values);
  }

  DB::Status CoreWorkload::TransactionScan(DB &db, ClientConfig *config)
  {
    uint64_t key_num = NextTransactionKeyNum(config);
    std::string table_name = config->cf;
    int client_id = config->client_id;
    int len = 100;
    uint64_t client_key_num = std::min(key_num, uint64_t(3125000 - len));
    // uint64_t client_key_num = key_num + (client_id%2) * (6250000 / 4);

    const std::string key = BuildKeyName(client_key_num);
    // int len = scan_len_chooser_->Next();
    std::vector<std::vector<DB::Field>> result;
    if (!read_all_fields())
    {
      std::vector<std::string> fields;
      fields.push_back(NextFieldName());
      return db.Scan(table_name, key, len, &fields, result, client_id);
    }
    else
    {
      return db.Scan(table_name, key, len, NULL, result, client_id);
    }
  }

  DB::Status CoreWorkload::TransactionUpdate(DB &db, ClientConfig *config)
  {
    uint64_t key_num = NextTransactionKeyNum(config);
    std::string table_name = config->cf;
    int client_id = config->client_id;
    uint64_t client_key_num = key_num;
    // uint64_t client_key_num = key_num + (client_id%2) * (6250000 / 4);

    const std::string key = BuildKeyName(client_key_num);
    std::vector<DB::Field> values;
    if (write_all_fields())
    {
      BuildValues(values);
    }
    else
    {
      BuildSingleValue(values);
    }
    return db.Update(table_name, key, values, client_id);
  }

  DB::Status CoreWorkload::TransactionRandomInsert(DB &db, ClientConfig *config)
  {
    uint64_t key_num = NextTransactionKeyNum(config);
    std::string table_name = config->cf;
    int client_id = config->client_id;
    uint64_t client_key_num = key_num;
    // uint64_t client_key_num = key_num + (client_id%2) * (6250000 / 4);

    const std::string key = BuildKeyName(client_key_num);
    std::vector<DB::Field> values;
    BuildValues(values);
    return db.Insert(table_name, key, values, client_id);
  }

  DB::Status CoreWorkload::TransactionInsertBatch(DB &db, ClientConfig *config)
  {
    uint64_t key_num = NextTransactionKeyNum(config);
    std::string table_name = config->cf;
    int client_id = config->client_id;
    // int batch_size = 86;  // 86 * 1KB == 86KB
    // int batch_size = 200;  // 200 * 1KB == 200KB
    int batch_size = 20000;  // 20000 * 1KB == 20MB per req
    // int batch_size = 86'000;
    uint64_t client_key_num = key_num;
    client_key_num = std::min(key_num, uint64_t(3125000 - batch_size));

    // const std::string key = BuildKeyName(client_key_num);
    std::vector<DB::Field> values;
    BuildValues(values);
    return db.InsertBatch(table_name, client_key_num, values, batch_size, client_id);
  }

  DB::Status CoreWorkload::TransactionInsert(DB &db, ClientConfig *config)
  {
    uint64_t key_num = config->transaction_insert_key_sequence_->Next();
    const std::string key = BuildKeyName(key_num);
    std::vector<DB::Field> values;
    BuildValues(values);
    DB::Status s = db.Insert(table_name_, key, values);
    config->transaction_insert_key_sequence_->Acknowledge(key_num);
    return s;
  }

} // ycsbc
