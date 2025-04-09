//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "basic_db.h"
#include "core/db_factory.h"

namespace {
  const std::string PROP_SILENT = "basic.silent";
  const std::string PROP_SILENT_DEFAULT = "false";
}

namespace ycsbc {

std::mutex BasicDB:: mutex_;

void BasicDB::Init() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (props_->GetProperty(PROP_SILENT, PROP_SILENT_DEFAULT) == "true") {
    out_ = new std::ofstream;
    out_->setstate(std::ios_base::badbit);
  } else {
    out_ = &std::cout;
  }
}

DB::Status BasicDB::Read(const std::string &table, const std::string &key,
                         const std::vector<std::string> *fields, std::vector<Field> &result,
                         int client_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "READ " << table << ' ' << key;
  if (fields) {
    *out_ << " [ ";
    for (auto f : *fields) {
      *out_ << f << ' ';
    }
    *out_ << ']' << std::endl;
  } else {
    *out_  << " < all fields >" << std::endl;
  }
  return kOK;
}

DB::Status BasicDB::ReadBatch(const std::string &table, const std::vector<std::string> &keys,
                   const std::vector<std::vector<std::string>> *fields,
                   std::vector<std::vector<Field>> &result, int client_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "READ_BATCH " << table << ' ' << keys.size() << std::endl;
  
  for (size_t i = 0; i < keys.size(); i++) {
    *out_ << "  Key: " << keys[i] << std::endl;
    if (fields) {
      *out_ << "    Fields: [ ";
      for (const auto& field : (*fields)[i]) {
        *out_ << field << ' ';
      }
      *out_ << ']' << std::endl;
    } else {
      *out_ << "    Fields: < all fields >" << std::endl;
    }
  }
  return kOK;
}

DB::Status BasicDB::Scan(const std::string &table, const std::string &key, int len,
                         const std::vector<std::string> *fields,
                         std::vector<std::vector<Field>> &result, int client_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "SCAN " << table << ' ' << key << " " << len;
  if (fields) {
    *out_ << " [ ";
    for (auto f : *fields) {
      *out_ << f << ' ';
    }
    *out_ << ']' << std::endl;
  } else {
    *out_  << " < all fields >" << std::endl;
  }
  return kOK;
}

DB::Status BasicDB::Update(const std::string &table, const std::string &key,
                           std::vector<Field> &values, int client_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "UPDATE " << table << ' ' << key << " [ ";
  for (auto v : values) {
    *out_ << v.name << '=' << v.value << ' ';
  }
  *out_ << ']' << std::endl;
  return kOK;
}

DB::Status BasicDB::Insert(const std::string &table, const std::string &key,
                           std::vector<Field> &values, int client_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "INSERT " << table << ' ' << key << " [ ";
  for (auto v : values) {
    *out_ << v.name << '=' << v.value << ' ';
  }
  *out_ << ']' << std::endl;
  return kOK;
}

DB::Status BasicDB::Delete(const std::string &table, const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "DELETE " << table << ' ' << key << std::endl;
  return kOK;
}

DB::Status BasicDB::InsertBatch(const std::string &table, int start_key, std::vector<Field> &values, int num_keys, int client_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "INSERT_BATCH " << table << ' ' << start_key << std::endl;
  return kOK;
}

DB::Status BasicDB::ReadModifyInsertBatch(const std::string &table,
                             const std::vector<std::string> &keys,
                             const std::vector<std::vector<std::string>> *fields,
                             std::vector<std::vector<Field>> &result,
                             std::vector<Field> &new_values, int client_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "READ_MODIFY_INSERT_BATCH " << table << ' ' << keys.size() << std::endl;
  return kOK;
}

void BasicDB::UpdateRateLimit(int client_id, int64_t rate_limit_bytes) {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "UPDATE_RATE_LIMIT " << client_id << ' ' << rate_limit_bytes << std::endl;
}

void BasicDB::UpdateMemtableSize(int client_id, int memtable_size_bytes) {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "UPDATE_MEMTABLE_SIZE " << client_id << ' ' << memtable_size_bytes << std::endl;
}


void BasicDB::UpdateResourceShares(std::vector<ycsbc::utils::MultiTenantResourceShares> res_opts) {
  std::lock_guard<std::mutex> lock(mutex_);
  (void) res_opts;
  *out_ << "UPDATE_RESOURCE_SHARES " << std::endl;
}

std::vector<ycsbc::utils::MultiTenantResourceUsage> BasicDB::GetResourceUsage() {
  std::lock_guard<std::mutex> lock(mutex_);
  *out_ << "GET_RESOURCE_USAGE " << std::endl;
  return {};
}

void BasicDB::PrintDbStats() {
  return;
}

DB *NewBasicDB() {
  return new BasicDB;
}

const bool registered = DBFactory::RegisterDB("basic", NewBasicDB);

} // ycsbc
