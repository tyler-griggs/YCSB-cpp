//
//  basic_db.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_BASIC_DB_H_
#define YCSB_C_BASIC_DB_H_

#include "db.h"
#include "utils/properties.h"
#include "utils/resources.h"

#include <iostream>
#include <string>
#include <memory>
#include <mutex>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
namespace ycsbc {

class BasicDB : public DB {
 public:
  BasicDB() : out_(nullptr) {}

  void Init();

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result,
              int client_id = 0);

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result, int client_id);

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values, 
                int client_id = 0);

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values, int client_id = 0);

  Status Delete(const std::string &table, const std::string &key);

  Status InsertBatch(const std::string &table, int start_key, std::vector<Field> &values, int num_keys, int client_id = 0);

  void UpdateRateLimit(int client_id, int64_t rate_limit_bytes);

  void UpdateMemtableSize(int client_id, int memtable_size_bytes);

  void UpdateResourceOptions(int client_id, ycsbc::utils::MultiTenantResourceOptions res_opts);
  
  void PrintDbStats();

 private:
  static std::mutex mutex_;

  std::ostream *out_;
};

DB *NewBasicDB();

} // ycsbc

#endif // YCSB_C_BASIC_DB_H_

