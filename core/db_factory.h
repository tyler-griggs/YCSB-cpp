//
//  db_factory.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_DB_FACTORY_H_
#define YCSB_C_DB_FACTORY_H_

#include "db.h"
#include "measurements.h"
#include "utils/properties.h"
#include "utils/resources.h"

#include <string>
#include <map>

namespace ycsbc {

class DBFactory {
 public:
  using DBCreator = DB *(*)();
  static bool RegisterDB(std::string db_name, DBCreator db_creator);
  static DB *CreateDB(utils::Properties *props, Measurements *measurements);
  static DB *CreateDBWithPerClientStats(utils::Properties *props, Measurements *measurements, 
                                        std::vector<Measurements*> per_client_measurements,
                                        std::shared_ptr<ycsbc::utils::MultiTenantCounter> per_client_bytes_written);
 private:
  static std::map<std::string, DBCreator> &Registry();
};

} // ycsbc

#endif // YCSB_C_DB_FACTORY_H_

