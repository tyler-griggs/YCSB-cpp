#ifndef YCSB_C_RESOURCES_H_
#define YCSB_C_RESOURCES_H_

#include <cstdint>

namespace ycsbc::utils {

struct MultiTenantResourceOptions {
  int64_t write_rate_limit;
  int64_t read_rate_limit;
  int write_buffer_size;
  int max_write_buffer_number;
};

}
#endif