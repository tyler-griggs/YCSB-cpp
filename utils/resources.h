#ifndef YCSB_C_RESOURCES_H_
#define YCSB_C_RESOURCES_H_

#include <sstream>
#include <cstdint>

namespace ycsbc::utils {

struct MultiTenantResourceOptions {
  int64_t write_rate_limit;
  int64_t read_rate_limit;
  int write_buffer_size;
  int max_write_buffer_number;
};

struct MultiTenantResourceUsage {
  int64_t io_bytes_read;
  int64_t io_bytes_written;
  int64_t mem_bytes_written;

  std::string ToString() const {
        std::ostringstream oss;
        oss << "IO Read: " << (io_bytes_read / 1024 / 1024) << " MB\n"
            << "IO Written: " << (io_bytes_written / 1024 / 1024) << " MB\n"
            << "Memory Written: " << mem_bytes_written << " MB\n";
        return oss.str();
    }
};

inline MultiTenantResourceUsage ComputeResourceUsageDiff(MultiTenantResourceUsage prev, MultiTenantResourceUsage cur) {
  MultiTenantResourceUsage diff;
  diff.io_bytes_read = cur.io_bytes_read - prev.io_bytes_read;
  diff.io_bytes_written = cur.io_bytes_written - prev.io_bytes_written;
  diff.mem_bytes_written = cur.mem_bytes_written - prev.mem_bytes_written;
  return diff;
}

}
#endif