#ifndef YCSB_C_RESOURCES_H_
#define YCSB_C_RESOURCES_H_

#include <atomic>
#include <sstream>
#include <cstdint>

namespace ycsbc::utils {

class MultiTenantCounter {
  public:
    MultiTenantCounter(size_t size) : counts(size) {
      for (size_t i = 0; i < size; ++i) {
        counts[i] = 0;
      }
    }

    void update(size_t index, int64_t value) {
      counts[index].fetch_add(value, std::memory_order_relaxed);
    }

    int64_t get_value(size_t index) {
      return counts[index].load(std::memory_order_relaxed);
    }

  private:
    std::vector<std::atomic<int64_t>> counts;
};

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
        oss << (io_bytes_read / 1024 / 1024) << " MB / "
            << (io_bytes_written / 1024 / 1024) << " MB / "
             << (mem_bytes_written / 1024 / 1024) << " MB (IO read / IO write / Mem write)\n";
        return oss.str();
    }
};

inline MultiTenantResourceUsage ComputeResourceUsageRateInInterval(
  MultiTenantResourceUsage prev, MultiTenantResourceUsage cur, int interval_ms) {
  MultiTenantResourceUsage diff;
  double interval_s = interval_ms / 1000.0;
  diff.io_bytes_read = (cur.io_bytes_read - prev.io_bytes_read) / interval_s;
  diff.io_bytes_written = (cur.io_bytes_written - prev.io_bytes_written) / interval_s;
  diff.mem_bytes_written = (cur.mem_bytes_written - prev.mem_bytes_written ) / interval_s;
  return diff;
}

}
#endif