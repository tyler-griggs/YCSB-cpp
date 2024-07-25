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

struct MultiTenantResourceShares {
  uint32_t write_rate_limit_kbs;
  uint32_t read_rate_limit_kbs;
  uint16_t write_buffer_size_kb;
  int max_write_buffer_number;

  std::string ToString() const {
    std::ostringstream oss;
    oss << "Resource Shares: " << (write_rate_limit_kbs/1024) << " MB/s / "
        << (read_rate_limit_kbs/1024) << " MB/s / "
        << (write_buffer_size_kb/1024) << " MB / "
        << (max_write_buffer_number) << " (Write IO / Read IO / Memtable Size / Memtable Count)\n";
    return oss.str();
  }

  std::string ToCSV() const {
    std::ostringstream oss;
    oss << (write_rate_limit_kbs) << ","
        << (read_rate_limit_kbs) << ","
        << (write_rate_limit_kbs) << ","
        << (max_write_buffer_number);
    return oss.str();
  }
};

struct MultiTenantResourceUsage {
  int64_t io_bytes_written_kb;
  int64_t io_bytes_read_kb;
  int64_t mem_bytes_written_kb;

  std::string ToString() const {
        std::ostringstream oss;
        oss << (io_bytes_written_kb / 1024) << " MB / "
            << (io_bytes_read_kb / 1024) << " MB / "
             << (mem_bytes_written_kb / 1024) << " MB (IO write / IO read / Mem write)\n";
        return oss.str();
    }

  std::string ToCSV() const {
    std::ostringstream oss;
    oss << (io_bytes_written_kb) << ","
        << (io_bytes_read_kb) << ","
        << (mem_bytes_written_kb);
    return oss.str();
  }
};

inline MultiTenantResourceUsage ComputeResourceUsageRateInInterval(
  MultiTenantResourceUsage prev, MultiTenantResourceUsage cur, int interval_ms) {
  MultiTenantResourceUsage diff;
  double interval_s = interval_ms / 1000.0;
  diff.io_bytes_written_kb = (cur.io_bytes_written_kb - prev.io_bytes_written_kb) / interval_s;
  diff.io_bytes_read_kb = (cur.io_bytes_read_kb - prev.io_bytes_read_kb) / interval_s;
  diff.mem_bytes_written_kb = (cur.mem_bytes_written_kb - prev.mem_bytes_written_kb ) / interval_s;
  return diff;
}

}
#endif