#ifndef YCSB_C_RESOURCE_SCHEDULER_H_
#define YCSB_C_RESOURCE_SCHEDULER_H_

#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

#include "measurements.h"
#include "utils/countdown_latch.h"
#include "utils/resources.h"
#include "utils/utils.h"

namespace ycsbc {

using ycsbc::utils::MultiTenantResourceOptions;
using ycsbc::utils::MultiTenantResourceUsage;

std::vector<int64_t> ComputePRFAllocation(
  const int64_t resource_capacity, std::vector<int64_t> interval_usage) {

  size_t num_clients = interval_usage.size();
  size_t num_clients_assigned = 0;
  std::vector<bool> assigned(num_clients);
  
  int64_t capacity_remaining = resource_capacity;
  int64_t fair_share;

  std::vector<int64_t> allocation(num_clients);

  // Iteratively grant resources to clients until all capacity is spent.
  while (num_clients_assigned < num_clients) {
    // The fair share for each round is an even split of remaining capacity.
    fair_share = capacity_remaining / (num_clients - num_clients_assigned);
    int assigned_this_round = 0;
    for (size_t i = 0; i < num_clients; ++i) {
      if (!assigned[i] && interval_usage[i] < fair_share) {
        // TODO(tgriggs): Only allow clients to increase by XX% per interval
        allocation[i] = std::max(1024.0 * 1024.0, 2.0 * interval_usage[i]);
        // allocation[i] = std::max(1024.0 * 1024.0, 1.1 * interval_usage[i]);
        // std::cout << "[TGRIGGS_LOG] Setting Client " << (i + 1) << " RateLimit to " << (allocation[i] / 1024 / 1024) << " MB/s\n";

        // TODO(tgriggs): Alternative: allow clients to always use at least fair share.
        // res_opts[i].read_rate_limit = fair_share;

        ++num_clients_assigned;
        ++assigned_this_round;
        assigned[i] = true;

        // TODO(tgriggs): Only count the amount actually used. This over-subscribes, which
        // is helpful for observing when a client wants to increase usage.
        capacity_remaining -= interval_usage[i];
      }
    }
    if (assigned_this_round == 0) {
      // If no clients were assigned this round, then evenly assign remaining capacity.
      std::cout << "[TGRIGGS_LOG] No clients assigned this round. Remaining: " << (num_clients - num_clients_assigned) << ", capacity: " << capacity_remaining << "\n";
      for (size_t i = 0; i < num_clients; ++i) {
        if (assigned[i]) {
          continue;
        }

        // TODO(tgriggs): change this
        allocation[i] = fair_share;
        // std::cout << "[TGRIGGS_LOG] Setting Client " << (i + 1) << " RateLimit to " << (allocation[i] / 1024 / 1024) << " MB/s\n";
        assigned[i] = true;
        ++num_clients_assigned;
      }
    }
  }
  return allocation;
}

void CentralResourceSchedulerThread(
  std::vector<ycsbc::DB *> dbs, ycsbc::Measurements *measurements, 
  std::vector<ycsbc::Measurements*> per_client_measurements, int res_update_interval_s,
  int stats_dump_interval_s, ycsbc::utils::CountDownLatch *latch) {
    // TODO(tgriggs): Wait for DB init before querying for stats. This is a hack.
    std::this_thread::sleep_for(std::chrono::seconds(5));
    
    // TODO(tgriggs): fix this, which pulls from dbs, but if num threads i
    // less, then it breaks
    size_t num_clients = dbs.size();

    // TODO(tgriggs): Pull default values from the config
    // TODO(tgriggs): artificially dropping this to 200MB/s for testing
    const int64_t kIOReadCapBps = 200 * 1024 * 1024;
    const int64_t kIOWriteCapBps = 200 * 1024 * 1024;
    std::vector<MultiTenantResourceOptions> res_opts(num_clients);
    for (auto& res_opt : res_opts) {
      res_opt.read_rate_limit = kIOReadCapBps;
      res_opt.write_rate_limit = kIOWriteCapBps;
      res_opt.write_buffer_size = 128 * 1024 * 1024;
      res_opt.max_write_buffer_number = 4;
    }

    std::vector<MultiTenantResourceUsage> prev_usage(num_clients);
    bool done = false;
    bool first_time = true;

    std::vector<MultiTenantResourceUsage> interval_usage(num_clients);
    while (1) {
      done = latch->AwaitFor(res_update_interval_s);
      if (done) {
        break;
      }
      std::vector<MultiTenantResourceUsage> total_usage = dbs[0]->GetResourceUsage();
      for (size_t i = 0; i < num_clients; ++i) {
        interval_usage[i] = ycsbc::utils::ComputeResourceUsageDiff(prev_usage[i], total_usage[i]);
      }
      prev_usage = total_usage;
      if (first_time) {
        first_time = false;
        continue;
      }
      std::cout << "[TGRIGGS_LOG] Usage in this interval: " << std::endl;
      for (const auto& usage : interval_usage) {
        std::cout << usage.ToString();
      }

      std::vector<int64_t> io_read_usage;
      std::vector<int64_t> io_write_usage;
      for (const auto& usage : interval_usage) {
        io_read_usage.push_back(usage.io_bytes_read);
        io_write_usage.push_back(usage.io_bytes_written);
      }
      std::vector<int64_t> io_read_allocation = ComputePRFAllocation(kIOReadCapBps, io_read_usage);
      std::vector<int64_t> io_write_allocation = ComputePRFAllocation(kIOWriteCapBps, io_write_usage);

      for (size_t i = 0; i < num_clients; ++i) {
        res_opts[i].read_rate_limit = io_read_allocation[i];
        res_opts[i].write_rate_limit = io_write_allocation[i];
      }

      // TODO(tgriggs): Access a single "Resource" object instead of going through a single DB
      dbs[0]->UpdateResourceOptions(res_opts);
    }
}

} // namespace ycsbc
#endif