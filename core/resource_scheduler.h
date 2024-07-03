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

using ycsbc::utils::MultiTenantResourceOptions;
using ycsbc::utils::MultiTenantResourceUsage;

void CentralResourceSchedulerThread(
  std::vector<ycsbc::DB *> dbs, ycsbc::Measurements *measurements, 
  std::vector<ycsbc::Measurements*> per_client_measurements, int res_update_interval_s,
  int stats_dump_interval_s, ycsbc::utils::CountDownLatch *latch) {
    // TODO(tgriggs): Wait for DB init before querying for stats. This is a hack.
    std::this_thread::sleep_for(std::chrono::seconds(5));
    bool done = false;
    bool first_time = true;
    
    // TODO(tgriggs): fix this, which pulls from dbs, but if num threads i
    // less, then it breaks
    // size_t num_clients = 2;
    size_t num_clients = dbs.size();

    std::vector<MultiTenantResourceUsage> prev_usage(num_clients);
    // TODO(tgriggs): artificially dropping this to 200MB/s for testing
    const int64_t kIOReadCapBps = 200 * 1024 * 1024;
    const int64_t kIOWriteCapBps = 400 * 1024 * 1024;

    // TODO(tgriggs): Pull default values from the config
    std::vector<MultiTenantResourceOptions> res_opts(num_clients);
    for (auto& res_opt : res_opts) {
      res_opt.read_rate_limit = kIOReadCapBps;
      res_opt.write_rate_limit = kIOWriteCapBps;
      res_opt.write_buffer_size = 128 * 1024 * 1024;
      res_opt.max_write_buffer_number = 4;
    }

    // time_point<system_clock> start_time = system_clock::now();
    while (1) {
      done = latch->AwaitFor(res_update_interval_s);
      if (done) {
        break;
      }
      std::vector<MultiTenantResourceUsage> total_usage = dbs[0]->GetResourceUsage();
      std::vector<MultiTenantResourceUsage> interval_usage;
      interval_usage.reserve(num_clients);
      for (size_t i = 0; i < num_clients; ++i) {
        interval_usage.push_back(ycsbc::utils::ComputeResourceUsageDiff(prev_usage[i], total_usage[i]));
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

      // IO Read
      // Iteratively assign client limits until all capacity is spent.
      size_t num_clients_assigned = 0;
      std::vector<bool> assigned(num_clients);
      
      int64_t capacity_remaining = kIOReadCapBps;
      int64_t io_read_fair_share;

      while (num_clients_assigned < num_clients) {
        int assigned_this_round = 0;
        io_read_fair_share = capacity_remaining / (num_clients - num_clients_assigned);
        for (size_t i = 0; i < num_clients; ++i) {
          if (!assigned[i] && interval_usage[i].io_bytes_read < io_read_fair_share) {
            // TODO(tgriggs): Only allow clients to increase by XX% per interval
            res_opts[i].read_rate_limit = std::max(1024.0 * 1024.0, 1.1 * interval_usage[i].io_bytes_read);
            // res_opts[i].read_rate_limit = std::max(1024.0 * 1024.0, 10.0 * interval_usage[i].io_bytes_read);
            std::cout << "[TGRIGGS_LOG] Setting Client " << (i + 1) << " RateLimit to " << (res_opts[i].read_rate_limit / 1024 / 1024) << " MB/s\n";

            // TODO(tgriggs): remove this
            res_opts[i].write_rate_limit = res_opts[i].read_rate_limit;

            // TODO(tgriggs): This allows clients to always use at least fair share.
            // res_opts[i].read_rate_limit = io_read_fair_share;

            ++num_clients_assigned;
            ++assigned_this_round;
            assigned[i] = true;
            // TODO(tgriggs): Only count the amount actually used. This over-subscribes.
            capacity_remaining -= interval_usage[i].io_bytes_read;
          }
        }
        if (assigned_this_round == 0) {
          std::cout << "[TGRIGGS_LOG] No clients assigned this round. Remaining: " << (num_clients - num_clients_assigned) << ", capacity: " << capacity_remaining << "\n";
          for (size_t i = 0; i < num_clients; ++i) {
            if (assigned[i]) {
              continue;
            }
            // TODO(tgriggs): change this
            res_opts[i].read_rate_limit = io_read_fair_share;
            res_opts[i].write_rate_limit = res_opts[i].read_rate_limit;
            std::cout << "[TGRIGGS_LOG] Setting Client " << (i + 1) << " RateLimit to " << (res_opts[i].read_rate_limit / 1024 / 1024) << " MB/s\n";
          }
          break;
          // io_read_fair_share = std::numeric_limits<int64_t>::max();
        }
      }

      // TODO(tgriggs): IO Write

      // TODO(tgriggs): Access a single "Resource" object instead of going through a single DB
      // TODO(tgriggs): Also, is there a performance hit to "Init" so many DBs? 
      dbs[0]->UpdateResourceOptions(res_opts);
      // for (size_t i = 0; i < num_clients; ++i) {
      //   dbs[i]->UpdateResourceOptions(i + 1, res_opts[i]);
      // }
    }
}

#endif