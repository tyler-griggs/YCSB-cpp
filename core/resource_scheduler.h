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

using ycsbc::utils::MultiTenantResourceShares;
using ycsbc::utils::MultiTenantResourceUsage;

struct ResourceSchedulerOptions {
  int rsched_interval_ms;
  int stats_dump_interval_s;
  size_t lookback_intervals;
  double ramp_up_multiplier; 
  int64_t io_read_capacity_mbps;
  int64_t io_write_capacity_mbps;
  int64_t memtable_capacity_mb; // TODO(tgriggs): decrease this size
  int max_memtable_size_mb;
  int min_memtable_size_mb;
  int min_memtable_count;
};

struct ResourceShareReport {
  ResourceShareReport(long timestamp, uint16_t client_id, MultiTenantResourceShares shares) 
    : timestamp(timestamp), client_id(client_id), shares(shares) {}

  long timestamp;
  uint16_t client_id;
  MultiTenantResourceShares shares;

  std::string ToCSV() const {
    std::ostringstream oss;
    oss << (timestamp) << ","
        << (client_id) << ","
        << shares.ToCSV();
    return oss.str();
  }
};

// TODO(tgriggs): Need to ensure:
//  1) idle clients don't take much (or any?) memtable space, 
//  2) clients are able to quickly gain memtable capacity (e.g., bursty clients)
// `write_buffer_sizes` and `max_write_buffer_numbers` are output parameters.
void MemtableAllocationToRocksDbParams(
    std::vector<int64_t> memtable_allocation, int64_t capacity_mb, 
    int max_memtable_size_mb, int min_memtable_size_mb, int min_memtable_count,
    std::vector<int>& write_buffer_sizes_mb, std::vector<int>& max_write_buffer_numbers) {

  // TODO(tgriggs): Lot of tuning necessary here.

  // for (size_t i = 0; i < memtable_allocation.size(); ++i) {
  //   if (memtable_allocation[i] > max_memtable_size) {
  //     // Get the integer ceiling of "Allocation / MaxMemtableSize" to get number of memtables
  //     max_write_buffer_numbers[i] = std::max(min_memtable_count, int((memtable_allocation[i] + max_memtable_size - 1) / max_memtable_size));
  //     // Divide the total allocation by number of memtables to get memtable size
  //     write_buffer_sizes[i] = memtable_allocation[i] / max_write_buffer_numbers[i];
  //   } else {
  //     write_buffer_sizes[i] = std::max(min_memtable_size, int(memtable_allocation[i]));
  //     max_write_buffer_numbers[i] = min_memtable_count;
  //   }
  //   // else if (memtable_allocation[i] <= min_memtable_size) {
  //   //   write_buffer_sizes[i] = min_memtable_size;
  //   //   max_write_buffer_numbers[i] = min_memtable_count;
  //   // } else {
  //   //   write_buffer_sizes[i] = memtable_allocation[i];
  //   //   max_write_buffer_numbers[i] = min_memtable_count;
  //   // }
  // } 


  // TODO(tgriggs): idea is to always assign all memtable space based on 1) min, 2) ratios 
  for (size_t i = 0; i < memtable_allocation.size(); ++i) {
    write_buffer_sizes_mb[i] = min_memtable_size_mb;
    max_write_buffer_numbers[i] = min_memtable_count;
  } 

  // Change memtable_allocation into proportions
  std::vector<double> proportions(memtable_allocation.size());
  double total_allocation = 0;
  for (size_t i = 0; i < memtable_allocation.size(); ++i) {
    total_allocation += memtable_allocation[i];
  }
  for (size_t i = 0; i < memtable_allocation.size(); ++i) {
    proportions[i] = memtable_allocation[i] / total_allocation;
  }
  int remaining_memtables = (capacity_mb - (memtable_allocation.size() * min_memtable_count * min_memtable_size_mb)) / min_memtable_size_mb;
  for (size_t i = 0; i < memtable_allocation.size(); ++i) {
    max_write_buffer_numbers[i] += int(proportions[i] * remaining_memtables);
  }
}

std::vector<int64_t> ComputePRFAllocation(
  const int64_t resource_capacity, std::vector<int64_t> interval_usage, double ramp_up_multiplier) {

  size_t num_clients = interval_usage.size();
  size_t num_clients_assigned = 0;
  std::vector<bool> assigned(num_clients);
  
  int64_t capacity_remaining = resource_capacity;

  std::vector<int64_t> allocation(num_clients);

  // Idea: Just always give XX% more than usage.
  // Allocate resources from least used to most used. 
  // Keep going until:
    // 1) Remaining usage exceeds remaining capacity --> evenly divide capacity
    // 2) All resources are assigned. 

  // Sort clients by usage.
  std::vector<std::pair<int64_t, int>> usage_by_client;
  for (size_t i = 0; i < num_clients; ++i) {
    usage_by_client.push_back(std::make_pair(interval_usage[i], i));
  }
  std::sort(usage_by_client.begin(), usage_by_client.end());

  // Assign from lowest to highest.
  int64_t fair_share;
  for (size_t i = 0; i < num_clients; ++i) {
    int64_t client_usage = usage_by_client[i].first;
    int client_id = usage_by_client[i].second;
    fair_share = capacity_remaining / (num_clients - num_clients_assigned);

    if (client_usage < fair_share) {
      // TODO(tgriggs): Replace this second value with resource-specific parameters
      allocation[client_id] = std::max(ramp_up_multiplier * client_usage, 10.0);
      capacity_remaining -= client_usage;
      ++num_clients_assigned;
    } else {
      // Evenly divide remaining capacity.
      for (size_t j = i; j < num_clients; ++j) {
        allocation[usage_by_client[j].second] = fair_share;
      }
      // All done!
      break;
    }
  }
  return allocation;
}

void DumpResourceShares(
  std::vector<ResourceShareReport>& share_report_buffer,
  std::ofstream& logfile) {
  for (const auto& report : share_report_buffer) {
    logfile << report.ToCSV() << std::endl;
  }
}

void WriteResourceShareHeader(std::ofstream& logfile) {
  logfile << "timestamp,client_id,write_rate_limit_mbs,read_rate_limit_mbs,write_buffer_size_mb,max_write_buffer_number" << std::endl;
}

void CentralResourceSchedulerThread(
  std::vector<ycsbc::DB *> dbs, ycsbc::Measurements *measurements, 
  std::vector<ycsbc::Measurements*> per_client_measurements, ResourceSchedulerOptions options,
  ycsbc::utils::CountDownLatch *latch) {

    std::string resource_share_filename = "logs/resource_shares.log";
    std::ofstream resource_share_logfile;
    resource_share_logfile.open(resource_share_filename, std::ios::out | std::ios::trunc);
    if (!resource_share_logfile.is_open()) {
      // TODO(tgriggs):  Handle file open failure, propagate exception
      // throw std::ios_base::failure("Failed to open the file.");
    }
    WriteResourceShareHeader(resource_share_logfile);

    // TODO(tgriggs): Wait for DB init before querying for stats. This is a hack.
    std::this_thread::sleep_for(std::chrono::seconds(5));

    size_t buffer_dump_threshold = 10 * 100;
    std::vector<ResourceShareReport> share_report_buffer;
    share_report_buffer.reserve(buffer_dump_threshold);
    
    // TODO(tgriggs): fix this, which pulls from dbs, but if num threads i
    // less, then it breaks
    size_t num_clients = dbs.size();

    std::vector<MultiTenantResourceShares> res_opts(num_clients);
    std::vector<MultiTenantResourceUsage> prev_usage(num_clients);
    bool first_time = true;

    // std::vector<MultiTenantResourceUsage> interval_usage(num_clients);
    std::deque<std::vector<MultiTenantResourceUsage>> interval_usages;

    while (!latch->AwaitForMs(options.rsched_interval_ms)) {
      // TODO(tgriggs): Optimize this path next -- are any locks taken?
      std::vector<MultiTenantResourceUsage> total_usage = dbs[0]->GetResourceUsage();
      std::vector<MultiTenantResourceUsage> interval_usage(num_clients);
      for (size_t i = 0; i < num_clients; ++i) {
        interval_usage[i] = ycsbc::utils::ComputeResourceUsageRateInInterval(prev_usage[i], total_usage[i], options.rsched_interval_ms);
      }

      // Push the current interval usage to the back of the deque
      interval_usages.push_back(interval_usage);
      if (interval_usages.size() > options.lookback_intervals) {
        interval_usages.pop_front();
      }

      prev_usage = total_usage;
      if (first_time) {
        first_time = false;
        continue;
      }
      // std::cout << "[TGRIGGS_LOG] Usage in this interval: " << std::endl;
      // for (const auto& usage : interval_usage) {
      //   std::cout << usage.ToString();
      // }

      // TODO(tgriggs): just update the resource usage struct to vectors so i don't have
      //                have to do this restructuring 
      std::vector<MultiTenantResourceUsage> max_interval_usage(num_clients);
      for (size_t i = 0; i < num_clients; ++i) {
        for (const auto& usage : interval_usages) {
            max_interval_usage[i].io_bytes_read = std::max(max_interval_usage[i].io_bytes_read, usage[i].io_bytes_read);
            max_interval_usage[i].io_bytes_written = std::max(max_interval_usage[i].io_bytes_written, usage[i].io_bytes_written);
            max_interval_usage[i].mem_bytes_written = std::max(max_interval_usage[i].mem_bytes_written, usage[i].mem_bytes_written);
        }
      }
      std::vector<int64_t> io_read_usage;
      std::vector<int64_t> io_write_usage;
      std::vector<int64_t> memtable_usage;
      for (const auto& usage : max_interval_usage) {
        io_read_usage.push_back(usage.io_bytes_read);
        io_write_usage.push_back(usage.io_bytes_written);
        memtable_usage.push_back(usage.mem_bytes_written);
      }
      std::vector<int64_t> io_read_allocation = ComputePRFAllocation(options.io_read_capacity_mbps, io_read_usage, options.ramp_up_multiplier);
      std::vector<int64_t> io_write_allocation = ComputePRFAllocation(options.io_write_capacity_mbps, io_write_usage, options.ramp_up_multiplier);
      std::vector<int64_t> memtable_allocation = ComputePRFAllocation(options.memtable_capacity_mb, memtable_usage, options.ramp_up_multiplier);
      std::vector<int> write_buffer_sizes(num_clients);
      std::vector<int> max_write_buffer_numbers(num_clients);
      MemtableAllocationToRocksDbParams(
        memtable_allocation, options.memtable_capacity_mb, 
        options.max_memtable_size_mb, options.min_memtable_size_mb,
        options.min_memtable_count,
        write_buffer_sizes, max_write_buffer_numbers);

      for (size_t i = 0; i < num_clients; ++i) {
        res_opts[i].max_write_buffer_number = max_write_buffer_numbers[i];
        res_opts[i].write_buffer_size_mb = write_buffer_sizes[i];
        res_opts[i].read_rate_limit_mbs = io_read_allocation[i];
        res_opts[i].write_rate_limit_mbs = io_write_allocation[i];
        if (i == 0) {
          res_opts[i].write_rate_limit_mbs = std::max(res_opts[i].write_rate_limit_mbs, uint32_t(100));
          // res_opts[i].read_rate_limit = 500 / 4;
        }
      }

      // TODO(tgriggs): Access a single "Resource" object instead of going through a single DB
      dbs[0]->UpdateResourceShares(res_opts);

      // TODO(tgriggs): Add new shares to a buffer, then dump at threshold
      auto now_us = std::chrono::time_point_cast<std::chrono::microseconds>( std::chrono::high_resolution_clock::now());
      long now = now_us.time_since_epoch().count();
      for (size_t i = 0; i < res_opts.size(); ++i) {
        share_report_buffer.push_back(ResourceShareReport(now, i, res_opts[i]));
      }
      if (share_report_buffer.size() > buffer_dump_threshold) {
        std::cout << "[TGRIGGS_LOG] Dumping resource shares to file\n\n\n";
        DumpResourceShares(share_report_buffer, resource_share_logfile);
        share_report_buffer.clear();
      }

      // TODO(tgriggs): also optionally record and dump usage
    }
}

} // namespace ycsbc
#endif