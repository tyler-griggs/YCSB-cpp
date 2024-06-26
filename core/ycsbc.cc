//
//  ycsbc.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <ctime>

#include <string>
#include <iostream>
#include <vector>
#include <thread>
#include <future>
#include <chrono>
#include <iomanip>
#include "threadpool.h"

#include "client.h"
#include "core_workload.h"
#include "db_factory.h"
#include "fair_scheduler.h"
#include "measurements.h"
#include "threadpool.h"
#include "utils/countdown_latch.h"
#include "utils/rate_limit.h"
#include "utils/resources.h"
#include "utils/timer.h"
#include "utils/utils.h"

// #ifdef HDRMEASUREMENT
#include <hdr/hdr_histogram.h>
// #endif

using namespace std::chrono;
using ycsbc::utils::MultiTenantResourceOptions;
using ycsbc::utils::MultiTenantResourceUsage;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
void ParseCommandLine(int argc, const char *argv[], ycsbc::utils::Properties &props);

void ResourceSchedulerThread(
  std::vector<ycsbc::DB *> dbs, ycsbc::Measurements *measurements, 
  std::vector<ycsbc::Measurements*> per_client_measurements, int res_update_interval_s,
  int stats_dump_interval_s, ycsbc::utils::CountDownLatch *latch) {
    bool done = false;
    // MultiTenantResourceOptions res_opts;
    std::vector<MultiTenantResourceUsage> prev_usage(dbs.size());

    // time_point<system_clock> start_time = system_clock::now();
    while (1) {
      done = latch->AwaitFor(res_update_interval_s);
      if (done) {
        break;
      }
      std::vector<MultiTenantResourceUsage> total_usage = dbs[0]->GetResourceUsage();
      std::vector<MultiTenantResourceUsage> interval_usage;
      interval_usage.reserve(dbs.size());
      for (size_t i = 0; i < total_usage.size(); ++i) {
        interval_usage.push_back(ycsbc::utils::ComputeResourceUsageDiff(prev_usage[i], total_usage[i]));
      }
      prev_usage = total_usage;
      std::cout << "[TGRIGGS_LOG] Usage in this interval: " << std::endl;
      for (const auto& usage : interval_usage) {
        std::cout << usage.ToString();
      }
    }







    // std::vector<std::tuple<int, int, hdr_histogram>> historical_stats;
      // // Scrape per-client per-op stats.
      // for (size_t i = 0; i < per_client_measurements.size(); ++i) {
      //   auto* hdrMeasurement = dynamic_cast<ycsbc::HdrHistogramMeasurements*>(per_client_measurements[i]);
      //   for (int j = 0; j < ycsbc::MAXOPTYPE; ++j) {
      //     const hdr_histogram* op_stats = hdrMeasurement->GetHistogram(j);
      //     if (op_stats->total_count > 0) {
      //       // Record stats (for later dumping).
      //       historical_stats.push_back(std::tuple(i, j, *op_stats));
      //     }
      //   }
      //   // Reset this client's stats.
      //   per_client_measurements[i]->Reset();
      // }

      // TODO(tgriggs): Scrape flush and compaction stats.

      // Compute usage stats.

      // If dump interval:
        // Print total counts
        // Write stats to file


  // bool done = false;
  // while (1) {
  //   time_point<system_clock> now = system_clock::now();
  //   std::time_t now_c = system_clock::to_time_t(now);
  //   duration<double> elapsed_time = now - start;

  //   std::cout << std::put_time(std::localtime(&now_c), "%F %T") << ' '
  //             << static_cast<long long>(elapsed_time.count()) << " sec: ";

  //   std::cout << measurements->GetStatusMsg() << std::endl;
  //   for (size_t i = 0; i < per_client_measurements.size(); ++i) {
  //     std::cout << "client" << i << " stats:\n";
  //     std::cout << std::put_time(std::localtime(&now_c), "%F %T") << ' ' << per_client_measurements[i]->GetStatusMsg() << std::endl;
  //     per_client_measurements[i]->Reset();
  //   }
  //   // Print DB-wide and CF-wide stats -- only need to use a single client
  //   // std::cout << "DB stats:\n";
  //   // dbs[0]->PrintDbStats();

  //   // for (size_t i = 0; i < dbs.size(); ++i) {
  //   //   // dbs[i]->GetCFMemTableStats();

  //   //   // if (i == 0) {
  //   //   //   continue;
  //   //   // }
  //   //   // std::cout << "db_get: client" << i << ": ";
  //   //   std::cout << "DB " << i << " stats:\n";
  //   //   dbs[i]->PrintDbStats();
  //   // }

  //   if (done) {
  //     break;
  //   }
  //   done = latch->AwaitFor(interval);
  // };
}



// void ResourceSchedulerThread(std::vector<ycsbc::DB *> dbs, ycsbc::utils::CountDownLatch *latch) {
//   int interval = 1;
//   bool done = false;
//   int interval_count = 0;

//   ycsbc::utils::MultiTenantResourceOptions res_opts;

//   while (1) {
//     done = latch->AwaitFor(interval);

//     // TODO(tgriggs): Get previous 1s resource usage
//     // Read IO - per-client reads, compaction reads
//     // Write IO - compaction writes
//     // Memtable - per-client writes (how to compute the cap?)

//     // TODO(tgriggs): compute per-tenant resource utilization (assume some cap)

//     // TODO(tgriggs): compute DRF-based allocations

//     // TODO(tgriggs): update resource limits for memtable and IO schedulers

//     int64_t rate_limit_mbs = 50 + interval_count * 50;
//     int64_t memtable_size_mb = (16 * (interval_count + 1));
//     for (size_t i = 0; i < dbs.size(); ++i) {
//       std::cout << "[TGRIGGS_LOG] Setting Client " << (i + 1) << " RateLimit to " << rate_limit_mbs << " MB/s\n";
//       dbs[i]->UpdateRateLimit(i + 1, rate_limit_mbs * 1024 * 1024);
//       std::cout << "[TGRIGGS_LOG] Setting Client " << (i + 1) << " MemtableSize to " << memtable_size_mb << " MB\n";
//       dbs[i]->UpdateMemtableSize(i + 1, memtable_size_mb * 1024 * 1024);
//       // res_opts.read_rate_limit = rate_limit_mbs * 1024 * 1024;
//       // res_opts.write_rate_limit = rate_limit_mbs * 1024 * 1024;
//       // res_opts.write_buffer_size = memtable_size_mb * 1024 * 1024;
//       // res_opts.max_write_buffer_number = 2;
//       // dbs[i]->UpdateResourceOptions(i + 1, res_opts);
//     }
//     if (done) {
//       break;
//     }
//     interval_count++;
//   }
// }

void StatusThread(ycsbc::Measurements *measurements, std::vector<ycsbc::Measurements*> per_client_measurements, 
                  ycsbc::utils::CountDownLatch *latch, int interval, std::vector<ycsbc::DB *> dbs) {
  using namespace std::chrono;
  time_point<system_clock> start = system_clock::now();
  bool done = false;
  while (1) {
    time_point<system_clock> now = system_clock::now();
    std::time_t now_c = system_clock::to_time_t(now);
    duration<double> elapsed_time = now - start;

    std::cout << std::put_time(std::localtime(&now_c), "%F %T") << ' '
              << static_cast<long long>(elapsed_time.count()) << " sec: ";

    std::cout << measurements->GetStatusMsg() << std::endl;
    for (size_t i = 0; i < per_client_measurements.size(); ++i) {
      std::cout << "client" << i << " stats:\n";
      std::cout << std::put_time(std::localtime(&now_c), "%F %T") << ' ' << per_client_measurements[i]->GetStatusMsg() << std::endl;
      per_client_measurements[i]->Reset();
    }
    // Print DB-wide and CF-wide stats -- only need to use a single client
    // std::cout << "DB stats:\n";
    // dbs[0]->PrintDbStats();

    // for (size_t i = 0; i < dbs.size(); ++i) {
    //   // dbs[i]->GetCFMemTableStats();

    //   // if (i == 0) {
    //   //   continue;
    //   // }
    //   // std::cout << "db_get: client" << i << ": ";
    //   std::cout << "DB " << i << " stats:\n";
    //   dbs[i]->PrintDbStats();
    // }

    if (done) {
      break;
    }
    done = latch->AwaitFor(interval);
  };
}

void RateLimitThread(std::string rate_file, std::vector<ycsbc::utils::RateLimiter *> rate_limiters,
                     ycsbc::utils::CountDownLatch *latch) {
  std::ifstream ifs;
  ifs.open(rate_file);

  if (!ifs.is_open()) {
    ycsbc::utils::Exception("failed to open: " + rate_file);
  }

  int64_t num_threads = rate_limiters.size();

  int64_t last_time = 0;
  while (!ifs.eof()) {
    int64_t next_time;
    int64_t next_rate;
    ifs >> next_time >> next_rate;

    if (next_time <= last_time) {
      ycsbc::utils::Exception("invalid rate file");
    }

    bool done = latch->AwaitFor(next_time - last_time);
    if (done) {
      break;
    }
    last_time = next_time;

    for (auto x : rate_limiters) {
      x->SetRate(next_rate / num_threads);
    }
  }
}

std::vector<int> stringToIntVector(const std::string& input) {
  std::vector<int> result;
  std::stringstream ss(input);
  std::string item;
  while (std::getline(ss, item, ',')) {
    result.push_back(std::stoi(item));
  }
  return result;
}

void writeToCSV(const std::string& filename, const std::vector<std::tuple<long long, std::vector<int>>>& data) {
    std::ofstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Failed to open file for writing.\n";
        return;
    }
    for (const auto& row : data) {
      auto& timestamp = std::get<0>(row);
      auto& ints = std::get<1>(row);
      // auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(timestamp.time_since_epoch()).count();
      file << timestamp << ",";
      for (size_t i = 0; i < ints.size(); ++i) {
        file << ints[i];
        if (i < ints.size() - 1) {
          file << ",";
        }
      }
      file << "\n";
    }
    file.close();
    std::cout << "Data written to " << filename << std::endl;
}

int main(const int argc, const char *argv[]) {
  ycsbc::utils::Properties props;
  ParseCommandLine(argc, argv, props);

  const bool do_load = (props.GetProperty("doload", "false") == "true");
  const bool do_transaction = (props.GetProperty("dotransaction", "false") == "true");
  if (!do_load && !do_transaction) {
    std::cerr << "No operation to do" << std::endl;
    exit(1);
  }

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));
  std::vector<int> target_rates = stringToIntVector(props.GetProperty("target_rates", "0"));

  ycsbc::Measurements *measurements = ycsbc::CreateMeasurements(&props);
  if (measurements == nullptr) {
    std::cerr << "Unknown measurements name" << std::endl;
    exit(1);
  }

  std::vector<ycsbc::Measurements*> per_client_measurements = ycsbc::CreatePerClientMeasurements(&props, num_threads);
  for (const auto measurement : per_client_measurements) {
    if (measurement == nullptr) {
      std::cerr << "Unknown per-client measurements name" << std::endl;
      exit(1);
    }
  }

  std::vector<ycsbc::DB *> dbs;
  for (int i = 0; i < num_threads; i++) {
    // ycsbc::DB *db = ycsbc::DBFactory::CreateDB(&props, measurements);
    ycsbc::DB *db = ycsbc::DBFactory::CreateDBWithPerClientStats(&props, measurements, per_client_measurements);
    if (db == nullptr) {
      std::cerr << "Unknown database name " << props["dbname"] << std::endl;
      exit(1);
    }
    dbs.push_back(db);
  }

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  // print status periodically
  const bool show_status = (props.GetProperty("status", "false") == "true");
  const int status_interval = std::stoi(props.GetProperty("status.interval", "2"));

  // load phase
  if (do_load) {
    const int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);

    ycsbc::utils::CountDownLatch latch(num_threads);
    ycsbc::utils::Timer<double> timer;

    timer.Start();
    std::future<void> status_future;
    if (show_status) {
      status_future = std::async(std::launch::async, StatusThread,
                                 measurements, per_client_measurements, &latch, status_interval, dbs);
    }
    std::vector<std::future<std::tuple<long long, std::vector<int>>>> client_threads;
    for (int i = 0; i < num_threads; ++i) {
      int thread_ops = total_ops / num_threads;
      if (i < total_ops % num_threads) {
        thread_ops++;
      }
      client_threads.emplace_back(std::async(std::launch::async, ycsbc::ClientThread, dbs[i], &wl,
                                             thread_ops, true, true, !do_transaction, &latch, nullptr, nullptr, i, /*target_op_per_s*/0, 0, 0));
    }
    assert((int)client_threads.size() == num_threads);

    std::vector<std::tuple<long long, std::vector<int>>> client_op_progresses;
    int sum = 0;
    for (auto &n : client_threads) {
      assert(n.valid());
      std::tuple<long long, std::vector<int>> start_and_client_op_progress = n.get();
      client_op_progresses.push_back(start_and_client_op_progress);
      sum += std::get<1>(start_and_client_op_progress).back();
    }
    double runtime = timer.End();

    if (show_status) {
      status_future.wait();
    }

    std::cout << "Load runtime(sec): " << runtime << std::endl;
    std::cout << "Load operations(ops): " << sum << std::endl;
    std::cout << "Load throughput(ops/sec): " << sum / runtime << std::endl;
  }

  measurements->Reset();
  std::this_thread::sleep_for(std::chrono::seconds(stoi(props.GetProperty("sleepafterload", "0"))));

  // FairScheduler scheduler;
  ThreadPool threadpool;
  // threadpool.start(/*num_threads=*/ 1);

  int burst_gap_s = std::stoi(props.GetProperty("burst_gap_s", "0"));
  int burst_size_ops = std::stoi(props.GetProperty("burst_size_ops", "0"));

  // transaction phase
  if (do_transaction) {
    // initial ops per second, unlimited if <= 0
    const int64_t ops_limit = std::stoi(props.GetProperty("limit.ops", "0"));
    // rate file path for dynamic rate limiting, format "time_stamp_sec new_ops_per_second" per line
    std::string rate_file = props.GetProperty("limit.file", "");

    const int total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);

    ycsbc::utils::CountDownLatch latch(num_threads);
    ycsbc::utils::Timer<double> timer;

    timer.Start();
    std::future<void> status_future;
    if (show_status) {
      status_future = std::async(std::launch::async, StatusThread,
                                 measurements, per_client_measurements, &latch, status_interval, dbs);
    }
    std::vector<std::future<std::tuple<long long, std::vector<int>>>> client_threads;
    std::vector<ycsbc::utils::RateLimiter *> rate_limiters;
    for (int i = 0; i < num_threads; ++i) {
      int thread_ops = total_ops / num_threads;
      if (i < total_ops % num_threads) {
        thread_ops++;
      }
      ycsbc::utils::RateLimiter *rlim = nullptr;
      if (ops_limit > 0 || rate_file != "") {
        int64_t per_thread_ops = ops_limit / num_threads;
        rlim = new ycsbc::utils::RateLimiter(per_thread_ops, per_thread_ops);
      }
      rate_limiters.push_back(rlim);
      client_threads.emplace_back(std::async(std::launch::async, ycsbc::ClientThread, dbs[i], &wl,
                                             thread_ops, false, !do_load, true, &latch, rlim, 
                                             &threadpool, i, target_rates[i], burst_gap_s, 
                                             burst_size_ops));
    }

    std::future<void> rlim_future;
    if (rate_file != "") {
      rlim_future = std::async(std::launch::async, RateLimitThread, rate_file, rate_limiters, &latch);
    }

    std::future<void> rsched_future;
    rsched_future = std::async(std::launch::async, ResourceSchedulerThread, dbs, 
                               measurements, per_client_measurements, 1, 5, &latch);

    assert((int)client_threads.size() == num_threads);

    std::vector<std::tuple<long long, std::vector<int>>> client_op_progresses;
    int sum = 0;
    for (auto &n : client_threads) {
      assert(n.valid());
      std::tuple<long long, std::vector<int>> start_and_client_op_progress = n.get();
      client_op_progresses.push_back(start_and_client_op_progress);
      sum += std::get<1>(start_and_client_op_progress).back();
    }
    double runtime = timer.End();

    if (show_status) {
      status_future.wait();
      rsched_future.wait();
    }

    writeToCSV("client_progress.csv", client_op_progresses);

    std::cout << "Run runtime(sec): " << runtime << std::endl;
    std::cout << "Run operations(ops): " << sum << std::endl;
    std::cout << "Run throughput(ops/sec): " << sum / runtime << std::endl;
  }

  for (int i = 0; i < num_threads; i++) {
    delete dbs[i];
  }
}

void ParseCommandLine(int argc, const char *argv[], ycsbc::utils::Properties &props) {
  int argindex = 1;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-load") == 0) {
      props.SetProperty("doload", "true");
      argindex++;
    } else if (strcmp(argv[argindex], "-run") == 0 || strcmp(argv[argindex], "-t") == 0) {
      props.SetProperty("dotransaction", "true");
      argindex++;
    } else if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -threads" << std::endl;
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-target_rates") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -target_rates" << std::endl;
        exit(0);
      }
      props.SetProperty("target_rates", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -db" << std::endl;
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -P" << std::endl;
        exit(0);
      }
      std::string filename(argv[argindex]);
      std::ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const std::string &message) {
        std::cerr << message << std::endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else if (strcmp(argv[argindex], "-p") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -p" << std::endl;
        exit(0);
      }
      std::string prop(argv[argindex]);
      size_t eq = prop.find('=');
      if (eq == std::string::npos) {
        std::cerr << "Argument '-p' expected to be in key=value format "
                     "(e.g., -p operationcount=99999)" << std::endl;
        exit(0);
      }
      props.SetProperty(ycsbc::utils::Trim(prop.substr(0, eq)),
                        ycsbc::utils::Trim(prop.substr(eq + 1)));
      argindex++;
    } else if (strcmp(argv[argindex], "-s") == 0) {
      props.SetProperty("status", "true");
      argindex++;
    } else {
      UsageMessage(argv[0]);
      std::cerr << "Unknown option '" << argv[argindex] << "'" << std::endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }
}

void UsageMessage(const char *command) {
  std::cout <<
      "Usage: " << command << " [options]\n"
      "Options:\n"
      "  -load: run the loading phase of the workload\n"
      "  -t: run the transactions phase of the workload\n"
      "  -run: same as -t\n"
      "  -threads n: execute using n threads (default: 1)\n"
      "  -db dbname: specify the name of the DB to use (default: basic)\n"
      "  -P propertyfile: load properties from the given file. Multiple files can\n"
      "                   be specified, and will be processed in the order specified\n"
      "  -p name=value: specify a property to be passed to the DB and workloads\n"
      "                 multiple properties can be specified, and override any\n"
      "                 values in the propertyfile\n"
      "  -s: print status every 10 seconds (use status.interval prop to override)"
      << std::endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

