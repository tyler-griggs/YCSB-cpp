//
//  client.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <iostream>
#include <string>
#include <chrono>
#include <vector>
#include <tuple>

#include "db.h"
#include "core_workload.h"
#include "utils/countdown_latch.h"
#include "utils/rate_limit.h"
#include "utils/utils.h"
#include "threadpool.h"
namespace ycsbc {

inline std::tuple<long long, std::vector<int>> ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops, bool is_loading,
                        bool init_db, bool cleanup_db, utils::CountDownLatch *latch, utils::RateLimiter *rlim, ThreadPool *threadpool, int client_id) {

  std::vector<int> op_progress;                        

  try {
    if (init_db) {
      db->Init();
    }

    int adjusted_num_ops = num_ops;
    // if (client_id == 0) {
    //   adjusted_num_ops = int(adjusted_num_ops * 1.5);
    // }

    auto full_start = std::chrono::system_clock::now();
    auto full_start_micros = std::chrono::duration_cast<std::chrono::microseconds>(full_start.time_since_epoch()).count();
    auto interval_start_time = std::chrono::steady_clock::now();
    int ops = 0;
    for (int i = 0; i < adjusted_num_ops; ++i) {
      if (rlim) {
        rlim->Consume(1);
      }

      if (is_loading) {
        wl->DoInsert(*db);
      } else {

        auto txn_lambda = [wl, db, client_id]() {
          wl->DoTransaction(*db, client_id);
          return nullptr;  // to match void* return
        };
        std::future<void*> result = threadpool->dispatch(txn_lambda);
        result.wait();

        // wl->DoTransaction(*db, client_id);
      }
      ops++;

      if (i % 100 == 0) {
        auto current_time = std::chrono::steady_clock::now();
        auto elapsedTime = std::chrono::duration_cast<std::chrono::seconds>(current_time - interval_start_time);
        if (elapsedTime.count() >= 2) { // Every 2 seconds
          op_progress.push_back(ops); // Add ops to the vector
          interval_start_time = std::chrono::steady_clock::now(); // Reset startTime
      }
      }
    }

    if (cleanup_db) {
      db->Cleanup();
    }

    latch->CountDown();
    op_progress.push_back(ops);
    // return ops;
    return std::make_tuple(full_start_micros, op_progress);
    // return op_progress;
  } catch (const utils::Exception &e) {
    std::cerr << "Caught exception: " << e.what() << std::endl;
    exit(1);
  }
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
