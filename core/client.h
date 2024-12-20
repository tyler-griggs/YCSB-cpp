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
#include <pthread.h>
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
#include "behavior.h"
#include "measurements.h"
namespace ycsbc
{

  inline long long ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops, bool is_loading,
                                bool init_db, bool cleanup_db, utils::CountDownLatch *latch, utils::RateLimiter *rlim, ThreadPool *threadpool,
                                ClientConfig *client_config, std::vector<ycsbc::Measurements *> &queuing_delay_measurements)
  {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);

    size_t cpu_for_client = client_config->client_id;
    CPU_SET(cpu_for_client, &cpuset);
    std::cout << "[FAIRDB_LOG] Pinning client " << client_config->client_id << " to core " << cpu_for_client << std::endl;
    int rc = pthread_setaffinity_np(pthread_self(),
                                    sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
      fprintf(stderr, "Couldn't set thread affinity.\n");
      std::exit(1);
    }
    try
    {
      if (init_db)
      {
        db->Init();
      }

      auto client_start = std::chrono::system_clock::now();
      auto client_start_micros = std::chrono::duration_cast<std::chrono::microseconds>(client_start.time_since_epoch()).count();
      if (is_loading)
      {
        for (int i = 0; i < num_ops; ++i)
        {
          if (rlim)
          {
            rlim->Consume(1);
          }
          wl->DoInsert(*db, client_config);
        }
      }
      else
      {
        auto transaction_executor = [wl, db, client_config, threadpool, rlim, &queuing_delay_measurements]()
        {
          if (rlim)
          {
            rlim->Consume(1);
          }
          auto enqueue_start_time = std::chrono::high_resolution_clock::now();
          auto transaction_task = [wl, db, client_config, enqueue_start_time, &queuing_delay_measurements]()
          {
            auto dequeue_time = std::chrono::high_resolution_clock::now();
            auto queueing_delay = std::chrono::duration_cast<std::chrono::nanoseconds>(dequeue_time - enqueue_start_time).count();
            queuing_delay_measurements[client_config->client_id]->Report(QUEUE, queueing_delay);

          wl->DoTransaction(*db, client_config);
            return nullptr; // to match void* return
          };
          threadpool->async_dispatch(client_config->client_id, transaction_task);
        };
        executeClientBehaviors(client_config->behaviors, transaction_executor);
      }

      if (cleanup_db)
      {
        db->Cleanup();
      }

      latch->CountDown();
      return client_start_micros;
    }
    catch (const utils::Exception &e)
    {
      std::cerr << "Caught exception: " << e.what() << std::endl;
      exit(1);
    }
  }

} // ycsbc

#endif // YCSB_C_CLIENT_H_
