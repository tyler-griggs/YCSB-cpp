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
namespace ycsbc
{

  inline long long ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops, bool is_loading,
                                bool init_db, bool cleanup_db, utils::CountDownLatch *latch, utils::RateLimiter *rlim, ThreadPool *threadpool,
                                int client_id, ClientConfig *client_config)
  {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);

    size_t cpu_for_client = client_id + 8;
    CPU_SET(cpu_for_client, &cpuset);
    std::cout << "[TGRIGGS_LOG] Pinning client to " << cpu_for_client << std::endl;
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
          wl->DoInsert(*db);
        }
      }
      else
      {
        auto txn_lambda = [wl, db, client_id, threadpool, rlim]()
        {
          if (rlim)
          {
            rlim->Consume(1);
          }
          auto lambda = [wl, db, client_id]()
          {
            wl->DoTransaction(*db, client_id);
            return nullptr; // to match void* return
          };
          threadpool->async_dispatch(client_id, lambda);
        };
        executeClientBehaviors(client_config->behaviors, txn_lambda);
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
