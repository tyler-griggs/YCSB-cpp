/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#include "threadpool.h"

#include <iostream>
#include <future>
#include <sched.h>
#include <utility>
#include <thread>

void ThreadPool::start(int num_threads){
  fprintf(stderr, "starting client threadpool\n");
  int num_cpus = std::thread::hardware_concurrency();
  fprintf(stderr, "Num_cpus: %d\nNum_threads: %d\n", num_cpus, num_threads);
  fprintf(stderr, "MainThread running on CPU %d\n", sched_getcpu());

  running = true;
  for (int i = 0; i < num_threads; i++) {
    std::thread *t;
    t = new std::thread([this, i] {
      fprintf(stderr, "Worker thread %d running on CPU %d\n", i, sched_getcpu());
      while (true) {
        std::function<void*()> job;
        {
          worklist.wait_dequeue(job);
          if (!running) {
            break;
          }
        }
        // fprintf(stderr, "Thread %d starting job\n", i);
        job();
      }
      fprintf(stderr, "Worker thread %d done\n", i);
    });
    // cpu_set_t cpuset;
    // CPU_ZERO(&cpuset);
    // CPU_SET(16 + i, &cpuset);
    // int rc = pthread_setaffinity_np(t->native_handle(),
    //                                 sizeof(cpu_set_t), &cpuset);
    // if (rc != 0) {
    //   fprintf(stderr, "Couldn't set thread affinity.\n");
    //   std::exit(1);
    // }
    threads.push_back(t);
    // t->detach();
  }
}

ThreadPool::~ThreadPool() {
  stop();
}

void* EmptyJob() {
  return nullptr;
}

void ThreadPool::stop() {
  running = false;

  // Add empty jobs to wake up threads.
  for (int i = 0; i < threads.size(); ++i) {
    dispatch(&EmptyJob);
  }

  for (auto t: threads){
    t->join();
    delete t;
  }
}

std::future<void*> ThreadPool::dispatch(std::function<void*()> f) {
    std::shared_ptr<std::promise<void*>> promise = std::make_shared<std::promise<void*>>();
    std::future<void*> result = promise->get_future();

    // Wrap the user function to set the promise value upon completion
    std::function<void*()> wrappedJob = [promise, f]() {
      promise->set_value(f());
      return nullptr;
    };

    worklist.enqueue(std::move(wrappedJob));
    return result;
}

// std::future<void*> ThreadPool::dispatch(std::function<void*()> f) {
//   std::packaged_task<void*()> task(std::move(f));
//   std::future<void*> result = task.get_future();
//   worklist.enqueue(std::move(f));
//   return result;
// }