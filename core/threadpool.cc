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

#include <thread>
#include <sched.h>
#include <utility>
#include <iostream>

void ThreadPool::start(int num_threads){
  fprintf(stderr, "starting client threadpool\n");
  int num_cpus = std::thread::hardware_concurrency();
  fprintf(stderr, "Num_cpus: %d\nNum_threads: %d\n", num_cpus, num_threads);

  running = true;
  for (int i = 0; i < num_threads; i++) {
    std::thread *t;
    t = new std::thread([this, i] {
      fprintf(stderr, "Thread %d running on CPU %d\n", i, sched_getcpu());
      while (true) {
        std::function<void*()> job;
        {
          worklist.wait_dequeue(job);
          if (!running) {
            break;
          }
        }
        job();
      }
      fprintf(stderr, "Thread %d done\n", i);
    });
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    int rc = pthread_setaffinity_np(t->native_handle(),
                                    sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      fprintf(stderr, "Couldn't set thread affinity.\n");
      std::exit(1);
    }
    fprintf(stderr, "MainThread running on CPU %d.", sched_getcpu());
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

  for (auto t: threads){
    // Add empty jobs to wake up threads.
    dispatch(&EmptyJob);
    t->join();
    delete t;
  }
}

void ThreadPool::dispatch(std::function<void*()> f) {
  worklist.enqueue(std::move(f));
}