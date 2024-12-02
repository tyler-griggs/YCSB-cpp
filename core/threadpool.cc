#include "threadpool.h"

#include <iostream>
#include <future>
#include <sched.h>
#include <utility>
#include <thread>
#include <cassert>

void ThreadPool::start(int num_threads, int num_clients){
    std::cout << "[FAIRDB_LOG] Starting thread pool with " << num_threads << " worker threads, " << num_clients << " clients\n";
    int num_cpus = std::thread::hardware_concurrency();

    assert(num_clients > 0);
    running = true;
    this->num_clients = num_clients;

    // Initialize per-client queues
    worklists.resize(num_clients);

    // Start worker threads
    for (int i = 0; i < num_threads; i++) {
        std::thread *t;
        t = new std::thread([this, i] {
            std::cout << "[FAIRDB_LOG] Worker thread " << i << " running on CPU " << sched_getcpu() << "\n";
            size_t client_index = i % this->num_clients; // Use this->num_clients
            while (this->running) {
                std::function<void*()> job;
                bool job_found = false;

                // Try to find a job from the queues in round-robin order
                for (size_t attempt = 0; attempt < this->num_clients; ++attempt) {
                    size_t idx = (client_index + attempt) % this->num_clients;
                    if (this->worklists[idx].try_dequeue(job)) {
                        job_found = true;
                        client_index = (idx + 1) % this->num_clients; // Move to next client
                        break;
                    }
                }

                if (job_found) {
                    job();
                } else {
                    // No job found, wait on condition variable
                    std::unique_lock<std::mutex> lock(this->cv_mutex);
                    if (!this->running) {
                        break;
                    }
                    this->cv.wait(lock);
                    if (!this->running) {
                        break;
                    }
                }
            }
            std::cout << "[FAIRDB_LOG] Worker thread " << i << " done\n";
        });

        /*
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(2*i, &cpuset);
        int rc = pthread_setaffinity_np(t->native_handle(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            fprintf(stderr, "Couldn't set thread affinity.\n");
            std::exit(1);
        }
        */

        threads.push_back(t);
    }
}

ThreadPool::~ThreadPool() {
    stop();
}

void ThreadPool::stop() {
    running = false;

    // Notify all worker threads to wake up and exit
    cv.notify_all();

    for (auto t : threads){
        t->join();
        delete t;
    }
    threads.clear();
}

std::future<void*> ThreadPool::dispatch(int client_id, std::function<void*()> f) {
    assert(client_id >= 0 && client_id < num_clients);

    auto promise = std::make_shared<std::promise<void*>>();
    std::future<void*> result = promise->get_future();

    // Wrap the user function to set the promise value upon completion
    std::function<void*()> wrappedJob = [promise, f]() {
        promise->set_value(f());
        return nullptr;
    };

    // Enqueue the job to the appropriate client's queue
    worklists[client_id].enqueue(std::move(wrappedJob));

    // Notify one worker thread that a new job is available
    cv.notify_one();

    return result;
}

void ThreadPool::async_dispatch(int client_id, std::function<void*()> f) {
    // std::cout << "TGRIGGS: enqueue " << client_id << "'s request\n";
    assert(client_id >= 0 && client_id < num_clients);
    worklists[client_id].enqueue(std::move(f));
    // Notify one worker thread that a new job is available
    cv.notify_one();
}

moodycamel::BlockingConcurrentQueue<std::function<void*()>>& ThreadPool::getClientQueue(int client_id) {
    assert(client_id >= 0 && client_id < num_clients);
    return worklists[client_id];
}