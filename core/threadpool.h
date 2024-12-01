#ifndef _LIB_THREADPOOL_H_
#define _LIB_THREADPOOL_H_

#include <functional>
#include <future>
#include <thread>
#include <vector>
#include <condition_variable>
#include <mutex>
#include <atomic>
#include "concurrentqueue/concurrentqueue.h"
#include "concurrentqueue/blockingconcurrentqueue.h"

class ThreadPool {
public:
    ThreadPool() {};
    virtual ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    void start(int num_threads = 1, int num_clients = 1);
    void stop();
    std::future<void*> dispatch(int client_id, std::function<void*()> f);
    void async_dispatch(int client_id, std::function<void*()> f);

    // Get access to the producer side of a specific client queue
    moodycamel::BlockingConcurrentQueue<std::function<void*()>>& getClientQueue(int client_id);

private:
    std::atomic<bool> running;
    std::vector<std::thread*> threads;
    int num_clients;

    // Vector of per-client queues
    std::vector<moodycamel::BlockingConcurrentQueue<std::function<void*()>>> worklists;

    // Synchronization primitives
    std::mutex cv_mutex;
    std::condition_variable cv;
};

#endif  // _LIB_THREADPOOL_H_
