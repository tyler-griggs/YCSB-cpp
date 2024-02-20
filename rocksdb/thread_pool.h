#ifndef TGClientThreadPool_H
#define TGClientThreadPool_H

#include "core/db.h"

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <semaphore>

class TGClientThreadPool {
public:
    TGClientThreadPool() {}

    template<class F, class... Args>
    ycsbc::DB::Status accessDB(F&& f, Args&&... args) {
        std::unique_lock<std::mutex> lock(mutex);

        while (num_threads == 0) {
            cond.wait(lock);
        }
        --num_threads;

        lock.unlock();

        // Perform the database operation outside of the critical section
        ycsbc::DB::Status s = f(args...);

        lock.lock();
        ++num_threads;
        cond.notify_one();
        
        return s;
    }

private:
    // AccessDB
    int num_threads = 1;
    std::mutex mutex;
    std::condition_variable cond;
};

#endif // TGClientThreadPool_H
