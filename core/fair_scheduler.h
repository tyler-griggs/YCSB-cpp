#ifndef FairScheduler_H
#define FairScheduler_H

#include <queue>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>

class FairScheduler {
public:
    FairScheduler() {}

    template<class F, class... Args>
    bool Schedule(F&& f, Args&&... args) {
        std::unique_lock<std::mutex> lock(mutex);

        while (num_threads == 0) {
            cond.wait(lock);
        }
        --num_threads;

        lock.unlock();

        // Perform the database operation outside of the critical section
        bool success = f(args...);

        lock.lock();
        ++num_threads;
        cond.notify_one();
        
        return success;
    }

private:
    // AccessDB
    int num_threads = 4;
    std::mutex mutex;
    std::condition_variable cond;
};

#endif // FairScheduler_H
