#include <iostream>
#include <vector>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <atomic>

#ifndef THREAD_POOL_H
#define THREAD_POOL_H

class MyThreadPool
{
public:
    MyThreadPool(size_t threads) : tasks_in_progress(0), stop(false)
    {
        for (size_t i = 0; i < threads; ++i)
        {
            workers.emplace_back(
                [this]
                {
                    while (true)
                    {
                        std::function<void()> task;

                        // Wait for a task or stop signal
                        {
                            std::unique_lock<std::mutex> lock(queue_mutex);
                            condition.wait(lock, [this]
                                           { return stop || !tasks.empty(); });
                            if (stop && tasks.empty())
                                return;
                            task = std::move(tasks.front());
                            tasks.pop();
                            ++tasks_in_progress;
                        }

                        // Execute the task
                        task();

                        // Notify when the task completes
                        {
                            std::lock_guard<std::mutex> lock(queue_mutex);
                            --tasks_in_progress;
                        }
                        all_done.notify_one();
                    }
                });
        }
    }

    // Add a new task to the pool
    template <class F, class... Args>
    void enqueue(F &&f, Args &&...args)
    {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            tasks.emplace([f, args...]
                          { f(args...); });
        }
        condition.notify_one();
    }

    // Wait for all tasks to complete
    void waitAll()
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        all_done.wait(lock, [this]
                      { return tasks.empty() && tasks_in_progress == 0; });
    }

    void stopAll()
    {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread &worker : workers)
        {
            if (worker.joinable())
                worker.join();
        }
    }

    // Destructor
    ~MyThreadPool()
    {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread &worker : workers)
        {
            if (worker.joinable())
                worker.join();
        }
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::condition_variable all_done;
    std::atomic<int> tasks_in_progress;
    bool stop;
};

#endif