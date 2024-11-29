#ifndef BEHAVIOR_H
#define BEHAVIOR_H

#include <iostream>
#include <thread>
#include <chrono>
#include <fstream>
#include <vector>
#include <string>
#include <cmath>

// Define behavior types
enum BehaviorType
{
    STEADY,
    BURSTY,
    INACTIVE,
    REPLAY,
};

// Define a structure to represent behaviors
struct Behavior
{
    BehaviorType type;
    int duration;           // Common duration field
    int request_rate = 0;   // For steady or bursty behavior
    int burst_duration = 0; // For bursts (duration of activity in seconds)
    int idle_duration = 0;  // For bursts (duration of inactivity in seconds)
    int repeats = 0;        // Number of bursts
    std::string trace_file; // For replay behavior
};

void executeSteadyBehavior(int request_rate, int duration, const std::function<void()> &send_request)
{
    int interval_us = 1'000'000 / request_rate; // Microseconds per request
    for (int t = 0; t < duration; ++t)
    {
        for (int i = 0; i < request_rate; ++i)
        {
            send_request(); // Use the callback
            std::this_thread::sleep_for(std::chrono::microseconds(interval_us));
        }
    }
}

void executeBurstyBehavior(int request_rate, int burst_duration, int idle_duration, int repeats, const std::function<void()> &send_request)
{
    int interval_us = 1'000'000 / request_rate; // Microseconds per request
    for (int r = 0; r < repeats; ++r)
    {
        for (int t = 0; t < burst_duration; ++t)
        {
            for (int i = 0; i < request_rate; ++i)
            {
                send_request(); // Use the callback
                std::this_thread::sleep_for(std::chrono::microseconds(interval_us));
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(idle_duration));
    }
}

void executeInactiveBehavior(int duration)
{
    std::this_thread::sleep_for(std::chrono::seconds(duration));
}

void executeReplayBehavior(const std::string &trace_file, const std::function<void()> &send_request)
{
    std::ifstream trace(trace_file);
    int inter_arrival_time;
    while (trace >> inter_arrival_time)
    {
        send_request(); // Use the callback
        std::this_thread::sleep_for(std::chrono::microseconds(inter_arrival_time));
    }
}

// Function to execute client behaviors in sequence
void executeClientBehaviors(const std::vector<Behavior> &behaviors, const std::function<void()> &send_request)
{
    for (const auto &behavior : behaviors)
    {
        switch (behavior.type)
        {
        case STEADY:
            executeSteadyBehavior(behavior.request_rate, behavior.duration, send_request);
            break;
        case BURSTY:
            executeBurstyBehavior(behavior.request_rate, behavior.burst_duration, behavior.idle_duration, behavior.repeats, send_request);
            break;
        case INACTIVE:
            executeInactiveBehavior(behavior.duration);
            break;
        case REPLAY:
            executeReplayBehavior(behavior.trace_file, send_request);
            break;
        }
    }
}

#endif // BEHAVIOR_H