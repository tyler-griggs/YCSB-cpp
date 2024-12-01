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

struct ClientConfig
{
    int client_id;
    std::vector<Behavior> behaviors;
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

BehaviorType parseBehaviorType(const std::string &type_str)
{
    if (type_str == "STEADY")
        return STEADY;
    if (type_str == "BURSTY")
        return BURSTY;
    if (type_str == "INACTIVE")
        return INACTIVE;
    if (type_str == "REPLAY")
        return REPLAY;
    throw std::invalid_argument("Unknown behavior type: " + type_str);
}

std::vector<ClientConfig> loadClientBehaviors(const std::string &yaml_file)
{
    YAML::Node config = YAML::LoadFile(yaml_file);
    std::vector<ClientConfig> clients;

    for (const auto &client_node : config["clients"])
    {
        ClientConfig client;
        client.client_id = client_node["client_id"].as<int>();

        for (const auto &behavior_node : client_node["behaviors"])
        {
            Behavior behavior;
            behavior.type = parseBehaviorType(behavior_node["type"].as<std::string>());

            switch (behavior.type)
            {
            case STEADY:
                behavior.request_rate = behavior_node["request_rate"].as<int>();
                behavior.duration = behavior_node["duration"].as<int>();
                break;
            case BURSTY:
                behavior.request_rate = behavior_node["request_rate"].as<int>();
                behavior.burst_duration = behavior_node["burst_duration"].as<int>();
                behavior.idle_duration = behavior_node["idle_duration"].as<int>();
                behavior.repeats = behavior_node["repeats"].as<int>();
                break;
            case INACTIVE:
                behavior.duration = behavior_node["duration"].as<int>();
                break;
            case REPLAY:
                behavior.trace_file = behavior_node["trace_file"].as<std::string>();
                break;
            }
            client.behaviors.push_back(behavior);
        }
        clients.push_back(client);
    }

    return clients;
}

#endif // BEHAVIOR_H