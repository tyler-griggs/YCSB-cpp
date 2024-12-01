#ifndef BEHAVIOR_H
#define BEHAVIOR_H

#include <iostream>
#include <thread>
#include <chrono>
#include <fstream>
#include <vector>
#include <string>
#include <cmath>
#include <jsoncpp/json/json.h>

// Define behavior types
enum BehaviorType
{
    STEADY,
    BURSTY,
    INACTIVE,
    REPLAY,
};

struct Behavior
{
    BehaviorType type;
    int request_rate;         // For STEADY and BURSTY
    int duration;             // For STEADY and INACTIVE
    int burst_duration;       // For BURSTY
    int idle_duration;        // For BURSTY
    int repeats;              // For BURSTY
    std::string trace_file;   // For REPLAY
    int client_id = -1;       // Client ID in the trace file (default: -1)
    double scale_ratio = 1.0; // Scale ratio for intervals (default: 1.0)
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

void executeReplayBehavior(const std::string &trace_file, int client_id, double scale_ratio, const std::function<void()> &send_request)
{
    assert(scale_ratio > 0 && "Scale ratio must be greater than 0.");

    // Open the JSON file
    std::ifstream trace(trace_file, std::ifstream::binary);
    if (!trace.is_open())
    {
        throw std::runtime_error("Failed to open trace file: " + trace_file);
    }

    // Parse the JSON file
    Json::Value trace_data;
    Json::CharReaderBuilder reader;
    std::string errs;
    if (!Json::parseFromStream(reader, trace, &trace_data, &errs))
    {
        throw std::runtime_error("Failed to parse JSON: " + errs);
    }

    // Check if the client ID exists in the JSON
    std::string client_id_str = std::to_string(client_id);
    if (!trace_data.isMember(client_id_str))
    {
        throw std::runtime_error("Client ID not found in trace: " + client_id_str);
    }

    // Extract intervals for the specified client
    const Json::Value &client_data = trace_data[client_id_str];
    const Json::Value &intervals = client_data["intervals"];
    if (!intervals.isArray())
    {
        throw std::runtime_error("Invalid format for intervals in trace.");
    }

    // Process intervals with scaling
    for (const auto &interval : intervals)
    {
        // Ensure the interval is numeric
        if (!interval.isNumeric())
        {
            throw std::runtime_error("Invalid interval value in trace.");
        }

        // Parse the interval and scale it
        double scaled_interval_seconds = interval.asDouble() / scale_ratio;

        // Convert seconds to microseconds
        int scaled_interval_microseconds = static_cast<int>(scaled_interval_seconds * 1'000'000);

        // Ensure non-negative intervals
        scaled_interval_microseconds = std::max(scaled_interval_microseconds, 0);

        // Send the request
        send_request();

        // Wait for the scaled interval
        std::this_thread::sleep_for(std::chrono::microseconds(scaled_interval_microseconds));
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
            assert(!behavior.trace_file.empty() && "Replay behavior must have a valid trace file.");
            executeReplayBehavior(behavior.trace_file, behavior.client_id, behavior.scale_ratio, send_request);
            break;
        default:
            throw std::runtime_error("Unknown behavior type.");
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

// Function to load client behaviors from a YAML configuration file
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
                behavior.client_id = behavior_node["replay_client_id"].as<int>();
                behavior.scale_ratio = behavior_node["scale_ratio"].as<double>();
                break;
            default:
                throw std::runtime_error("Unknown behavior type in YAML configuration.");
            }
            client.behaviors.push_back(behavior);
        }
        clients.push_back(client);
    }
    return clients;
}

#endif // BEHAVIOR_H