#include "behavior.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <fstream>
#include <vector>
#include <string>
#include <cmath>
#include <jsoncpp/json/json.h>
#include <yaml-cpp/yaml.h>
#include <functional>
#include <cassert>

namespace ycsbc
{

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
            if (!interval.isNumeric())
            {
                throw std::runtime_error("Invalid interval value in trace.");
            }
            double scaled_interval_seconds = interval.asDouble() / scale_ratio;
            int scaled_interval_microseconds = static_cast<int>(scaled_interval_seconds * 1'000'000);
            scaled_interval_microseconds = std::max(scaled_interval_microseconds, 0);

            send_request();
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

    std::vector<ClientConfig> loadClientBehaviors(const std::string &yaml_file)
    {
        YAML::Node config = YAML::LoadFile(yaml_file); // Load the YAML configuration file.
        std::vector<ClientConfig> clients;

        // Keep track of used client IDs to ensure uniqueness.
        std::set<int> used_client_ids;
        int total_clients = config["clients"].size();

        for (const auto &client_node : config["clients"])
        {
            ClientConfig client;
            int client_id = client_node["client_id"].as<int>(); // Parse client_id.

            // Check if client_id is unique.
            if (used_client_ids.find(client_id) != used_client_ids.end())
            {
                throw std::runtime_error("Duplicate client_id found: " + std::to_string(client_id));
            }

            // Check if client_id is within valid range.
            if (client_id < 0 || client_id >= total_clients)
            {
                throw std::runtime_error("Invalid client_id: " + std::to_string(client_id) +
                                         ". It must be between 0 and " + std::to_string(total_clients - 1));
            }

            // Add the client_id to the set of used IDs.
            used_client_ids.insert(client_id);

            client.client_id = client_id; // Assign the validated client_id.

            // Parse optional column family (cf) and operation type (op).
            if (client_node["cf"])
            {
                client.cf = client_node["cf"].as<std::string>();
            }
            else
            {
                client.cf = "default"; // Default value if cf is not provided.
            }

            if (client_node["op"])
            {
                client.op = stringToOperation(client_node["op"].as<std::string>());
            }
            else
            {
                client.op = READ; // Default value if op is not provided.
            }

            // Parse client behaviors.
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

    int calculateReplayOperations(const std::string &trace_file, int replay_client_id, double scale_ratio)
    {
        std::ifstream file(trace_file, std::ifstream::binary);
        if (!file.is_open())
        {
            throw std::runtime_error("Failed to open replay trace file: " + trace_file);
        }

        Json::Value root;
        Json::CharReaderBuilder builder;
        std::string errs;

        if (!Json::parseFromStream(builder, file, &root, &errs))
        {
            throw std::runtime_error("Failed to parse replay trace JSON: " + errs);
        }

        const Json::Value &client_data = root[std::to_string(replay_client_id)];
        const Json::Value &intervals = client_data["intervals"];

        int total_operations = intervals.size();
        return static_cast<int>(total_operations * scale_ratio);
    }

    int calculateTotalOperations(const std::vector<ClientConfig> &clients)
    {
        int total_operations = 0;

        for (const auto &client : clients)
        {
            for (const auto &behavior : client.behaviors)
            {
                switch (behavior.type)
                {
                case STEADY:
                    total_operations += behavior.request_rate * behavior.duration;
                    break;
                case BURSTY:
                    total_operations += (behavior.request_rate * behavior.burst_duration) * behavior.repeats;
                    break;
                case INACTIVE:
                    // No operations for INACTIVE
                    break;
                case REPLAY:
                    total_operations += calculateReplayOperations(behavior.trace_file, behavior.client_id, behavior.scale_ratio);
                    break;
                }
            }
        }

        return total_operations;
    }

    Operation stringToOperation(const std::string &operationName)
    {
        static const std::unordered_map<std::string, Operation> operationMap = {
            {"INSERT", INSERT},
            {"READ", READ},
            {"UPDATE", UPDATE},
            {"SCAN", SCAN},
            {"READMODIFYWRITE", READMODIFYWRITE},
            {"DELETE", DELETE},
            {"RANDOM_INSERT", RANDOM_INSERT},
            {"INSERT_BATCH", INSERT_BATCH},
            {"INSERT_FAILED", INSERT_FAILED},
            {"READ_FAILED", READ_FAILED},
            {"UPDATE_FAILED", UPDATE_FAILED},
            {"SCAN_FAILED", SCAN_FAILED},
            {"READMODIFYWRITE_FAILED", READMODIFYWRITE_FAILED},
            {"DELETE_FAILED", DELETE_FAILED},
            {"INSERT_BATCH_FAILED", INSERT_BATCH_FAILED},
            {"MAXOPTYPE", MAXOPTYPE}};

        auto it = operationMap.find(operationName);
        if (it != operationMap.end())
        {
            return it->second;
        }
        else
        {
            throw std::invalid_argument("Invalid operation name: " + operationName);
        }
    }

}