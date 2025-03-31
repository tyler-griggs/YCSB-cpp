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

    void enforceRequestRate(int interval_us)
    {
        auto start = std::chrono::high_resolution_clock::now();
        while (true)
        {
            auto now = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(now - start).count();
            if (elapsed >= interval_us)
            {
                break;
            }
        }
    }

    void executeSteadyBehavior(int request_rate_qps, int duration_s, const std::function<void()> &send_request)
    {
        int interval_us = 1'000'000 / request_rate_qps; // Microseconds per request
        for (int t = 0; t < duration_s; ++t)
        {
            for (int i = 0; i < request_rate_qps; ++i)
            {
                send_request(); // Use the callback
                enforceRequestRate(interval_us);
            }
        }
    }

    void executeBurstyBehavior(int request_rate_qps, int burst_duration_ms, int idle_duration_ms, int repeats, const std::function<void()> &send_request)
    {
        int total_ops_per_cycle = request_rate_qps * burst_duration_ms / 1000;
        int interval_us = 1'000'000 / request_rate_qps; // Microseconds per request
        for (int r = 0; r < repeats; ++r)
        {
            for (int i = 0; i < total_ops_per_cycle; ++i)
            {
                send_request(); // Use the callback
                enforceRequestRate(interval_us);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(idle_duration_ms));
        }
    }

    void executeInactiveBehavior(int duration_s)
    {
        std::this_thread::sleep_for(std::chrono::seconds(duration_s));
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
            enforceRequestRate(scaled_interval_microseconds);
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
                executeSteadyBehavior(behavior.request_rate_qps, behavior.duration_s, send_request);
                break;
            case BURSTY:
                executeBurstyBehavior(behavior.request_rate_qps, behavior.burst_duration_ms, behavior.idle_duration_ms, behavior.repeats, send_request);
                break;
            case INACTIVE:
                executeInactiveBehavior(behavior.duration_s);
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

            // Parse optional column family (cf) and operation type (op).
            if (!client_node["cf"])
            {
                throw std::runtime_error("column family must be specified for each client.");
            }
            std::string cf = client_node["cf"].as<std::string>();
            // Ensure record_count is specified
            if (!client_node["record_count"])
            {
                throw std::runtime_error("record_count must be specified for each client.");
            }
            int record_count = client_node["record_count"].as<int>();
            clients.emplace_back(client_id, cf, record_count);
            auto &client = clients.back();

            if (client_node["op_distribution"])
            {
                const auto &op_distribution = client_node["op_distribution"];
                double total_weight = 0.0;

                // Calculate total weight for normalization
                for (auto it = op_distribution.begin(); it != op_distribution.end(); ++it)
                {
                    total_weight += it->second.as<double>();
                }

                if (total_weight <= 0.0)
                {
                    throw std::runtime_error("Total weight in op_distribution must be greater than 0.");
                }

                // Add operations and their normalized weights to the op_chooser_
                for (auto it = op_distribution.begin(); it != op_distribution.end(); ++it)
                {
                    std::string operationName = it->first.as<std::string>();
                    double weight = it->second.as<double>() / total_weight; // Normalize weight
                    try
                    {
                        Operation op = stringToOperation(operationName);
                        client.op_chooser_->AddValue(op, weight);
                    }
                    catch (const std::invalid_argument &e)
                    {
                        throw std::runtime_error(std::string("Error in op_distribution: ") + e.what());
                    }
                }
            }
            else
            {
                // Default to always READ if no op_distribution is provided
                client.op_chooser_->AddValue(READ, 1.0);
                std::cout << "[FAIR_LOG] Warning: op_distribution not specified for client_id "
                          << client_node["client_id"].as<int>() << ". Defaulting to READ.\n";
            }

            // Parse client behaviors.
            for (const auto &behavior_node : client_node["behaviors"])
            {
                Behavior behavior;
                behavior.type = parseBehaviorType(behavior_node["type"].as<std::string>());

                switch (behavior.type)
                {
                case STEADY:
                    behavior.request_rate_qps = behavior_node["request_rate_qps"].as<int>();
                    behavior.duration_s = behavior_node["duration_s"].as<int>();
                    break;
                case BURSTY:
                    behavior.request_rate_qps = behavior_node["request_rate_qps"].as<int>();
                    behavior.burst_duration_ms = behavior_node["burst_duration_ms"].as<int>();
                    behavior.idle_duration_ms = behavior_node["idle_duration_ms"].as<int>();
                    behavior.repeats = behavior_node["repeats"].as<int>();
                    break;
                case INACTIVE:
                    behavior.duration_s = behavior_node["duration_s"].as<int>();
                    break;
                case REPLAY:
                    behavior.trace_file = behavior_node["trace_file"].as<std::string>();
                    behavior.client_id = behavior_node["replay_client_id"].as<int>();
                    behavior.scale_ratio = behavior_node["scale_ratio"].as<double>();
                    break;
                default:
                    throw std::runtime_error("Unknown behavior type in YAML configuration.");
                }
                client.behaviors.emplace_back(behavior);
            }
            // Parse optional fields
            int insert_start = client_node["insert_start"] ? client_node["insert_start"].as<int>() : 0;
            client.insert_start_ = insert_start;
            client.request_distribution = client_node["request_distribution"]
                                                   ? client_node["request_distribution"].as<std::string>()
                                                   : "uniform";
            if (client_node["zipfian_const"])
            {
                client.zipfian_const = client_node["zipfian_const"].as<double>();
            }
            // Set up key chooser
            int op_count = calculateOperations(client.behaviors); // Custom function to calculate total ops.
            generateKeyChooser(client, op_count);
        }
        return clients;
    }

    void generateKeyChooser(ClientConfig &client_config, int op_count)
    {
        // Initialize key generators
        client_config.insert_key_sequence_ = std::make_unique<CounterGenerator>(client_config.insert_start_);
        client_config.transaction_insert_key_sequence_ = std::make_unique<AcknowledgedCounterGenerator>(client_config.record_count_);

        // Key chooser logic
        if (client_config.request_distribution == "uniform")
        {
            std::cout << "[FAIRDB_LOG] Client " << client_config.client_id << ": Uniform distribution." << std::endl;
            client_config.key_chooser_ = std::make_unique<UniformGenerator>(0, client_config.record_count_ - 1);
        }
        else if (client_config.request_distribution == "zipfian")
        {
            std::cout << "[FAIRDB_LOG] Client " << client_config.client_id << ": Zipfian distribution." << std::endl;

            // Fetch insert proportion from op_chooser_
            double insert_proportion = client_config.op_chooser_->GetWeight(INSERT);

            // Calculate new keys
            int op_count = calculateOperations(client_config.behaviors);
            int new_keys = static_cast<int>(op_count * insert_proportion * 2); // Fudge factor

            if (client_config.zipfian_const.has_value())
            {
                double zipfian_const = client_config.zipfian_const.value();
                client_config.key_chooser_ = std::make_unique<ScrambledZipfianGenerator>(0, client_config.record_count_ + new_keys - 1, zipfian_const);
                std::cout << "[FAIRDB_LOG] Client " << client_config.client_id << ": Zipfian constant set to " << zipfian_const << std::endl;
            }
            else
            {
                client_config.key_chooser_ = std::make_unique<ScrambledZipfianGenerator>(0, client_config.record_count_ + new_keys - 1);
                std::cout << "[FAIRDB_LOG] Client " << client_config.client_id << ": Using default Zipfian constant." << std::endl;
            }
        }
        else if (client_config.request_distribution == "latest")
        {
            client_config.key_chooser_ = std::make_unique<SkewedLatestGenerator>(*client_config.transaction_insert_key_sequence_);
            std::cout << "[FAIRDB_LOG] Client " << client_config.client_id << ": Latest distribution." << std::endl;
        }
        else
        {
            throw utils::Exception("Unknown request distribution: " + client_config.request_distribution);
        }
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

    int calculateOperations(const std::vector<Behavior> &behaviors)
    {
        int total_operations = 0;

        for (const auto &behavior : behaviors)
        {
            switch (behavior.type)
            {
            case STEADY:
                total_operations += behavior.request_rate_qps * behavior.duration_s;
                break;
            case BURSTY:
                total_operations += (behavior.request_rate_qps * behavior.burst_duration_ms) * behavior.repeats;
                break;
            case INACTIVE:
                // No operations for INACTIVE
                break;
            case REPLAY:
                total_operations += calculateReplayOperations(behavior.trace_file, behavior.client_id, behavior.scale_ratio);
                break;
            }
        }

        return total_operations;
    }

    Operation stringToOperation(const std::string &operationName)
    {
        static const std::unordered_map<std::string, Operation> operationMap = {
            {"INSERT", INSERT},
            {"READ", READ},
            {"READ_BATCH", READ_BATCH},
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
            {"READ_BATCH_FAILED", READ_BATCH_FAILED},
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