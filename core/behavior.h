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
#include <yaml-cpp/yaml.h>
#include <functional>

namespace ycsbc
{

    enum Operation
    {
        INSERT = 0,
        READ,
        UPDATE,
        SCAN,
        READMODIFYWRITE,
        DELETE,
        RANDOM_INSERT,
        INSERT_BATCH,
        INSERT_FAILED,
        READ_FAILED,
        UPDATE_FAILED,
        SCAN_FAILED,
        READMODIFYWRITE_FAILED,
        DELETE_FAILED,
        INSERT_BATCH_FAILED,
        MAXOPTYPE
    };

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
        int client_id;                   // Unique ID for the client.
        std::string cf;                  // Column family for the client's operations.
        Operation op;                    // Operation type (e.g., read, insert).
        std::vector<Behavior> behaviors; // List of behaviors for this client.
    };

    Operation stringToOperation(const std::string &operationName);

    void executeClientBehaviors(const std::vector<Behavior> &behaviors, const std::function<void()> &send_request);

    std::vector<ClientConfig> loadClientBehaviors(const std::string &yaml_file);

    int calculateTotalOperations(const std::vector<ClientConfig> &clients);

}

#endif // BEHAVIOR_H