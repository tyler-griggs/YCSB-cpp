#ifndef BEHAVIOR_H
#define BEHAVIOR_H

#include "generator.h"
#include "uniform_generator.h"
#include "zipfian_generator.h"
#include "scrambled_zipfian_generator.h"
#include "skewed_latest_generator.h"
#include "discrete_generator.h"
#include "acknowledged_counter_generator.h"
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
#include <optional>

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
        QUEUE,
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
        int client_id;                                                                  // Unique client ID
        std::string cf;                                                                 // Column family name
        std::unique_ptr<DiscreteGenerator<Operation>> op_chooser_;                      // Operation chooser
        std::unique_ptr<Generator<uint64_t>> key_chooser_;                              // Key chooser (e.g., Uniform, Zipfian, etc.)
        std::unique_ptr<CounterGenerator> insert_key_sequence_;                         // Sequence for insert keys
        std::unique_ptr<AcknowledgedCounterGenerator> transaction_insert_key_sequence_; // Sequence for transaction inserts
        std::vector<Behavior> behaviors;                                                // Client behaviors
        int record_count_;                                                              // Total records
        int insert_start_ = 0;                                                          // Starting key for inserts (default 0)
        std::string request_distribution = "uniform";                                   // Request distribution (default: uniform)
        std::optional<double> zipfian_const;                                            // Optional Zipfian constant for zipfian distribution

        ClientConfig(int client_id, const std::string &cf_value, int record_count_)
            : client_id(client_id), cf(cf_value), // Initialize in declaration order
              op_chooser_(std::make_unique<DiscreteGenerator<Operation>>()),
              record_count_(record_count_)
        {
        }
    };

    Operation stringToOperation(const std::string &operationName);

    void executeClientBehaviors(const std::vector<Behavior> &behaviors, const std::function<void()> &send_request);

    std::vector<ClientConfig> loadClientBehaviors(const std::string &yaml_file);

    int calculateOperations(const std::vector<Behavior> &behaviors);

    void generateKeyChooser(ClientConfig &client_config, int op_count);

}

#endif // BEHAVIOR_H