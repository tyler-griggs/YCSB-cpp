#ifndef BEHAVIOR_EXECUTOR_H
#define BEHAVIOR_EXECUTOR_H

#include "behavior.h"

void executeSteadyBehavior(int request_rate, int duration);
void executeBurstyBehavior(int request_rate, int burst_duration, int idle_duration, int repeats);
void executeInactiveBehavior(int duration);
void executeReplayBehavior(const std::string &trace_file);

#endif // BEHAVIOR_EXECUTOR_H