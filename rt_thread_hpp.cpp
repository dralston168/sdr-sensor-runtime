#pragma once

#include <cstdint>
#include <pthread.h>
#include <string>

namespace sdr_runtime {

struct RtThreadConfig {
    int core_id{-1};         // >= 0 pins thread to specific logical CPU core
    int fifo_priority{0};    // > 0 attempts SCHED_FIFO with given priority
    std::string name;
};

// Applies CPU affinity and optional SCHED_FIFO scheduling policy to the given pthread.
// Returns 0 on success or a positive errno value on failure.
// Note: If SCHED_FIFO fails due to permissions (EPERM), logs a warning and continues
// with affinity set so unprivileged test environments do not crash.
int configure_realtime_thread(pthread_t thread, const RtThreadConfig& cfg) noexcept;

// Returns monotonic nanosecond timestamp using CLOCK_MONOTONIC.
std::uint64_t monotonic_now_ns() noexcept;

} // namespace sdr_runtime