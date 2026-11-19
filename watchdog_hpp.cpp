#pragma once

#include "sdr_runtime/telemetry_shm.hpp"
#include <atomic>
#include <cstdint>
#include <thread>

namespace sdr_runtime {

class WatchdogSupervisor {
public:
    WatchdogSupervisor(TelemetryBlock* telemetry,
                       std::atomic<bool>& running,
                       std::uint64_t stall_threshold_ns = 50'000'000ULL, // 50 ms
                       std::uint64_t poll_interval_ns = 10'000'000ULL);  // 10 ms
    ~WatchdogSupervisor();

    WatchdogSupervisor(const WatchdogSupervisor&) = delete;
    WatchdogSupervisor& operator=(const WatchdogSupervisor&) = delete;

    void start();
    void stop();

private:
    void run_loop();

    TelemetryBlock* telemetry_;
    std::atomic<bool>& running_;
    std::uint64_t stall_threshold_ns_;
    std::uint64_t poll_interval_ns_;
    std::thread worker_;
    std::atomic<bool> started_{false};
};

} // namespace sdr_runtime