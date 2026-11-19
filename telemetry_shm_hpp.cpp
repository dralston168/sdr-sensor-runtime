#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

namespace sdr_runtime {

inline constexpr std::size_t kMaxMonitoredThreads = 4;

struct alignas(64) ThreadHealthSlot {
    std::atomic<std::uint64_t> last_heartbeat_ns{0};
    std::atomic<std::uint64_t> iterations{0};
    std::atomic<std::uint32_t> active{0};
    std::atomic<std::uint32_t> stalled{0};
};

// Shared memory telemetry block exposed at /dev/shm/<name>
struct alignas(64) TelemetryBlock {
    std::uint32_t magic{0x53445254}; // 'SDRT'
    std::uint32_t version{1};
    std::atomic<std::uint64_t> uptime_ns{0};
    std::atomic<std::uint64_t> chunks_produced{0};
    std::atomic<std::uint64_t> chunks_consumed{0};
    std::atomic<std::uint64_t> chunks_dropped{0};
    std::atomic<std::uint64_t> ring_occupancy{0};
    std::atomic<std::uint64_t> max_process_latency_ns{0};
    std::atomic<std::uint64_t> last_process_latency_ns{0};
    std::atomic<std::uint32_t> watchdog_alerts{0};
    std::atomic<std::uint32_t> shutdown_requested{0};
    ThreadHealthSlot threads[kMaxMonitoredThreads];
};

class TelemetryShm {
public:
    explicit TelemetryShm(std::string shm_name = "/sdr_runtime_telemetry");
    ~TelemetryShm();

    TelemetryShm(const TelemetryShm&) = delete;
    TelemetryShm& operator=(const TelemetryShm&) = delete;

    bool init_creator();
    void cleanup();

    TelemetryBlock* block() noexcept { return block_; }
    const TelemetryBlock* block() const noexcept { return block_; }

private:
    std::string name_;
    int fd_{-1};
    bool created_{false};
    TelemetryBlock* block_{nullptr};
};

} // namespace sdr_runtime