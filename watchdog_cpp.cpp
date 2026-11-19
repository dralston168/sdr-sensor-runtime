#include "sdr_runtime/watchdog.hpp"
#include "sdr_runtime/rt_thread.hpp"

#include <chrono>
#include <thread>

namespace sdr_runtime {

WatchdogSupervisor::WatchdogSupervisor(TelemetryBlock* telemetry,
                                       std::atomic<bool>& running,
                                       std::uint64_t stall_threshold_ns,
                                       std::uint64_t poll_interval_ns)
    : telemetry_(telemetry),
      running_(running),
      stall_threshold_ns_(stall_threshold_ns),
      poll_interval_ns_(poll_interval_ns) {}

WatchdogSupervisor::~WatchdogSupervisor() {
    stop();
}

void WatchdogSupervisor::start() {
    if (started_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    worker_ = std::thread(&WatchdogSupervisor::run_loop, this);
}

void WatchdogSupervisor::stop() {
    if (!started_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    if (worker_.joinable()) {
        worker_.join();
    }
}

void WatchdogSupervisor::run_loop() {
    while (running_.load(std::memory_order_acquire) &&
           started_.load(std::memory_order_acquire)) {
        const std::uint64_t now = monotonic_now_ns();

        if (telemetry_ != nullptr) {
            for (std::size_t i = 0; i < kMaxMonitoredThreads; ++i) {
                auto& slot = telemetry_->threads[i];
                if (slot.active.load(std::memory_order_acquire) == 0) {
                    continue;
                }
                const std::uint64_t hb = slot.last_heartbeat_ns.load(std::memory_order_acquire);
                if (hb > 0 && (now - hb) > stall_threshold_ns_) {
                    const std::uint32_t prev_stalled =
                        slot.stalled.exchange(1, std::memory_order_acq_rel);
                    if (prev_stalled == 0) {
                        telemetry_->watchdog_alerts.fetch_add(1, std::memory_order_acq_rel);
                    }
                } else {
                    slot.stalled.store(0, std::memory_order_release);
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::nanoseconds(poll_interval_ns_));
    }
}

} // namespace sdr_runtime