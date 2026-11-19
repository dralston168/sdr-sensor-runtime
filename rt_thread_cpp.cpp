#include "sdr_runtime/rt_thread.hpp"

#include <cerrno>
#include <cstring>
#include <iostream>
#include <sched.h>
#include <time.h>

namespace sdr_runtime {

int configure_realtime_thread(pthread_t thread, const RtThreadConfig& cfg) noexcept {
    int status = 0;

    if (cfg.core_id >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(static_cast<unsigned int>(cfg.core_id), &cpuset);

        const int aff_rc = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
        if (aff_rc != 0) {
            std::cerr << "[rt_thread] Warning: pthread_setaffinity_np failed for '"
                      << cfg.name << "' on core " << cfg.core_id
                      << ": " << std::strerror(aff_rc) << "\n";
            if (status == 0) {
                status = aff_rc;
            }
        }
    }

    if (cfg.fifo_priority > 0) {
        sched_param param{};
        param.sched_priority = cfg.fifo_priority;
        const int sched_rc = pthread_setschedparam(thread, SCHED_FIFO, &param);
        if (sched_rc != 0) {
            std::cerr << "[rt_thread] Warning: pthread_setschedparam(SCHED_FIFO, prio="
                      << cfg.fifo_priority << ") failed for '" << cfg.name
                      << "': " << std::strerror(sched_rc)
                      << " (run with CAP_SYS_NICE or sudo for RT scheduling)\n";
            if (status == 0) {
                status = sched_rc;
            }
        }
    }

    return status;
}

std::uint64_t monotonic_now_ns() noexcept {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<std::uint64_t>(ts.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(ts.tv_nsec);
}

} // namespace sdr_runtime