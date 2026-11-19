#include "sdr_runtime/iq_sample.hpp"
#include "sdr_runtime/rt_thread.hpp"
#include "sdr_runtime/spsc_ring_buffer.hpp"
#include "sdr_runtime/telemetry_shm.hpp"
#include "sdr_runtime/watchdog.hpp"

#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>

namespace {

std::atomic<bool> g_running{true};

void signal_handler(int) {
    g_running.store(false, std::memory_order_release);
}

struct RuntimeArgs {
    double duration_sec{3.0};
    double sample_rate_hz{2'400'000.0}; // 2.4 MSPS nominal
    int producer_core{0};
    int consumer_core{1};
};

RuntimeArgs parse_args(int argc, char** argv) {
    RuntimeArgs args;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--duration-sec") == 0 && i + 1 < argc) {
            args.duration_sec = std::atof(argv[++i]);
        } else if (std::strcmp(argv[i], "--sample-rate") == 0 && i + 1 < argc) {
            args.sample_rate_hz = std::atof(argv[++i]);
        } else if (std::strcmp(argv[i], "--producer-core") == 0 && i + 1 < argc) {
            args.producer_core = std::atoi(argv[++i]);
        } else if (std::strcmp(argv[i], "--consumer-core") == 0 && i + 1 < argc) {
            args.consumer_core = std::atoi(argv[++i]);
        }
    }
    if (args.duration_sec <= 0.0) args.duration_sec = 1.0;
    if (args.sample_rate_hz <= 0.0) args.sample_rate_hz = 2'400'000.0;
    return args;
}

} // namespace

int main(int argc, char** argv) {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    const RuntimeArgs args = parse_args(argc, argv);

    sdr_runtime::TelemetryShm shm("/sdr_runtime_telemetry");
    if (!shm.init_creator()) {
        std::cerr << "Failed to initialize shared memory telemetry\n";
        return 1;
    }
    auto* telem = shm.block();

    // Power-of-two ring capacity (64 chunks = ~65k samples buffered)
    constexpr std::size_t kRingCapacity = 64;
    sdr_runtime::SpscRingBuffer<sdr_runtime::IqChunk, kRingCapacity> iq_ring;

    // Pre-allocated chunk templates outside hot loops
    sdr_runtime::IqChunk prod_chunk{};
    sdr_runtime::IqChunk cons_chunk{};

    const std::uint64_t start_ns = sdr_runtime::monotonic_now_ns();
    const std::uint64_t end_ns =
        start_ns + static_cast<std::uint64_t>(args.duration_sec * 1e9);

    // Chunk cadence for 2.4 MSPS with 1024 samples/chunk (~426.67 us)
    const double chunk_interval_ns_exact =
        (static_cast<double>(sdr_runtime::kSamplesPerChunk) / args.sample_rate_hz) * 1e9;
    const auto chunk_interval_ns = static_cast<std::uint64_t>(chunk_interval_ns_exact);

    // Producer thread: simulates SDR DMA/RF hardware burst acquisition at sample_rate_hz
    std::thread producer([&]() {
        auto& slot = telem->threads[0];
        slot.active.store(1, std::memory_order_release);

        std::uint64_t seq = 0;
        double phase = 0.0;
        const double phase_inc = 2.0 * 3.14159265358979323846 * 125'000.0 / args.sample_rate_hz;
        std::uint64_t next_deadline_ns = sdr_runtime::monotonic_now_ns();

        while (g_running.load(std::memory_order_acquire)) {
            const std::uint64_t now = sdr_runtime::monotonic_now_ns();
            if (now >= end_ns) {
                g_running.store(false, std::memory_order_release);
                break;
            }

            if (now >= next_deadline_ns) {
                prod_chunk.sequence_id = seq++;
                prod_chunk.timestamp_ns = now;
                prod_chunk.sample_count = static_cast<std::uint32_t>(sdr_runtime::kSamplesPerChunk);
                prod_chunk.flags = 0;

                for (std::size_t i = 0; i < sdr_runtime::kSamplesPerChunk; ++i) {
                    prod_chunk.samples[i].i = static_cast<float>(std::cos(phase));
                    prod_chunk.samples[i].q = static_cast<float>(std::sin(phase));
                    phase += phase_inc;
                    if (phase >= 6.28318530717958647692) {
                        phase -= 6.28318530717958647692;
                    }
                }

                if (iq_ring.try_push(prod_chunk)) {
                    telem->chunks_produced.fetch_add(1, std::memory_order_release);
                } else {
                    telem->chunks_dropped.fetch_add(1, std::memory_order_release);
                }

                next_deadline_ns += chunk_interval_ns;
                if (next_deadline_ns < now) {
                    next_deadline_ns = now + chunk_interval_ns;
                }
            }

            slot.last_heartbeat_ns.store(now, std::memory_order_release);
            slot.iterations.fetch_add(1, std::memory_order_relaxed);
            telem->ring_occupancy.store(iq_ring.size_approx(), std::memory_order_release);
            telem->uptime_ns.store(now - start_ns, std::memory_order_release);
        }

        slot.active.store(0, std::memory_order_release);
    });

    // Consumer thread: deterministic DSP/energy detector over incoming IQ chunks
    std::thread consumer([&]() {
        auto& slot = telem->threads[1];
        slot.active.store(1, std::memory_order_release);

        std::uint64_t max_lat = 0;
        volatile float sink_power = 0.0f; // Prevent dead-code elimination of DSP pass

        while (g_running.load(std::memory_order_acquire) || iq_ring.size_approx() > 0) {
            const std::uint64_t now = sdr_runtime::monotonic_now_ns();
            slot.last_heartbeat_ns.store(now, std::memory_order_release);
            slot.iterations.fetch_add(1, std::memory_order_relaxed);

            if (iq_ring.try_pop(cons_chunk)) {
                const std::uint64_t t0 = sdr_runtime::monotonic_now_ns();

                float energy_acc = 0.0f;
                for (std::uint32_t i = 0; i < cons_chunk.sample_count; ++i) {
                    const float re = cons_chunk.samples[i].i;
                    const float im = cons_chunk.samples[i].q;
                    energy_acc += (re * re + im * im);
                }
                sink_power = energy_acc / static_cast<float>(cons_chunk.sample_count ? cons_chunk.sample_count : 1);

                const std::uint64_t t1 = sdr_runtime::monotonic_now_ns();
                const std::uint64_t lat = (t1 >= t0) ? (t1 - t0) : 0;

                telem->last_process_latency_ns.store(lat, std::memory_order_release);
                if (lat > max_lat) {
                    max_lat = lat;
                    telem->max_process_latency_ns.store(max_lat, std::memory_order_release);
                }
                telem->chunks_consumed.fetch_add(1, std::memory_order_release);
            }
        }

        (void)sink_power;
        slot.active.store(0, std::memory_order_release);
    });

    sdr_runtime::configure_realtime_thread(
        producer.native_handle(),
        sdr_runtime::RtThreadConfig{args.producer_core, 80, "sdr_iq_producer"});
    sdr_runtime::configure_realtime_thread(
        consumer.native_handle(),
        sdr_runtime::RtThreadConfig{args.consumer_core, 75, "dsp_iq_consumer"});

    sdr_runtime::WatchdogSupervisor watchdog(telem, g_running);
    watchdog.start();

    producer.join();
    consumer.join();
    g_running.store(false, std::memory_order_release);
    telem->shutdown_requested.store(1, std::memory_order_release);
    watchdog.stop();

    const std::uint64_t produced = telem->chunks_produced.load(std::memory_order_acquire);
    const std::uint64_t consumed = telem->chunks_consumed.load(std::memory_order_acquire);
    const std::uint64_t dropped = telem->chunks_dropped.load(std::memory_order_acquire);
    const std::uint64_t max_lat_ns = telem->max_process_latency_ns.load(std::memory_order_acquire);
    const std::uint32_t alerts = telem->watchdog_alerts.load(std::memory_order_acquire);

    std::cout << "=== SDR Runtime Summary ===\n"
              << "Target Sample Rate : " << args.sample_rate_hz << " SPS\n"
              << "Chunks Produced    : " << produced << "\n"
              << "Chunks Consumed    : " << consumed << "\n"
              << "Chunks Dropped     : " << dropped << "\n"
              << "Max DSP Chunk Lat  : " << max_lat_ns << " ns\n"
              << "Watchdog Alerts    : " << alerts << "\n";

    return 0;
}