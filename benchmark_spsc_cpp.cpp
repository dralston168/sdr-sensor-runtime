#include "sdr_runtime/iq_sample.hpp"
#include "sdr_runtime/rt_thread.hpp"
#include "sdr_runtime/spsc_ring_buffer.hpp"

#include <atomic>
#include <cstdint>
#include <iostream>
#include <thread>

int main() {
    constexpr std::size_t kCapacity = 256;
    constexpr std::uint64_t kTotalChunks = 500'000;

    sdr_runtime::SpscRingBuffer<sdr_runtime::IqChunk, kCapacity> ring;
    sdr_runtime::IqChunk sample_chunk{};
    sample_chunk.sample_count = static_cast<std::uint32_t>(sdr_runtime::kSamplesPerChunk);

    std::atomic<bool> go{false};
    std::atomic<std::uint64_t> max_pop_ns{0};

    std::thread producer([&]() {
        while (!go.load(std::memory_order_acquire)) {
        }
        for (std::uint64_t seq = 0; seq < kTotalChunks; ++seq) {
            sdr_runtime::IqChunk c = sample_chunk;
            c.sequence_id = seq;
            c.timestamp_ns = sdr_runtime::monotonic_now_ns();
            while (!ring.try_push(c)) {
            }
        }
    });

    std::thread consumer([&]() {
        sdr_runtime::IqChunk out{};
        std::uint64_t local_max = 0;
        while (!go.load(std::memory_order_acquire)) {
        }
        for (std::uint64_t seq = 0; seq < kTotalChunks; ++seq) {
            while (!ring.try_pop(out)) {
            }
            const std::uint64_t now = sdr_runtime::monotonic_now_ns();
            const std::uint64_t lat = (now >= out.timestamp_ns) ? (now - out.timestamp_ns) : 0;
            if (lat > local_max) {
                local_max = lat;
            }
        }
        max_pop_ns.store(local_max, std::memory_order_release);
    });

    sdr_runtime::configure_realtime_thread(
        producer.native_handle(),
        sdr_runtime::RtThreadConfig{0, 80, "bench_prod"});
    sdr_runtime::configure_realtime_thread(
        consumer.native_handle(),
        sdr_runtime::RtThreadConfig{1, 75, "bench_cons"});

    const std::uint64_t run_start_ns = sdr_runtime::monotonic_now_ns();
    go.store(true, std::memory_order_release);

    producer.join();
    consumer.join();

    const std::uint64_t run_end_ns = sdr_runtime::monotonic_now_ns();
    const double elapsed_sec = static_cast<double>(run_end_ns - run_start_ns) / 1e9;
    const double total_samples = static_cast<double>(kTotalChunks) *
                                 static_cast<double>(sdr_runtime::kSamplesPerChunk);
    const double msps = (total_samples / elapsed_sec) / 1e6;

    std::cout << "=== SPSC Ring Buffer Benchmark ===\n"
              << "Chunks Transferred : " << kTotalChunks << "\n"
              << "Total IQ Samples   : " << static_cast<std::uint64_t>(total_samples) << "\n"
              << "Elapsed Time       : " << elapsed_sec << " s\n"
              << "Throughput         : " << msps << " MSPS\n"
              << "Max Transit Lat    : " << max_pop_ns.load(std::memory_order_acquire) << " ns\n";

    return 0;
}