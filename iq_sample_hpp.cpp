#pragma once

#include <cstddef>
#include <cstdint>

namespace sdr_runtime {

// Compact 8-byte complex I/Q sample aligned cleanly in contiguous arrays.
struct alignas(8) IqSample {
    float i{0.0f};
    float q{0.0f};
};

static_assert(sizeof(IqSample) == 8, "IqSample must be 8 bytes");

// Fixed-size burst chunk so producer and consumer exchange batches at ~2.4 MSPS.
// At 2.4 MSPS, 1024 samples per chunk = ~2343 chunks/sec (~426.67 us per chunk).
constexpr std::size_t kSamplesPerChunk = 1024;

struct alignas(64) IqChunk {
    std::uint64_t sequence_id{0};
    std::uint64_t timestamp_ns{0};
    std::uint32_t sample_count{0};
    std::uint32_t flags{0};
    IqSample samples[kSamplesPerChunk];
};

} // namespace sdr_runtime