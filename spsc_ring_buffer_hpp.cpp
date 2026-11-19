#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <type_traits>

namespace sdr_runtime {

// Cache line size assumption for x86_64 / ARM64 deterministic layout
inline constexpr std::size_t kCacheLineSize = 64;

// Lock-free Single-Producer Single-Consumer (SPSC) bounded ring buffer.
// Capacity must be a power of two greater than 1 so index masking is branch-cheap
// and one slot distinguishes full vs empty without extra locks.
template <typename T, std::size_t Capacity>
class SpscRingBuffer {
    static_assert(Capacity >= 2, "Capacity must be at least 2");
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");
    static_assert(std::is_trivially_copyable_v<T> || std::is_move_constructible_v<T>,
                  "T must be copyable or movable");

public:
    SpscRingBuffer() = default;

    SpscRingBuffer(const SpscRingBuffer&) = delete;
    SpscRingBuffer& operator=(const SpscRingBuffer&) = delete;

    // Called exclusively by the single producer thread.
    bool try_push(const T& item) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t next_tail = increment(tail);
        if (next_tail == head_.load(std::memory_order_acquire)) {
            return false; // Buffer full
        }
        buffer_[tail] = item;
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    bool try_push(T&& item) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t next_tail = increment(tail);
        if (next_tail == head_.load(std::memory_order_acquire)) {
            return false; // Buffer full
        }
        buffer_[tail] = static_cast<T&&>(item);
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    // Called exclusively by the single consumer thread.
    bool try_pop(T& out_item) noexcept {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        if (head == tail_.load(std::memory_order_acquire)) {
            return false; // Buffer empty
        }
        out_item = buffer_[head];
        head_.store(increment(head), std::memory_order_release);
        return true;
    }

    // Approximate current occupancy (safe for telemetry/monitoring).
    std::size_t size_approx() const noexcept {
        const std::size_t head = head_.load(std::memory_order_acquire);
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        if (tail >= head) {
            return tail - head;
        }
        return Capacity - (head - tail);
    }

    static constexpr std::size_t capacity() noexcept {
        return Capacity - 1;
    }

private:
    static constexpr std::size_t increment(std::size_t idx) noexcept {
        return (idx + 1) & (Capacity - 1);
    }

    alignas(kCacheLineSize) std::array<T, Capacity> buffer_{};
    alignas(kCacheLineSize) std::atomic<std::size_t> head_{0};
    alignas(kCacheLineSize) std::atomic<std::size_t> tail_{0};
};

} // namespace sdr_runtime