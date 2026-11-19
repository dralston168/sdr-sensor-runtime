#include "sdr_runtime/telemetry_shm.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <new>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace sdr_runtime {

TelemetryShm::TelemetryShm(std::string shm_name)
    : name_(shm_name.empty() ? "/sdr_runtime_telemetry" :
            (shm_name[0] == '/' ? std::move(shm_name) : "/" + std::move(shm_name))) {}

TelemetryShm::~TelemetryShm() {
    cleanup();
}

bool TelemetryShm::init_creator() {
    shm_unlink(name_.c_str());

    fd_ = shm_open(name_.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd_ < 0) {
        std::cerr << "[telemetry_shm] shm_open failed for " << name_
                  << ": " << std::strerror(errno) << "\n";
        return false;
    }
    created_ = true;

    const off_t total_size = static_cast<off_t>(sizeof(TelemetryBlock));
    if (ftruncate(fd_, total_size) != 0) {
        std::cerr << "[telemetry_shm] ftruncate failed: " << std::strerror(errno) << "\n";
        cleanup();
        return false;
    }

    void* addr = mmap(nullptr,
                      static_cast<std::size_t>(total_size),
                      PROT_READ | PROT_WRITE,
                      MAP_SHARED,
                      fd_,
                      0);
    if (addr == MAP_FAILED) {
        std::cerr << "[telemetry_shm] mmap failed: " << std::strerror(errno) << "\n";
        cleanup();
        return false;
    }

    block_ = new (addr) TelemetryBlock();
    return true;
}

void TelemetryShm::cleanup() {
    if (block_ != nullptr) {
        block_->~TelemetryBlock();
        munmap(static_cast<void*>(block_), sizeof(TelemetryBlock));
        block_ = nullptr;
    }
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
    if (created_) {
        shm_unlink(name_.c_str());
        created_ = false;
    }
}

} // namespace sdr_runtime