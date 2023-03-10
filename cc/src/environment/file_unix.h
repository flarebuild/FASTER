#pragma once

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../core/async.h"
#include "../core/status.h"
#include "file_common.h"

namespace FASTER {
namespace environment {

constexpr const char* kPathSeparator = "/";


/// The File class encapsulates the OS file handle.
class File {
protected:
    File()
        : fd_{ -1 }
        , device_alignment_{ 0 }
        , filename_{}
        , owner_{ false }
    {
    }

    File(const std::string& filename)
        : fd_{ -1 }
        , device_alignment_{ 0 }
        , filename_{ filename }
        , owner_{ false }
#ifdef IO_STATISTICS
    , bytes_written_ { 0 }
    , read_count_{ 0 }
    , bytes_read_{ 0 }
#endif
    {
    }

    ~File() {
        if(owner_) {
            core::Status s = Close();
        }
    }

    File(const File&) = delete;
    File &operator=(const File&) = delete;

    /// Move constructor.
    File(File&& other)
        : fd_{ other.fd_ }
        , device_alignment_{ other.device_alignment_ }
        , filename_{ std::move(other.filename_) }
        , owner_{ other.owner_ }
#ifdef IO_STATISTICS
    , bytes_written_ { other.bytes_written_ }
    , read_count_{ other.read_count_ }
    , bytes_read_{ other.bytes_read_ }
#endif
    {
        other.owner_ = false;
    }

    /// Move assignment operator.
    File& operator=(File&& other) {
        fd_ = other.fd_;
        device_alignment_ = other.device_alignment_;
        filename_ = std::move(other.filename_);
        owner_ = other.owner_;
#ifdef IO_STATISTICS
        bytes_written_ = other.bytes_written_;
    read_count_ = other.read_count_;
    bytes_read_ = other.bytes_read_;
#endif
        other.owner_ = false;
        return *this;
    }

protected:
    core::Status Open(int flags, FileCreateDisposition create_disposition, bool* exists = nullptr);

public:
    core::Status Close();
    core::Status Delete();

    uint64_t size() const {
        struct stat stat_buffer;
        int result = ::fstat(fd_, &stat_buffer);
        return (result == 0) ? stat_buffer.st_size : 0;
    }

    size_t device_alignment() const {
        return device_alignment_;
    }

    const std::string& filename() const {
        return filename_;
    }

#ifdef IO_STATISTICS
    uint64_t bytes_written() const {
    return bytes_written_.load();
  }
  uint64_t read_count() const {
    return read_count_.load();
  }
  uint64_t bytes_read() const {
    return bytes_read_.load();
  }
#endif

private:
    core::Status GetDeviceAlignment();
    static int GetCreateDisposition(FileCreateDisposition create_disposition);

protected:
    int fd_;

private:
    size_t device_alignment_;
    std::string filename_;
    bool owner_;

#ifdef IO_STATISTICS
    protected:
  std::atomic<uint64_t> bytes_written_;
  std::atomic<uint64_t> read_count_;
  std::atomic<uint64_t> bytes_read_;
#endif
};

}
} // namespace FASTER::environment