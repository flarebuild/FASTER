#pragma once

#include "environment/file_unix.h"

namespace FASTER {
namespace environment {

class KQueueFile;

class KQueueHandler {

public:
    typedef KQueueFile async_file_t;

    KQueueHandler() {}

    KQueueHandler(size_t max_threads) {

    }

    ~KQueueHandler() {}

    bool TryComplete();

};

class KQueueFile: public File {

public:
    KQueueFile()
    : File() {
    }

    KQueueFile(const std::string& filename)
    : File(filename)
    {}
    /// Move constructor
    KQueueFile(KQueueFile&& other)
    : File(std::move(other))
    {
    }
    /// Move assignment operator.
    KQueueFile& operator=(KQueueFile&& other) {
        File::operator=(std::move(other));
        return *this;
    }

    core::Status Open(FileCreateDisposition create_disposition, const FileOptions& options,
                      KQueueHandler* handler, bool* exists = nullptr);

    core::Status Read(size_t offset, uint32_t length, uint8_t* buffer,
                      core::IAsyncContext& context, core::AsyncIOCallback callback) const;
    core::Status Write(size_t offset, uint32_t length, const uint8_t* buffer,
                       core::IAsyncContext& context, core::AsyncIOCallback callback);

};

}
} // namespace FASTER::environment