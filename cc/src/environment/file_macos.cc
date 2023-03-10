#include "file_macos.h"

#include <fcntl.h>

namespace FASTER {
namespace environment {

using namespace FASTER::core;

bool KQueueHandler::TryComplete() {
    return false;
}

Status KQueueFile::Open(
    FileCreateDisposition create_disposition,
    const FileOptions& options,
   KQueueHandler* handler,
   bool* exists
) {
    int flags = 0;

    RETURN_NOT_OK(File::Open(flags, create_disposition, exists));
    if(exists && !*exists) {
        return Status::Ok;
    }
    if(options.unbuffered) {
        fcntl(fd_, F_NOCACHE, 1);
    }

    return Status::Ok;
}

Status KQueueFile::Read(
    size_t offset,
    uint32_t length,
    uint8_t* buffer,
    core::IAsyncContext& context,
    core::AsyncIOCallback callback
) const {

    return Status::Ok;

}

Status KQueueFile::Write(
    size_t offset, uint32_t
    length,
    const uint8_t* buffer,
    core::IAsyncContext& context,
    core::AsyncIOCallback callback
) {

    return Status::Ok;
}

}
} // namespace FASTER::environment
