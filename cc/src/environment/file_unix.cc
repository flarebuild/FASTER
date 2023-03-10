#include <fcntl.h>

#include "file_unix.h"

namespace FASTER {
namespace environment {

using namespace FASTER::core;

Status File::Open(int flags, FileCreateDisposition create_disposition, bool* exists) {
    if(exists) {
        *exists = false;
    }

    int create_flags = GetCreateDisposition(create_disposition);

    /// Always unbuffered (O_DIRECT).
    fd_ = ::open(filename_.c_str(), flags | O_RDWR | create_flags, S_IRUSR | S_IWUSR);

    if(exists) {
        // Let the caller know whether the file we tried to open or create (already) exists.
        if(create_disposition == FileCreateDisposition::CreateOrTruncate ||
           create_disposition == FileCreateDisposition::OpenOrCreate) {
            *exists = (errno == EEXIST);
        } else if(create_disposition == FileCreateDisposition::OpenExisting) {
            *exists = (errno != ENOENT);
            if(!*exists) {
                // The file doesn't exist. Don't return an error, since the caller is expecting this case.
                return Status::Ok;
            }
        }
    }
    if(fd_ == -1) {
        int error = errno;
        return Status::IOError;
    }

    Status result = GetDeviceAlignment();
    if(result != Status::Ok) {
        Close();
    }
    owner_ = true;
    return result;
}

Status File::Close() {
    if(fd_ != -1) {
        int result = ::close(fd_);
        fd_ = -1;
        if(result == -1) {
            int error = errno;
            return Status::IOError;
        }
    }
    owner_ = false;
    return Status::Ok;
}

Status File::Delete() {
    int result = ::remove(filename_.c_str());
    if(result == -1) {
        int error = errno;
        return Status::IOError;
    }
    return Status::Ok;
}

Status File::GetDeviceAlignment() {
    // For now, just hardcode 512-byte alignment.
    device_alignment_ = 512;
    return Status::Ok;
}

int File::GetCreateDisposition(FileCreateDisposition create_disposition) {
    switch(create_disposition) {
        case FileCreateDisposition::CreateOrTruncate:
            return O_CREAT | O_TRUNC;
        case FileCreateDisposition::OpenOrCreate:
            return O_CREAT;
        case FileCreateDisposition::OpenExisting:
            return 0;
        default:
            assert(false);
            return 0; // not reached
    }
}

}
} // namespace FASTER::environment