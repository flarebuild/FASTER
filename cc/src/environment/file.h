// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#if defined(_WIN32)
#include "file_windows.h"
#elif defined(__APPLE__)
#include "file_macos.h"
#elif defined(__linux__)
#include "file_linux.h"
#endif
