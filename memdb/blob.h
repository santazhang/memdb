#pragma once

#include "utils.h"

namespace mdb {

struct blob {
    const char* data;
    int len;

    bool operator == (const blob& other) const {
        return (len == other.len) && (memcmp(data, other.data, len) == 0);
    }

    class hash {
    public:
        size_t operator() (const blob& b) const {
            return stringhash32(b.data, b.len);
        }
    };
};

} // namespace mdb

