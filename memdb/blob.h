#pragma once

#include "utils.h"

namespace mdb {

struct blob {
    const char* data;
    int len;

    class hash {
    public:
        size_t operator() (const blob& b) const {
            return stringhash32(b.data, b.len);
        }
    };

    class equal {
    public:
        bool operator() (const blob& b1, const blob& b2) const {
            return (b1.len == b2.len) && (memcmp(b1.data, b2.data, b1.len) == 0);
        }
    };
};

} // namespace mdb

