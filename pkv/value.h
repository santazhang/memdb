#pragma once

#include "utils.h"

namespace pkv {

class Value {
    union {
        i32 i32_;
        i64 i62_;
        double double_;
        std::string* p_str_;
    };

public:
    typedef enum {
        I32,
        I64,
        DOUBLE,
        STR
    } kind;
};

} // namespace pkv
