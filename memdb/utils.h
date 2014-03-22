#pragma once

#include <map>
#include <string>
#include <unordered_map>

#include "base/all.h"

namespace mdb {

using base::i32;
using base::i64;
using base::NoCopy;
using base::RefCounted;
using base::Enumerator;
using base::Log;
using base::insert_into_map;
using base::format_decimal;

typedef enum {
    NONE,

    ROW_BASIC,
    ROW_COARSE,
    ROW_FINE,

    TBL_SORTED,
    TBL_UNSORTED,
    TBL_SNAPSHOT,

    TXN_UNSAFE,
    TXN_2PL,
    TXN_OCC,

    TXN_ABORT,
    TXN_COMMIT,
} symbol_t;

uint32_t stringhash32(const void* data, int len);

inline uint32_t stringhash32(const std::string& str) {
    return stringhash32(&str[0], str.size());
}

} // namespace mdb
