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

uint32_t stringhash32(const void* data, int len);

inline uint32_t stringhash32(const std::string& str) {
    return stringhash32(&str[0], str.size());
}

} // namespace mdb
