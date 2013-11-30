#pragma once

#include <map>
#include <string>
#include <unordered_map>

#include "base/all.h"

namespace mdb {

using base::i32;
using base::i64;
using base::f64;
using base::NoCopy;
using base::Lockable;
using base::SpinLock;
using base::Mutex;
using base::ScopedLock;
using base::CondVar;
using base::Log;
using base::RefCounted;
using base::Counter;
using base::Rand;

template<class K, class V>
inline void insert_into_map(std::map<K, V>& dict, const K& key, const V& value) {
    dict.insert(typename std::map<K, V>::value_type(key, value));
}

template<class K, class V, class Hash, class Equal>
inline void insert_into_map(std::unordered_map<K, V, Hash, Equal>& dict, const K& key, const V& value) {
    dict.insert(typename std::unordered_map<K, V, Hash, Equal>::value_type(key, value));
}

uint32_t stringhash32(const void* data, int len);

inline uint32_t stringhash32(const std::string& str) {
    return stringhash32(&str[0], str.size());
}

} // namespace mdb
