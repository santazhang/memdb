#pragma once

#include <map>
#include <unordered_map>

#include "base/all.h"

namespace pkv {

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

// reference to (part of) a string
struct str_ref {
    char* str;
    int len;
};

template<class K, class V>
inline void insert_into_map(std::map<K, V>& dict, const K& key, const V& value) {
    dict.insert(typename std::map<K, V>::value_type(key, value));
}

template<class K, class V>
inline void insert_into_map(std::unordered_map<K, V>& dict, const K& key, const V& value) {
    dict.insert(typename std::unordered_map<K, V>::value_type(key, value));
}

} // namespace pkv
