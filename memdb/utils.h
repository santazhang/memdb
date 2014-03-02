#pragma once

#include <map>
#include <string>
#include <unordered_map>

#include "base/all.h"

namespace mdb {

using base::i32;
using base::i64;
using base::NoCopy;
using base::Log;

template<class K, class V>
inline void insert_into_map(std::map<K, V>& dict, const K& key, const V& value) {
    dict.insert(typename std::map<K, V>::value_type(key, value));
}

template<class K, class V>
inline void insert_into_map(std::multimap<K, V>& dict, const K& key, const V& value) {
    dict.insert(typename std::multimap<K, V>::value_type(key, value));
}

template<class K, class V, class Hash, class Equal>
inline void insert_into_map(std::unordered_map<K, V, Hash, Equal>& dict, const K& key, const V& value) {
    dict.insert(typename std::unordered_map<K, V, Hash, Equal>::value_type(key, value));
}

template<class K, class V, class Hash, class Equal>
inline void insert_into_map(std::unordered_multimap<K, V, Hash, Equal>& dict, const K& key, const V& value) {
    dict.insert(typename std::unordered_multimap<K, V, Hash, Equal>::value_type(key, value));
}

uint32_t stringhash32(const void* data, int len);

inline uint32_t stringhash32(const std::string& str) {
    return stringhash32(&str[0], str.size());
}

} // namespace mdb
