#pragma once

#include <inttypes.h>
#include <map>

namespace mdb {

typedef int64_t version_t;

template <class Key, class Value>
class snapshot_multimap {

    struct versioned_value {
        version_t ver;
        Value val;

        versioned_value(version_t _ver, const Value& _val): ver(_ver), val(_val) {}
    };

    std::multimap<Key, versioned_value> data_;
    version_t ver_;

public:

    snapshot_multimap(): ver_(0) {}

    void insert(const Key& key, const Value& value) {
        ver_++;
        versioned_value vv(ver_, value);
        insert_to_map(data_, key, vv);
    }
};

} // namespace mdb
