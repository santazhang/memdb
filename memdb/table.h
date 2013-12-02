#pragma once

#include <string>
#include <list>
#include <unordered_map>

#include "value.h"
#include "row.h"
#include "schema.h"
#include "utils.h"
#include "blob.h"

namespace mdb {

class Table: public RefCounted {
public:
    typedef std::unordered_multimap<blob, Row*, blob::hash>::const_iterator iterator;

    Table(Schema* schema);

    void insert(Row* row);

    iterator begin() {
        return std::begin(rows_);
    }
    iterator end() {
        return std::end(rows_);
    }

    std::pair<iterator, iterator> query(const Value& key) {
        return query(key.get_blob());
    }
    std::pair<iterator, iterator> query(const blob& key) {
        return rows_.equal_range(key);
    }

    void remove(const Value& key) {
        remove(key.get_blob());
    }
    void remove(const blob& key_blob) {
        std::pair<iterator, iterator> query_range = query(key_blob);
        auto it = query_range.first;
        while (it != query_range.second) {
            it = remove(it);
        }
    }
    iterator remove(iterator it);

protected:
    // protected dtor as requried by RefCounted
    ~Table();

private:
    Schema* schema_;

    // indexed by key values
    std::unordered_multimap<blob, Row*, blob::hash> rows_;
};

} // namespace mdb
