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
    typedef std::unordered_multimap<MultiBlob, Row*, MultiBlob::hash>::const_iterator iterator;

    Table(Schema* schema): schema_(schema) {}

    void insert(Row* row) {
        MultiBlob key = row->get_key();
        insert_into_map(rows_, key, (Row *) row->ref_copy());
    }

    iterator begin() {
        return std::begin(rows_);
    }
    iterator end() {
        return std::end(rows_);
    }

    std::pair<iterator, iterator> query(const Value& kv) {
        return query(kv.get_blob());
    }
    std::pair<iterator, iterator> query(const MultiBlob& key) {
        return rows_.equal_range(key);
    }

    void remove(const Value& kv) {
        remove(kv.get_blob());
    }
    void remove(const MultiBlob& key) {
        std::pair<iterator, iterator> query_range = query(key);
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
    std::unordered_multimap<MultiBlob, Row*, MultiBlob::hash> rows_;
};

} // namespace mdb
