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

class UnsortedTable {
    typedef std::unordered_multimap<MultiBlob, Row*, MultiBlob::hash>::const_iterator iterator;

public:

    class Cursor {
        iterator begin_, end_, next_;
    public:
        Cursor(const iterator& begin, const iterator& end): begin_(begin), end_(end), next_(begin) {}

        bool has_next() const {
            return next_ != end_;
        }
        operator bool () const {
            return has_next();
        }
        Row* next() {
            Row* row = next_->second;
            ++next_;
            return row;
        }
    };

    UnsortedTable(Schema* schema): schema_(schema) {}

    ~UnsortedTable();

    void insert(Row* row) {
        MultiBlob key = row->get_key();
        insert_into_map(rows_, key, row);
    }

    Cursor query(const Value& kv) {
        return query(kv.get_blob());
    }
    Cursor query(const MultiBlob& key) {
        auto range = rows_.equal_range(key);
        return Cursor(range.first, range.second);
    }
    Cursor all() {
        return Cursor(std::begin(rows_), std::end(rows_));
    }

    void clear();

    void remove(const Value& kv) {
        remove(kv.get_blob());
    }
    void remove(const MultiBlob& key);
    void remove(Row* row, bool do_free = true);

private:

    iterator remove(iterator it, bool do_free = true);

    Schema* schema_;

    // indexed by key values
    std::unordered_multimap<MultiBlob, Row*, MultiBlob::hash> rows_;
};

} // namespace mdb
