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

class Table {
public:
    virtual ~Table() {}
    virtual void insert(Row* row) = 0;
};

class UnsortedTable: public Table {
    typedef std::unordered_multimap<MultiBlob, Row*, MultiBlob::hash>::const_iterator iterator;

public:

    class Cursor {
        iterator begin_, end_, next_;
        int count_;
    public:
        Cursor(const iterator& begin, const iterator& end): begin_(begin), end_(end), next_(begin), count_(-1) {}

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
        int count() {
            if (count_ < 0) {
                count_ = 0;
                for (auto it = begin_; it != end_; ++it) {
                    count_++;
                }
            }
            return count_;
        }
    };

    UnsortedTable(Schema* schema): schema_(schema) {}

    virtual ~UnsortedTable();

    void insert(Row* row) {
        MultiBlob key = row->get_key();
        verify(row->schema() == schema_);
        row->set_table(this);
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
