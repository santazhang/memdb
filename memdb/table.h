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
    virtual void remove(Row* row, bool do_free = true) = 0;
};

class SortedMultiKey {
    MultiBlob mb_;
    const Schema* schema_;
public:
    SortedMultiKey(const MultiBlob& mb, const Schema* schema): mb_(mb), schema_(schema) {
        verify(mb_.count() == (int) schema->key_columns_id().size());
    }

    // -1: this < o, 0: this == o, 1: this > o
    // UNKNOWN == UNKNOWN
    // both side should have same kind
    int compare(const SortedMultiKey& o) const;

    bool operator ==(const SortedMultiKey& o) const {
        return compare(o) == 0;
    }
    bool operator !=(const SortedMultiKey& o) const {
        return compare(o) != 0;
    }
    bool operator <(const SortedMultiKey& o) const {
        return compare(o) == -1;
    }
    bool operator >(const SortedMultiKey& o) const {
        return compare(o) == 1;
    }
    bool operator <=(const SortedMultiKey& o) const {
        return compare(o) != 1;
    }
    bool operator >=(const SortedMultiKey& o) const {
        return compare(o) != -1;
    }
};

class SortedTable: public Table {
    typedef std::multimap<SortedMultiKey, Row*>::const_iterator iterator;

public:

    class Cursor {
        iterator begin_, end_, next_;
        int count_;
    public:
        Cursor(const iterator& begin, const iterator& end): begin_(begin), end_(end), next_(begin), count_(-1) {}

        const iterator& begin() const {
            return begin_;
        }
        const iterator& end() const {
            return end_;
        }
        bool has_next() const {
            return next_ != end_;
        }
        operator bool () const {
            return has_next();
        }
        Row* next() {
            verify(next_ != end_);
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

    SortedTable(Schema* schema): schema_(schema) {}

    ~SortedTable();

    const Schema* schema() const {
        return schema_;
    }

    void insert(Row* row) {
        SortedMultiKey key = SortedMultiKey(row->get_key(), schema_);
        verify(row->schema() == schema_);
        row->set_table(this);
        insert_into_map(rows_, key, row);
    }

    Cursor query(const Value& kv) {
        return query(kv.get_blob());
    }
    Cursor query(const MultiBlob& mb) {
        return query(SortedMultiKey(mb, schema_));
    }
    Cursor query(const SortedMultiKey& smk) {
        auto range = rows_.equal_range(smk);
        return Cursor(range.first, range.second);
    }

    Cursor query_lt(const Value& kv) {
        return query_lt(kv.get_blob());
    }
    Cursor query_lt(const MultiBlob& mb) {
        return query_lt(SortedMultiKey(mb, schema_));
    }
    Cursor query_lt(const SortedMultiKey& smk) {
        auto bound = rows_.lower_bound(smk);
        return Cursor(rows_.begin(), bound);
    }

    Cursor query_gt(const Value& kv) {
        return query_gt(kv.get_blob());
    }
    Cursor query_gt(const MultiBlob& mb) {
        return query_gt(SortedMultiKey(mb, schema_));
    }
    Cursor query_gt(const SortedMultiKey& smk) {
        auto bound = rows_.upper_bound(smk);
        return Cursor(bound, rows_.end());
    }

    Cursor query_in(const Value& low, const Value& high) {
        assert(low < high);
        return query_in(low.get_blob(), high.get_blob());
    }
    Cursor query_in(const MultiBlob& low, const MultiBlob& high) {
        return query_in(SortedMultiKey(low, schema_), SortedMultiKey(high, schema_));
    }
    Cursor query_in(const SortedMultiKey& low, const SortedMultiKey& high) {
        auto low_bound = rows_.upper_bound(low);
        auto high_bound = rows_.lower_bound(high);
        return Cursor(low_bound, high_bound);
    }

    Cursor all() const {
        return Cursor(std::begin(rows_), std::end(rows_));
    }

    void clear();

    void remove(const Value& kv) {
        remove(kv.get_blob());
    }
    void remove(const MultiBlob& mb) {
        remove(SortedMultiKey(mb, schema_));
    }
    void remove(const SortedMultiKey& smk);
    void remove(Row* row, bool do_free = true);
    void remove(Cursor cur);

private:

    iterator remove(iterator it, bool do_free = true);

    const Schema* schema_;

    // indexed by key values
    std::multimap<SortedMultiKey, Row*> rows_;
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
            verify(next_ != end_);
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

    ~UnsortedTable();

    const Schema* schema() const {
        return schema_;
    }

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
    Cursor all() const {
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

    const Schema* schema_;

    // indexed by key values
    std::unordered_multimap<MultiBlob, Row*, MultiBlob::hash> rows_;
};

} // namespace mdb
