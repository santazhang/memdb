#include <limits>

#include "row.h"
#include "table.h"
#include "txn.h"

using namespace std;

namespace mdb {

Table* Txn::get_table(const std::string& tbl_name) const {
    return mgr_->get_table(tbl_name);
}

SortedTable* Txn::get_sorted_table(const std::string& tbl_name) const {
    return mgr_->get_sorted_table(tbl_name);
}

UnsortedTable* Txn::get_unsorted_table(const std::string& tbl_name) const {
    return mgr_->get_unsorted_table(tbl_name);
}

SnapshotTable* Txn::get_snapshot_table(const std::string& tbl_name) const {
    return mgr_->get_snapshot_table(tbl_name);
}

ResultSet Txn::query_lt(Table* tbl, const MultiBlob& mb, symbol_t order /* =? */) {
    return query_lt(tbl, SortedMultiKey(mb, tbl->schema()), order);
}

ResultSet Txn::query_gt(Table* tbl, const MultiBlob& mb, symbol_t order /* =? */) {
    return query_gt(tbl, SortedMultiKey(mb, tbl->schema()), order);
}

ResultSet Txn::query_in(Table* tbl, const MultiBlob& low, const MultiBlob& high, symbol_t order /* =? */) {
    return query_in(tbl, SortedMultiKey(low, tbl->schema()), SortedMultiKey(high, tbl->schema()), order);
}


UnsortedTable* TxnMgr::get_unsorted_table(const std::string& tbl_name) const {
    Table* tbl = get_table(tbl_name);
    if (tbl != nullptr) {
        verify(tbl->rtti() == TBL_UNSORTED);
    }
    return (UnsortedTable *) tbl;
}

SortedTable* TxnMgr::get_sorted_table(const std::string& tbl_name) const {
    Table* tbl = get_table(tbl_name);
    if (tbl != nullptr) {
        verify(tbl->rtti() == TBL_SORTED);
    }
    return (SortedTable *) tbl;
}

SnapshotTable* TxnMgr::get_snapshot_table(const std::string& tbl_name) const {
    Table* tbl = get_table(tbl_name);
    if (tbl != nullptr) {
        verify(tbl->rtti() == TBL_SNAPSHOT);
    }
    return (SnapshotTable *) tbl;
}



bool TxnUnsafe::read_column(Row* row, column_id_t col_id, Value* value) {
    *value = row->get_column(col_id);
    // always allowed
    return true;
}

bool TxnUnsafe::write_column(Row* row, column_id_t col_id, const Value& value) {
    row->update(col_id, value);
    // always allowed
    return true;
}

bool TxnUnsafe::insert_row(Table* tbl, Row* row) {
    tbl->insert(row);
    // always allowed
    return true;
}

bool TxnUnsafe::remove_row(Table* tbl, Row* row) {
    tbl->remove(row);
    // always allowed
    return true;
}

ResultSet TxnUnsafe::query(Table* tbl, const MultiBlob& mb) {
    // always sendback query result from raw table
    if (tbl->rtti() == TBL_UNSORTED) {
        UnsortedTable* t = (UnsortedTable *) tbl;
        UnsortedTable::Cursor* cursor = new UnsortedTable::Cursor(t->query(mb));
        return ResultSet(cursor);
    } else if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->query(mb));
        return ResultSet(cursor);
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->query(mb));
        return ResultSet(cursor);
    } else {
        verify(tbl->rtti() == TBL_UNSORTED || tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
        return ResultSet(nullptr);
    }
}

ResultSet TxnUnsafe::query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order /* =? */) {
    // always sendback query result from raw table
    if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->query_lt(smk, order));
        return ResultSet(cursor);
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->query_lt(smk, order));
        return ResultSet(cursor);
    } else {
        // range query only works on sorted and snapshot table
        verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
        return ResultSet(nullptr);
    }
}

ResultSet TxnUnsafe::query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order /* =? */) {
    // always sendback query result from raw table
    if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->query_gt(smk, order));
        return ResultSet(cursor);
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->query_gt(smk, order));
        return ResultSet(cursor);
    } else {
        // range query only works on sorted and snapshot table
        verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
        return ResultSet(nullptr);
    }
}

ResultSet TxnUnsafe::query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order /* =? */) {
    // always sendback query result from raw table
    if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->query_in(low, high, order));
        return ResultSet(cursor);
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->query_in(low, high, order));
        return ResultSet(cursor);
    } else {
        // range query only works on sorted and snapshot table
        verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
        return ResultSet(nullptr);
    }
}


ResultSet TxnUnsafe::all(Table* tbl, symbol_t order /* =? */) {
    // always sendback query result from raw table
    if (tbl->rtti() == TBL_UNSORTED) {
        // unsorted tables only accept ORD_ANY
        verify(order == symbol_t::ORD_ANY);
        UnsortedTable* t = (UnsortedTable *) tbl;
        UnsortedTable::Cursor* cursor = new UnsortedTable::Cursor(t->all());
        return ResultSet(cursor);
    } else if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->all(order));
        return ResultSet(cursor);
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->all(order));
        return ResultSet(cursor);
    } else {
        verify(tbl->rtti() == TBL_UNSORTED || tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
        return ResultSet(nullptr);
    }
}

bool TableRowPair::operator < (const TableRowPair& o) const {
    if (table != o.table) {
        return table < o.table;
    } else {
        if (cmp_content_ && o.cmp_content_) {
            // we use ROW_MIN and ROW_MAX as special markers
            // this helps to get a range query on staged insert set
            if (row == ROW_MIN) {
                return o.row != ROW_MIN;
            } else if (row == ROW_MAX) {
                return false;
            } else if (o.row == ROW_MIN) {
                return false;
            } else if (o.row == ROW_MAX) {
                return row != ROW_MAX;
            }
            return (*row) < (*o.row);
        } else {
            // compare by Row* pointer
            return row < o.row;
        }
    }
}

Row* TableRowPair::ROW_MIN = (Row *) 0;
Row* TableRowPair::ROW_MAX = (Row *) ~0;

Txn2PL::~Txn2PL() {
    relese_resource();
}

void Txn2PL::relese_resource() {
    updates_.clear();
    inserts_.clear();
    removes_.clear();

    // unlocking
    for (auto& it : locks_) {
        Row* row = it.first;
        if (row->rtti() == ROW_COARSE) {
            assert(it.second == -1);
            ((CoarseLockedRow *) row)->unlock_row_by(this->id());
        } else if (row->rtti() == ROW_FINE) {
            column_id_t column_id = it.second;
            ((FineLockedRow *) row)->unlock_column_by(column_id, this->id());
        } else {
            // row must either be FineLockedRow or CoarseLockedRow
            verify(row->rtti() == symbol_t::ROW_COARSE || row->rtti() == symbol_t::ROW_FINE);
        }
    }
    locks_.clear();
}

void Txn2PL::abort() {
    verify(outcome_ == symbol_t::NONE);
    outcome_ = symbol_t::TXN_ABORT;
    relese_resource();
}

bool Txn2PL::commit() {
    verify(outcome_ == symbol_t::NONE);
    for (auto& it : inserts_) {
        it.table->insert(it.row);
    }
    for (auto& it : updates_) {
        // TODO update on snapshot table (remove then insert)
        Row* row = it.first;
        column_id_t column_id = it.second.first;
        Value& value = it.second.second;
        row->update(column_id, value);
    }
    for (auto& it : removes_) {
        // remove the locks since the row has gone already
        locks_.erase(it.row);
        it.table->remove(it.row);
    }
    outcome_ = symbol_t::TXN_COMMIT;
    relese_resource();
    return true;
}

bool Txn2PL::read_column(Row* row, column_id_t col_id, Value* value) {
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    if (row->get_table() == nullptr) {
        // row not inserted into table, just read from staging area
        *value = row->get_column(col_id);
        return true;
    }

    auto eq_range = updates_.equal_range(row);
    for (auto it = eq_range.first; it != eq_range.second; ++it) {
        if (it->second.first == col_id) {
            *value = it->second.second;
            return true;
        }
    }

    // reading from actual table data, needs locking
    if (row->rtti() == symbol_t::ROW_COARSE) {
        CoarseLockedRow* coarse_row = (CoarseLockedRow *) row;
        if (!coarse_row->rlock_row_by(this->id())) {
            return false;
        }
        insert_into_map(locks_, row, -1);
    } else if (row->rtti() == symbol_t::ROW_FINE) {
        FineLockedRow* fine_row = ((FineLockedRow *) row);
        if (!fine_row->rlock_column_by(col_id, this->id())) {
            return false;
        }
        insert_into_map(locks_, row, col_id);
    } else {
        // row must either be FineLockedRow or CoarseLockedRow
        verify(row->rtti() == symbol_t::ROW_COARSE || row->rtti() == symbol_t::ROW_FINE);
    }
    *value = row->get_column(col_id);

    return true;
}

bool Txn2PL::write_column(Row* row, column_id_t col_id, const Value& value) {
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    if (row->get_table() == nullptr) {
        // row not inserted into table, just write to staging area
        row->update(col_id, value);
        return true;
    }

    auto eq_range = updates_.equal_range(row);
    for (auto it = eq_range.first; it != eq_range.second; ++it) {
        if (it->second.first == col_id) {
            it->second.second = value;
            return true;
        }
    }

    // update staging area, needs locking
    if (row->rtti() == symbol_t::ROW_COARSE) {
        CoarseLockedRow* coarse_row = (CoarseLockedRow *) row;
        if (!coarse_row->wlock_row_by(this->id())) {
            return false;
        }
        insert_into_map(locks_, row, -1);
    } else if (row->rtti() == symbol_t::ROW_FINE) {
        FineLockedRow* fine_row = ((FineLockedRow *) row);
        if (!fine_row->wlock_column_by(col_id, this->id())) {
            return false;
        }
        insert_into_map(locks_, row, col_id);
    } else {
        // row must either be FineLockedRow or CoarseLockedRow
        verify(row->rtti() == symbol_t::ROW_COARSE || row->rtti() == symbol_t::ROW_FINE);
    }
    insert_into_map(updates_, row, make_pair(col_id, value));

    return true;
}

bool Txn2PL::insert_row(Table* tbl, Row* row) {
    verify(outcome_ == symbol_t::NONE);
    verify(row->get_table() == nullptr);
    inserts_.insert(TableRowPair(tbl, row));
    removes_.erase(TableRowPair(tbl, row));
    return true;
}

bool Txn2PL::remove_row(Table* tbl, Row* row) {
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);
    // NOTE: finding row by pointer value instead of its content
    auto it = inserts_.find(TableRowPair(tbl, row, false));
    if (it == inserts_.end()) {
        // lock whole row, only if row is on real table
        if (row->rtti() == symbol_t::ROW_COARSE) {
            CoarseLockedRow* coarse_row = (CoarseLockedRow *) row;
            if (!coarse_row->wlock_row_by(this->id())) {
                return false;
            }
            insert_into_map(locks_, row, -1);
        } else if (row->rtti() == symbol_t::ROW_FINE) {
            FineLockedRow* fine_row = ((FineLockedRow *) row);
            for (size_t col_id = 0; col_id < row->schema()->columns_count(); col_id++) {
                if (!fine_row->wlock_column_by(col_id, this->id())) {
                    return false;
                }
                insert_into_map(locks_, row, col_id);
            }
        } else {
            // row must either be FineLockedRow or CoarseLockedRow
            verify(row->rtti() == symbol_t::ROW_COARSE || row->rtti() == symbol_t::ROW_FINE);
        }
    } else {
        delete it->row;
        inserts_.erase(it);
    }
    updates_.erase(row);
    removes_.insert(TableRowPair(tbl, row));
    return true;
}


// for helping locating in inserts set
class KeyOnlySearchRow: public Row {
    const MultiBlob* mb_;
public:
    KeyOnlySearchRow(const Schema* schema, const MultiBlob* mb): mb_(mb) {
        schema_ = schema;
    }
    virtual MultiBlob get_key() const {
        return *mb_;
    }
};


// merge query result in staging area and real table data
class MergedCursor: public NoCopy, public Enumerator<const Row*> {
    Table* tbl_;
    Enumerator<const Row*>* cursor_;

    bool reverse_order_;
    std::multiset<TableRowPair>::const_iterator inserts_next_, inserts_end_;
    std::multiset<TableRowPair>::const_reverse_iterator r_inserts_next_, r_inserts_end_;

    const std::unordered_set<TableRowPair, TableRowPair::hash>& removes_;

    bool cached_;
    const Row* cached_next_;
    const Row* next_candidate_;

    bool insert_has_next() {
        if (reverse_order_) {
            return r_inserts_next_ != r_inserts_end_;
        } else {
            return inserts_next_ != inserts_end_;
        }
    }

    const Row* insert_get_next() {
        if (reverse_order_) {
            return r_inserts_next_->row;
        } else {
            return inserts_next_->row;
        }
    }

    void insert_advance_next() {
        if (reverse_order_) {
            ++r_inserts_next_;
        } else {
            ++inserts_next_;
        }
    }

    bool prefetch_next() {
        verify(cached_ == false);

        while (next_candidate_ == nullptr && cursor_->has_next()) {
            next_candidate_ = cursor_->next();

            // check if row has been removeds
            TableRowPair needle(tbl_, const_cast<Row*>(next_candidate_));
            if (removes_.find(needle) != removes_.end()) {
                next_candidate_ = nullptr;
            }
        }

        // check if there's data in inserts_
        if (next_candidate_ == nullptr) {
            if (insert_has_next()) {
                cached_ = true;
                cached_next_ = insert_get_next();
                insert_advance_next();
            }
        } else {
            // next_candidate_ != nullptr
            // check which is next: next_candidate_, or next in inserts_
            cached_ = true;
            if (insert_has_next()) {
                if (next_candidate_ < insert_get_next()) {
                    cached_next_ = next_candidate_;
                    next_candidate_ = nullptr;
                } else {
                    cached_next_ = insert_get_next();
                    insert_advance_next();
                }
            } else {
                cached_next_ = next_candidate_;
                next_candidate_ = nullptr;
            }
        }

        return cached_;
    }

public:
    MergedCursor(Table* tbl,
                 Enumerator<const Row*>* cursor,
                 const std::multiset<TableRowPair>::const_iterator& inserts_begin,
                 const std::multiset<TableRowPair>::const_iterator& inserts_end,
                 const std::unordered_set<TableRowPair, TableRowPair::hash>& removes)
        : tbl_(tbl), cursor_(cursor), reverse_order_(false),
          inserts_next_(inserts_begin), inserts_end_(inserts_end), removes_(removes),
          cached_(false), cached_next_(nullptr), next_candidate_(nullptr) {}

    MergedCursor(Table* tbl,
                 Enumerator<const Row*>* cursor,
                 const std::multiset<TableRowPair>::const_reverse_iterator& inserts_rbegin,
                 const std::multiset<TableRowPair>::const_reverse_iterator& inserts_rend,
                 const std::unordered_set<TableRowPair, TableRowPair::hash>& removes)
        : tbl_(tbl), cursor_(cursor), reverse_order_(true),
          r_inserts_next_(inserts_rbegin), r_inserts_end_(inserts_rend), removes_(removes),
          cached_(false), cached_next_(nullptr), next_candidate_(nullptr) {}

    ~MergedCursor() {
        delete cursor_;
    }

    bool has_next() {
        if (cached_) {
            return true;
        } else {
            return prefetch_next();
        }
    }

    const Row* next() {
        if (!cached_) {
            verify(prefetch_next());
        }
        cached_ = false;
        return cached_next_;
    }
};


ResultSet Txn2PL::query(Table* tbl, const MultiBlob& mb) {
    MergedCursor* merged_cursor = nullptr;
    KeyOnlySearchRow key_search_row(tbl->schema(), &mb);

    // NOTE: finding row by its content instead of pointer value
    auto inserts_begin = inserts_.lower_bound(TableRowPair(tbl, &key_search_row));
    auto inserts_end = inserts_.upper_bound(TableRowPair(tbl, &key_search_row));

    if (tbl->rtti() == TBL_UNSORTED) {
        UnsortedTable* t = (UnsortedTable *) tbl;
        UnsortedTable::Cursor* cursor = new UnsortedTable::Cursor(t->query(mb));
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
    } else if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->query(mb));
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->query(mb));
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
    } else {
        verify(tbl->rtti() == TBL_UNSORTED || tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
    }

    return ResultSet(merged_cursor);
}


ResultSet Txn2PL::query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order /* =? */) {
    verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);

    MergedCursor* merged_cursor = nullptr;
    KeyOnlySearchRow key_search_row(tbl->schema(), &smk.get_multi_blob());

    // NOTE: finding row by its content instead of pointer value
    auto inserts_begin = inserts_.lower_bound(TableRowPair(tbl, TableRowPair::ROW_MIN));
    auto inserts_end = inserts_.lower_bound(TableRowPair(tbl, &key_search_row));

    if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->query_lt(smk, order));

        if (order == symbol_t::ORD_DESC) {
            auto inserts_rbegin = std::multiset<TableRowPair>::const_reverse_iterator(inserts_end);
            auto inserts_rend = std::multiset<TableRowPair>::const_reverse_iterator(inserts_begin);
            merged_cursor = new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);

        } else {
            merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
        }

    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        // TODO ordering for snapshot table

        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->query_lt(smk, order));
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);

    } else {
        // range query only works on sorted and snapshot table
        verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
    }

    return ResultSet(merged_cursor);
}

ResultSet Txn2PL::query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order /* =? */) {
    verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);

    MergedCursor* merged_cursor = nullptr;
    KeyOnlySearchRow key_search_row(tbl->schema(), &smk.get_multi_blob());

    // NOTE: finding row by its content instead of pointer value
    auto inserts_begin = inserts_.upper_bound(TableRowPair(tbl, &key_search_row));
    auto inserts_end = inserts_.upper_bound(TableRowPair(tbl, TableRowPair::ROW_MAX));

    if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->query_gt(smk, order));

        if (order == symbol_t::ORD_DESC) {
            auto inserts_rbegin = std::multiset<TableRowPair>::const_reverse_iterator(inserts_end);
            auto inserts_rend = std::multiset<TableRowPair>::const_reverse_iterator(inserts_begin);
            merged_cursor = new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);

        } else {
            merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
        }

    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        // TODO ordering for snapshot table

        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->query_gt(smk, order));
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);

    } else {
        // range query only works on sorted and snapshot table
        verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
    }

    return ResultSet(merged_cursor);
}

ResultSet Txn2PL::query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order /* =? */) {
    verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);

    MergedCursor* merged_cursor = nullptr;
    KeyOnlySearchRow key_search_row_low(tbl->schema(), &low.get_multi_blob());
    KeyOnlySearchRow key_search_row_high(tbl->schema(), &high.get_multi_blob());

    // NOTE: finding row by its content instead of pointer value
    auto inserts_begin = inserts_.upper_bound(TableRowPair(tbl, &key_search_row_low));
    auto inserts_end = inserts_.lower_bound(TableRowPair(tbl, &key_search_row_high));

    if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->query_in(low, high, order));

        if (order == symbol_t::ORD_DESC) {
            auto inserts_rbegin = std::multiset<TableRowPair>::const_reverse_iterator(inserts_end);
            auto inserts_rend = std::multiset<TableRowPair>::const_reverse_iterator(inserts_begin);
            merged_cursor = new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);

        } else {
            merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
        }

    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        // TODO ordering for snapshot table

        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->query_in(low, high, order));
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);

    } else {
        // range query only works on sorted and snapshot table
        verify(tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
    }

    return ResultSet(merged_cursor);
}


ResultSet Txn2PL::all(Table* tbl, symbol_t order /* =? */) {
    verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);
    MergedCursor* merged_cursor = nullptr;

    // NOTE: finding row by its content instead of pointer value
    auto inserts_begin = inserts_.lower_bound(TableRowPair(tbl, TableRowPair::ROW_MIN));
    auto inserts_end = inserts_.upper_bound(TableRowPair(tbl, TableRowPair::ROW_MAX));

    if (tbl->rtti() == TBL_UNSORTED) {
        // unsorted tables only accept ORD_ANY
        verify(order == symbol_t::ORD_ANY);
        UnsortedTable* t = (UnsortedTable *) tbl;
        UnsortedTable::Cursor* cursor = new UnsortedTable::Cursor(t->all());
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);

    } else if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->all(order));
        if (order == symbol_t::ORD_DESC) {
            auto inserts_rbegin = std::multiset<TableRowPair>::const_reverse_iterator(inserts_end);
            auto inserts_rend = std::multiset<TableRowPair>::const_reverse_iterator(inserts_begin);
            merged_cursor = new MergedCursor(tbl, cursor, inserts_rbegin, inserts_rend, removes_);
        } else {
            merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);
        }

    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        // TODO ordering for snapshot table
        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->all(order));
        merged_cursor = new MergedCursor(tbl, cursor, inserts_begin, inserts_end, removes_);

    } else {
        verify(tbl->rtti() == TBL_UNSORTED || tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
    }

    return ResultSet(merged_cursor);
}



bool TxnOCC::commit() {
    verify(outcome_ == symbol_t::NONE);

    // TODO handle the case where the row being checked has already been deleted in other transactions
    // do version checks
    for (auto& it : ver_check_) {
        Row* row = it.first.row;
        column_id_t col_id = it.first.col_id;
        version_t ver = it.second;
        verify(row->rtti() == ROW_VERSIONED);
        VersionedRow* v_row = (VersionedRow *) row;
        if (v_row->get_column_ver(col_id) != ver) {
            return false;
        }
    }

    for (auto& it : inserts_) {
        it.table->insert(it.row);
    }
    for (auto& it : updates_) {
        // TODO update on snapshot table (remove then insert)
        Row* row = it.first;
        column_id_t column_id = it.second.first;
        Value& value = it.second.second;
        row->update(column_id, value);
    }
    for (auto& it : removes_) {
        // remove the locks since the row has gone already
        locks_.erase(it.row);
        it.table->remove(it.row);
    }
    outcome_ = symbol_t::TXN_COMMIT;
    relese_resource();
    return true;
}


bool TxnOCC::read_column(Row* row, column_id_t col_id, Value* value) {
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    if (row->get_table() == nullptr) {
        // row not inserted into table, just read from staging area
        *value = row->get_column(col_id);
        return true;
    }

    auto eq_range = updates_.equal_range(row);
    for (auto it = eq_range.first; it != eq_range.second; ++it) {
        if (it->second.first == col_id) {
            *value = it->second.second;
            return true;
        }
    }

    // reading from actual table data, track version
    if (row->rtti() == symbol_t::ROW_VERSIONED) {
        VersionedRow* v_row = (VersionedRow *) row;
        insert_into_map(ver_check_, row_column_pair(v_row, col_id), v_row->get_column_ver(col_id));

    } else {
        verify(row->rtti() == symbol_t::ROW_VERSIONED);
    }
    *value = row->get_column(col_id);

    return true;
}

bool TxnOCC::write_column(Row* row, column_id_t col_id, const Value& value) {
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    if (row->get_table() == nullptr) {
        // row not inserted into table, just write to staging area
        row->update(col_id, value);
        return true;
    }

    auto eq_range = updates_.equal_range(row);
    for (auto it = eq_range.first; it != eq_range.second; ++it) {
        if (it->second.first == col_id) {
            it->second.second = value;
            return true;
        }
    }

    // update staging area, track version
    if (row->rtti() == symbol_t::ROW_VERSIONED) {
        VersionedRow* v_row = (VersionedRow *) row;
        v_row->incr_column_ver(col_id);
        insert_into_map(ver_check_, row_column_pair(v_row, col_id), v_row->get_column_ver(col_id));

    } else {
        // row must either be FineLockedRow or CoarseLockedRow
        verify(row->rtti() == symbol_t::ROW_VERSIONED);
    }
    insert_into_map(updates_, row, make_pair(col_id, value));

    return true;
}

bool TxnOCC::insert_row(Table* tbl, Row* row) {
    verify(outcome_ == symbol_t::NONE);
    verify(row->rtti() == symbol_t::ROW_VERSIONED);
    verify(row->get_table() == nullptr);
    inserts_.insert(TableRowPair(tbl, row));
    removes_.erase(TableRowPair(tbl, row));
    return true;
}

bool TxnOCC::remove_row(Table* tbl, Row* row) {
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    // TODO handle the case where deleted rows are also beening accessed by other transactions

    // NOTE: finding row by pointer value instead of its content
    auto it = inserts_.find(TableRowPair(tbl, row, false));
    if (it == inserts_.end()) {
        if (row->rtti() == symbol_t::ROW_VERSIONED) {
            VersionedRow* v_row = (VersionedRow *) row;

            // remember whole row version
            for (size_t col_id = 0; col_id < v_row->schema()->columns_count(); col_id++) {
                v_row->incr_column_ver(col_id);
                insert_into_map(ver_check_, row_column_pair(v_row, col_id), v_row->get_column_ver(col_id));
            }

        } else {
            // row must either be FineLockedRow or CoarseLockedRow
            verify(row->rtti() == symbol_t::ROW_VERSIONED);
        }
    } else {
        delete it->row;
        inserts_.erase(it);
    }
    updates_.erase(row);
    removes_.insert(TableRowPair(tbl, row));
    return true;
}

} // namespace mdb
