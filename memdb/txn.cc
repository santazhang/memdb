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



bool TxnUnsafe::read_column(Row* row, int col_id, Value* value) {
    *value = row->get_column(col_id);
    // always allowed
    return true;
}

bool TxnUnsafe::write_column(Row* row, int col_id, const Value& value) {
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

bool table_row_pair::operator < (const table_row_pair& o) const {
    if (table != o.table) {
        return table < o.table;
    } else {
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
    }
}

Row* table_row_pair::ROW_MIN = (Row *) 0;
Row* table_row_pair::ROW_MAX = (Row *) ~0;

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
            int column_id = it.second;
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
        Row* row = it.first;
        for (auto& pair : it.second) {
            int column_id = pair.first;
            Value& value = pair.second;
            row->update(column_id, value);
        }
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

bool Txn2PL::read_column(Row* row, int col_id, Value* value) {
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    if (row->get_table() == nullptr) {
        // row not inserted into table, just read from staging area
        *value = row->get_column(col_id);
        return true;
    }

    auto it = updates_.find(row);
    if (it != updates_.end()) {
        // try reading from updates
        for (auto& pair : it->second) {
            if (pair.first == col_id) {
                *value = pair.second;
                return true;
            }
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

bool Txn2PL::write_column(Row* row, int col_id, const Value& value) {
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);

    if (row->get_table() == nullptr) {
        // row not inserted into table, just write to staging area
        row->update(col_id, value);
        return true;
    }

    auto it = updates_.find(row);
    if (it != updates_.end()) {
        // try rewriting updates
        for (auto& pair : it->second) {
            if (pair.first == col_id) {
                pair.second = value;
                return true;
            }
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
    updates_[row].push_back(make_pair(col_id, value));

    return true;
}

bool Txn2PL::insert_row(Table* tbl, Row* row) {
    verify(outcome_ == symbol_t::NONE);
    verify(row->get_table() == nullptr);
    inserts_.insert(table_row_pair(tbl, row));
    removes_.erase(table_row_pair(tbl, row));
    return true;
}

bool Txn2PL::remove_row(Table* tbl, Row* row) {
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);
    if (inserts_.find(table_row_pair(tbl, row)) == inserts_.end()) {
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
    }
    inserts_.erase(table_row_pair(tbl, row));
    updates_.erase(row);
    removes_.insert(table_row_pair(tbl, row));
    return true;
}


// merge query result in staging area and real table data
class MergedCursor: public NoCopy, public Enumerator<const Row*> {
    Table* tbl_;
    Enumerator<const Row*>* cursor_;

    const std::set<table_row_pair>& inserts_;
    std::set<table_row_pair>::const_iterator inserts_next_, inserts_end_;

    const std::unordered_set<table_row_pair, table_row_pair::hash>& removes_;

    bool cached_;
    const Row* cached_next_;
    const Row* next_candidate_;

    bool prefetch_next() {
        verify(cached_ == false);

        while (next_candidate_ == nullptr && cursor_->has_next()) {
            next_candidate_ = cursor_->next();

            // check if row has been removeds
            table_row_pair needle(tbl_, const_cast<Row*>(next_candidate_));
            if (removes_.find(needle) != removes_.end()) {
                next_candidate_ = nullptr;
            }
        }

        // check if there's data in inserts_
        if (next_candidate_ == nullptr) {
            if (inserts_next_ != inserts_end_) {
                cached_ = true;
                cached_next_ = inserts_next_->row;
                ++inserts_next_;
            }
        } else {
            // next_candidate_ != nullptr
            // check which is next: next_candidate_, or next in inserts_
            cached_ = true;
            if (inserts_next_ != inserts_end_) {
                if (next_candidate_ < inserts_next_->row) {
                    cached_next_ = next_candidate_;
                    next_candidate_ = nullptr;
                } else {
                    cached_next_ = inserts_next_->row;
                    ++inserts_next_;
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
                 const std::set<table_row_pair>& inserts,
                 const std::unordered_set<table_row_pair, table_row_pair::hash>& removes)
        : tbl_(tbl), cursor_(cursor), inserts_(inserts), removes_(removes),
          cached_(false), cached_next_(nullptr), next_candidate_(nullptr) {
              inserts_next_ = inserts_.lower_bound(table_row_pair(tbl, table_row_pair::ROW_MIN));
              inserts_end_ = inserts_.upper_bound(table_row_pair(tbl, table_row_pair::ROW_MAX));
        }

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
    if (tbl->rtti() == TBL_UNSORTED) {
        UnsortedTable* t = (UnsortedTable *) tbl;
        UnsortedTable::Cursor* cursor = new UnsortedTable::Cursor(t->query(mb));
        MergedCursor* merged_cursor = new MergedCursor(tbl, cursor, inserts_, removes_);
        return ResultSet(merged_cursor);
    } else if (tbl->rtti() == TBL_SORTED) {
        SortedTable* t = (SortedTable *) tbl;
        SortedTable::Cursor* cursor = new SortedTable::Cursor(t->query(mb));
        MergedCursor* merged_cursor = new MergedCursor(tbl, cursor, inserts_, removes_);
        return ResultSet(merged_cursor);
    } else if (tbl->rtti() == TBL_SNAPSHOT) {
        SnapshotTable* t = (SnapshotTable *) tbl;
        SnapshotTable::Cursor* cursor = new SnapshotTable::Cursor(t->query(mb));
        MergedCursor* merged_cursor = new MergedCursor(tbl, cursor, inserts_, removes_);
        return ResultSet(merged_cursor);
    } else {
        verify(tbl->rtti() == TBL_UNSORTED || tbl->rtti() == TBL_SORTED || tbl->rtti() == TBL_SNAPSHOT);
        return ResultSet(nullptr);
    }
}


} // namespace mdb
