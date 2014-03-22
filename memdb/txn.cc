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
        Table* tbl = it.first;
        Row* row = it.second;
        tbl->insert(row);
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
        Table* tbl = it.first;
        Row* row = it.second;
        tbl->remove(row);
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
        locks_.insert(make_pair(row, -1));
    } else if (row->rtti() == symbol_t::ROW_FINE) {
        FineLockedRow* fine_row = ((FineLockedRow *) row);
        if (!fine_row->rlock_column_by(col_id, this->id())) {
            return false;
        }
        locks_.insert(make_pair(row, col_id));
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
        locks_.insert(make_pair(row, -1));
    } else if (row->rtti() == symbol_t::ROW_FINE) {
        FineLockedRow* fine_row = ((FineLockedRow *) row);
        if (!fine_row->wlock_column_by(col_id, this->id())) {
            return false;
        }
        locks_.insert(make_pair(row, col_id));
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
    inserts_.insert(make_pair(tbl, row));
    removes_.erase(make_pair(tbl, row));
    return true;
}

bool Txn2PL::remove_row(Table* tbl, Row* row) {
    assert(debug_check_row_valid(row));
    verify(outcome_ == symbol_t::NONE);
    if (inserts_.find(make_pair(tbl, row)) == inserts_.end()) {
        // lock whole row, only if row is on real table
        if (row->rtti() == symbol_t::ROW_COARSE) {
            CoarseLockedRow* coarse_row = (CoarseLockedRow *) row;
            if (!coarse_row->wlock_row_by(this->id())) {
                return false;
            }
            locks_.insert(make_pair(row, -1));
        } else if (row->rtti() == symbol_t::ROW_FINE) {
            FineLockedRow* fine_row = ((FineLockedRow *) row);
            for (size_t col_id = 0; col_id < row->schema()->columns_count(); col_id++) {
                if (!fine_row->wlock_column_by(col_id, this->id())) {
                    return false;
                }
                locks_.insert(make_pair(row, col_id));
            }
        } else {
            // row must either be FineLockedRow or CoarseLockedRow
            verify(row->rtti() == symbol_t::ROW_COARSE || row->rtti() == symbol_t::ROW_FINE);
        }
    }
    inserts_.erase(make_pair(tbl, row));
    updates_.erase(row);
    removes_.insert(make_pair(tbl, row));
    return true;
}


} // namespace mdb
