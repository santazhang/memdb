#pragma once

#include <string>
#include <vector>
#include <map>
#include <set>

#include "utils.h"
#include "value.h"


namespace mdb {

// forward declaration
class Row;
class Table;
class UnsortedTable;
class SortedTable;
class SnapshotTable;
class TxnMgr;

typedef i64 txn_id_t;

class ResultSet: public Enumerator<Row*> {
    int* refcnt_;
    Enumerator<const Row*>* rows_;

    void decr_ref() {
        (*refcnt_)--;
        if (*refcnt_ == 0) {
            delete refcnt_;
            delete rows_;
        }
    }
public:
    ResultSet(Enumerator<const Row*>* rows): refcnt_(new int(1)), rows_(rows) {}
    ResultSet(const ResultSet& o): refcnt_(o.refcnt_), rows_(o.rows_) {
        (*refcnt_)++;
    }
    const ResultSet& operator =(const ResultSet& o) {
        if (this != &o) {
            decr_ref();
            refcnt_ = o.refcnt_;
            rows_ = o.rows_;
            (*refcnt_)++;
        }
        return *this;
    }
    ~ResultSet() {
        decr_ref();
    }

    bool has_next() {
        return rows_->has_next();
    }
    Row* next() {
        return const_cast<Row*>(rows_->next());
    }
};

class Txn: public NoCopy {
protected:
    const TxnMgr* mgr_;
    txn_id_t txnid_;
    Txn(const TxnMgr* mgr, txn_id_t txnid): mgr_(mgr),  txnid_(txnid) {}

public:
    txn_id_t id() const {
        return txnid_;
    }

    Table* get_table(const std::string& tbl_name) const;
    SortedTable* get_sorted_table(const std::string& tbl_name) const;
    UnsortedTable* get_unsorted_table(const std::string& tbl_name) const;
    SnapshotTable* get_snapshot_table(const std::string& tbl_name) const;

    virtual void abort() = 0;
    virtual bool commit() = 0;

    virtual bool read_column(Row* row, int col_id, Value* value) = 0;
    virtual bool write_column(Row* row, int col_id, const Value& value) = 0;
    virtual bool insert_row(Table* tbl, Row* row) = 0;
    virtual bool remove_row(Table* tbl, Row* row) = 0;

    bool read_columns(Row* row, const std::vector<int>& col_ids, std::vector<Value>* values) {
        for (auto col_id : col_ids) {
            Value v;
            if (read_column(row, col_id, &v)) {
                values->push_back(v);
            } else {
                return false;
            }
        }
        return true;
    }

    bool write_columns(Row* row, const std::vector<int>& col_ids, const std::vector<Value>& values) {
        verify(col_ids.size() == values.size());
        for (size_t i = 0; i < col_ids.size(); i++) {
            if (!write_column(row, col_ids[i], values[i])) {
                return false;
            }
        }
        return true;
    }

    ResultSet query(Table* tbl, const Value& kv) {
        return this->query(tbl, kv.get_blob());
    }
    virtual ResultSet query(Table* tbl, const MultiBlob& mb) = 0;
};


class TxnMgr: public NoCopy {
    std::map<std::string, Table*> tables_;

public:

    virtual Txn* start(txn_id_t txnid) = 0;

    void reg_table(const std::string& tbl_name, Table* tbl) {
        verify(tables_.find(tbl_name) == tables_.end());
        insert_into_map(tables_, tbl_name, tbl);
    }

    Table* get_table(const std::string& tbl_name) const {
        auto it = tables_.find(tbl_name);
        if (it == tables_.end()) {
            return nullptr;
        } else {
            return it->second;
        }
    }

    UnsortedTable* get_unsorted_table(const std::string& tbl_name) const;
    SortedTable* get_sorted_table(const std::string& tbl_name) const;
    SnapshotTable* get_snapshot_table(const std::string& tbl_name) const;
};


class TxnUnsafe: public Txn {
public:
    TxnUnsafe(const TxnMgr* mgr, txn_id_t txnid): Txn(mgr, txnid) {}
    void abort() {
        // do nothing
    }
    bool commit() {
        // always allowed
        return true;
    }
    virtual bool read_column(Row* row, int col_id, Value* value);
    virtual bool write_column(Row* row, int col_id, const Value& value);
    virtual bool insert_row(Table* tbl, Row* row);
    virtual bool remove_row(Table* tbl, Row* row);

    ResultSet query(Table* tbl, const MultiBlob& mb);
};

class TxnMgrUnsafe: public TxnMgr {
public:
    virtual Txn* start(txn_id_t txnid) {
        return new TxnUnsafe(this, txnid);
    }
};


class Txn2PL: public Txn {
    int outcome_;
    std::unordered_map<Row*, std::vector<std::pair<int, Value>>> updates_;
    std::set<std::pair<Table*, Row*>> inserts_;
    std::set<std::pair<Table*, Row*>> removes_;
    std::set<std::pair<Row*, int>> locks_;

    void relese_resource();

    bool debug_check_row_valid(Row* row) const {
        for (auto& it : removes_) {
            if (it.second == row) {
                return false;
            }
        }
        return true;
    }

public:

    Txn2PL(const TxnMgr* mgr, txn_id_t txnid): Txn(mgr, txnid), outcome_(symbol_t::NONE) {}
    ~Txn2PL();

    void abort();
    bool commit();
    virtual bool read_column(Row* row, int col_id, Value* value);
    virtual bool write_column(Row* row, int col_id, const Value& value);
    virtual bool insert_row(Table* tbl, Row* row);
    virtual bool remove_row(Table* tbl, Row* row);

    ResultSet query(Table* tbl, const MultiBlob& mb);
};

class TxnMgr2PL: public TxnMgr {
public:
    virtual Txn* start(txn_id_t txnid) {
        return new Txn2PL(this, txnid);
    }
};

} // namespace mdb
