#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <unordered_set>
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
class SortedMultiKey;

typedef i64 txn_id_t;

// forward declaration
class TxnNested;

class ResultSet: public Enumerator<Row*> {
    friend class TxnNested;

    int* refcnt_;
    bool unboxed_;
    Enumerator<const Row*>* rows_;

    void decr_ref() {
        (*refcnt_)--;
        if (*refcnt_ == 0) {
            delete refcnt_;
            if (!unboxed_) {
                delete rows_;
            }
        }
    }

    // only called by TxnNested
    Enumerator<const Row*>* unbox() {
        verify(!unboxed_);
        unboxed_ = true;
        return rows_;
    }

public:
    ResultSet(Enumerator<const Row*>* rows): refcnt_(new int(1)), unboxed_(false), rows_(rows) {}
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
    Txn(const TxnMgr* mgr, txn_id_t txnid): mgr_(mgr), txnid_(txnid) {}

public:
    virtual ~Txn() {}
    virtual symbol_t rtti() const = 0;
    txn_id_t id() const {
        return txnid_;
    }

    Table* get_table(const std::string& tbl_name) const;
    SortedTable* get_sorted_table(const std::string& tbl_name) const;
    UnsortedTable* get_unsorted_table(const std::string& tbl_name) const;
    SnapshotTable* get_snapshot_table(const std::string& tbl_name) const;

    virtual void abort() = 0;
    virtual bool commit() = 0;

    bool commit_or_abort() {
        bool ret = commit();
        if (!ret) {
            abort();
        }
        return ret;
    }

    virtual bool read_column(Row* row, column_id_t col_id, Value* value) = 0;
    virtual bool write_column(Row* row, column_id_t col_id, const Value& value) = 0;
    virtual bool insert_row(Table* tbl, Row* row) = 0;
    virtual bool remove_row(Table* tbl, Row* row) = 0;

    bool read_columns(Row* row, const std::vector<column_id_t>& col_ids, std::vector<Value>* values) {
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

    bool write_columns(Row* row, const std::vector<column_id_t>& col_ids, const std::vector<Value>& values) {
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

    ResultSet query_lt(Table* tbl, const Value& kv, symbol_t order = symbol_t::ORD_ASC) {
        return query_lt(tbl, kv.get_blob(), order);
    }
    ResultSet query_lt(Table* tbl, const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC);
    virtual ResultSet query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) = 0;

    ResultSet query_gt(Table* tbl, const Value& kv, symbol_t order = symbol_t::ORD_ASC) {
        return query_gt(tbl, kv.get_blob(), order);
    }
    ResultSet query_gt(Table* tbl, const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC);
    virtual ResultSet query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) = 0;

    ResultSet query_in(Table* tbl, const Value& low, const Value& high, symbol_t order = symbol_t::ORD_ASC) {
        return query_in(tbl, low.get_blob(), high.get_blob(), order);
    }
    ResultSet query_in(Table* tbl, const MultiBlob& low, const MultiBlob& high, symbol_t order = symbol_t::ORD_ASC);
    virtual ResultSet query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order = symbol_t::ORD_ASC) = 0;

    virtual ResultSet all(Table* tbl, symbol_t order = symbol_t::ORD_ANY) = 0;
};


class TxnMgr: public NoCopy {
    std::map<std::string, Table*> tables_;

public:

    virtual ~TxnMgr() {}
    virtual symbol_t rtti() const = 0;
    virtual Txn* start(txn_id_t txnid) = 0;
    Txn* start_nested(Txn* base);

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
    virtual symbol_t rtti() const {
        return symbol_t::TXN_UNSAFE;
    }
    void abort() {
        // do nothing
    }
    bool commit() {
        // always allowed
        return true;
    }
    virtual bool read_column(Row* row, column_id_t col_id, Value* value);
    virtual bool write_column(Row* row, column_id_t col_id, const Value& value);
    virtual bool insert_row(Table* tbl, Row* row);
    virtual bool remove_row(Table* tbl, Row* row);

    ResultSet query(Table* tbl, const MultiBlob& mb);

    virtual ResultSet query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC);
    virtual ResultSet query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC);
    virtual ResultSet query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order = symbol_t::ORD_ASC);

    virtual ResultSet all(Table* tbl, symbol_t order = symbol_t::ORD_ANY);
};

class TxnMgrUnsafe: public TxnMgr {
public:
    virtual Txn* start(txn_id_t txnid) {
        return new TxnUnsafe(this, txnid);
    }
    virtual symbol_t rtti() const {
        return symbol_t::TXN_UNSAFE;
    }
};


struct table_row_pair {
    Table* table;
    Row* row;

    table_row_pair(Table* t, Row* r): table(t), row(r) {}

    // NOTE: used by set, to do range query in insert_ set
    bool operator < (const table_row_pair& o) const;

    // NOTE: only used by unsorted_set, to lookup in removes_ set
    bool operator == (const table_row_pair& o) const {
        return table == o.table && row == o.row;
    }

    struct hash {
        size_t operator() (const table_row_pair& p) const {
            size_t v1 = size_t(p.table);
            size_t v2 = size_t(p.row);
            return inthash64(v1, v2);
        }
    };

    static Row* ROW_MIN;
    static Row* ROW_MAX;
};


class Txn2PL: public Txn {

    void release_resource();

protected:

    symbol_t outcome_;
    std::multimap<Row*, std::pair<column_id_t, Value>> updates_;
    std::multiset<table_row_pair> inserts_;
    std::unordered_set<table_row_pair, table_row_pair::hash> removes_;
    std::unordered_multimap<Row*, column_id_t> locks_;

    bool debug_check_row_valid(Row* row) const {
        for (auto& it : removes_) {
            if (it.row == row) {
                return false;
            }
        }
        return true;
    }

    ResultSet do_query(Table* tbl, const MultiBlob& mb);

    ResultSet do_query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC);
    ResultSet do_query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC);
    ResultSet do_query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order = symbol_t::ORD_ASC);

    ResultSet do_all(Table* tbl, symbol_t order = symbol_t::ORD_ANY);

public:

    Txn2PL(const TxnMgr* mgr, txn_id_t txnid): Txn(mgr, txnid), outcome_(symbol_t::NONE) {}
    ~Txn2PL();

    virtual symbol_t rtti() const {
        return symbol_t::TXN_2PL;
    }

    void abort();
    bool commit();
    virtual bool read_column(Row* row, column_id_t col_id, Value* value);
    virtual bool write_column(Row* row, column_id_t col_id, const Value& value);
    virtual bool insert_row(Table* tbl, Row* row);
    virtual bool remove_row(Table* tbl, Row* row);

    ResultSet query(Table* tbl, const MultiBlob& mb) {
        return do_query(tbl, mb);
    }
    virtual ResultSet query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) {
        return do_query_lt(tbl, smk, order);
    }
    virtual ResultSet query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) {
        return do_query_gt(tbl, smk, order);
    }
    virtual ResultSet query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order = symbol_t::ORD_ASC) {
        return do_query_in(tbl, low, high, order);
    }
    virtual ResultSet all(Table* tbl, symbol_t order = symbol_t::ORD_ANY) {
        return do_all(tbl, order);
    }
};

class TxnMgr2PL: public TxnMgr {
    std::multimap<Row*, std::pair<column_id_t, version_t>> vers_;
public:
    virtual Txn* start(txn_id_t txnid) {
        return new Txn2PL(this, txnid);
    }
    virtual symbol_t rtti() const {
        return symbol_t::TXN_2PL;
    }
};


struct row_column_pair {
    Row* row;
    column_id_t col_id;

    row_column_pair(Row* r, column_id_t c): row(r), col_id(c) {}

    bool operator == (const row_column_pair& o) const {
        return row == o.row && col_id == o.col_id;
    }

    struct hash {
        size_t operator() (const row_column_pair& p) const {
            size_t v1 = size_t(p.row);
            size_t v2 = size_t(p.col_id);
            return inthash64(v1, v2);
        }
    };
};


class TxnOCC: public Txn2PL {
    // when ever a read/write is performed, record its version
    // check at commit time if all version values are not changed
    std::unordered_map<row_column_pair, version_t, row_column_pair::hash> ver_check_read_;
    std::unordered_map<row_column_pair, version_t, row_column_pair::hash> ver_check_write_;

    // incr refcount on a Row whenever it gets accessed
    std::set<Row*> accessed_rows_;

    // whether the commit has been verified
    bool verified_;

    // OCC_LAZY: update version only at commit time
    // OCC_EAGER (default): update version at first write (early conflict detection)
    symbol_t policy_;

    std::map<std::string, SnapshotTable*> snapshots_;
    std::set<Table*> snapshot_tables_;

    void incr_row_refcount(Row* r);
    bool version_check();
    bool version_check(const std::unordered_map<row_column_pair, version_t, row_column_pair::hash>& ver_info);
    void release_resource();

public:
    TxnOCC(const TxnMgr* mgr, txn_id_t txnid): Txn2PL(mgr, txnid), verified_(false), policy_(symbol_t::OCC_EAGER) {}

    TxnOCC(const TxnMgr* mgr, txn_id_t txnid, const std::vector<std::string>& table_names);

    ~TxnOCC();

    virtual symbol_t rtti() const {
        return symbol_t::TXN_OCC;
    }

    bool is_readonly() const {
        return !snapshot_tables_.empty();
    }
    SnapshotTable* get_snapshot(const std::string& table_name) const {
        auto it = snapshots_.find(table_name);
        verify(it != snapshots_.end());
        return it->second;
    }

    // call this before ANY operation
    void set_policy(symbol_t _policy) {
        verify(_policy == symbol_t::OCC_EAGER || _policy == symbol_t::OCC_LAZY);
        policy_ = _policy;
    }
    symbol_t policy() const {
        return policy_;
    }

    virtual void abort();
    virtual bool commit();

    // for 2 phase commit, prepare will hold writer locks on verified columns,
    // confirm will commit updates and drop those locks
    bool commit_prepare();
    void commit_confirm();

    bool commit_prepare_or_abort() {
        bool ret = commit_prepare();
        if (!ret) {
            abort();
        }
        return ret;
    }

    virtual bool read_column(Row* row, column_id_t col_id, Value* value);
    virtual bool write_column(Row* row, column_id_t col_id, const Value& value);
    virtual bool insert_row(Table* tbl, Row* row);
    virtual bool remove_row(Table* tbl, Row* row);

    using Txn::query;
    using Txn::query_lt;
    using Txn::query_gt;
    using Txn::query_in;

    ResultSet query(Table* tbl, const MultiBlob& mb) {
        verify(!is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
        return do_query(tbl, mb);
    }
    virtual ResultSet query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) {
        verify(!is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
        return do_query_lt(tbl, smk, order);
    }
    virtual ResultSet query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) {
        verify(!is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
        return do_query_gt(tbl, smk, order);
    }
    virtual ResultSet query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order = symbol_t::ORD_ASC) {
        verify(!is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
        return do_query_in(tbl, low, high, order);
    }
    virtual ResultSet all(Table* tbl, symbol_t order = symbol_t::ORD_ANY) {
        verify(!is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
        return do_all(tbl, order);
    }
};

class TxnMgrOCC: public TxnMgr {
public:
    virtual Txn* start(txn_id_t txnid) {
        return new TxnOCC(this, txnid);
    }

    virtual symbol_t rtti() const {
        return symbol_t::TXN_OCC;
    }

    TxnOCC* start_readonly(txn_id_t txnid, const std::vector<std::string>& table_names) {
        return new TxnOCC(this, txnid, table_names);
    }
};


class TxnNested: public Txn2PL {
    Txn* base_;
    std::unordered_set<Row*> row_inserts_;

public:
    TxnNested(const TxnMgr* mgr, Txn* base): Txn2PL(mgr, base->id()), base_(base) {}

    virtual symbol_t rtti() const {
        return symbol_t::TXN_NESTED;
    }

    virtual void abort();
    virtual bool commit();

    virtual bool read_column(Row* row, column_id_t col_id, Value* value);
    virtual bool write_column(Row* row, column_id_t col_id, const Value& value);
    virtual bool insert_row(Table* tbl, Row* row);
    virtual bool remove_row(Table* tbl, Row* row);

    ResultSet query(Table* tbl, const MultiBlob& mb);
    ResultSet query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC);
    ResultSet query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC);
    ResultSet query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order = symbol_t::ORD_ASC);
    ResultSet all(Table* tbl, symbol_t order = symbol_t::ORD_ANY);
};


} // namespace mdb
