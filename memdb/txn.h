#pragma once

#include <string>
#include <map>

#include "utils.h"

namespace mdb {

// foreward declaration
class Table;
class TxnMgr;

typedef i64 txn_id_t;


class Txn: public NoCopy {
protected:
    const TxnMgr* mgr_;
    txn_id_t txnid_;
    Txn(const TxnMgr* mgr, txn_id_t txnid): mgr_(mgr),  txnid_(txnid) {}

public:
    txn_id_t id() const {
        return txnid_;
    }
    virtual void abort() = 0;
    virtual bool commit() = 0;
};


class TxnMgr: public NoCopy {
    std::map<std::string, Table*> tables_;
public:

    virtual Txn* start(txn_id_t txnid) = 0;

    void reg_table(const std::string& tbl_name, Table* tbl) {
        verify(tables_.find(tbl_name) == tables_.end());
        insert_into_map(tables_, tbl_name, tbl);
    }
};


class TxnUnsafe: public Txn {
public:
    TxnUnsafe(const TxnMgr* mgr, txn_id_t txnid): Txn(mgr, txnid) {}
    void abort() {
        // do nothing
    }
    bool commit() {
        return true;
    }
};

class TxnMgrUnsafe: public TxnMgr {
public:
    virtual Txn* start(txn_id_t txnid) {
        return new TxnUnsafe(this, txnid);
    }
};

} // namespace mdb
