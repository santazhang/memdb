#include "base/all.h"
#include "memdb/txn.h"
#include "memdb/table.h"

#include "test-helper.h"

using namespace std;
using namespace base;
using namespace mdb;

TEST(issue, 1) {
    Schema schema;
    schema.add_key_column("id", Value::I32);
    schema.add_column("balance", Value::I32);

    Table* tbl = new UnsortedTable(&schema);
    TxnMgrOCC txn_mgr;
    txn_mgr.reg_table("account", tbl);

    // populate table
    const int max_record_id = 1000;
    for (int row_id = 0; row_id < max_record_id; row_id++) {
        vector<Value> row_data = {Value((i32) row_id), Value((i32) 0)};
        Row* row = VersionedRow::create(&schema, row_data);
        tbl->insert(row);
    }
    Log_info("populated %d rows", max_record_id);

    // OCC_EAGER mode test
    {
        Log::info("OCC_EAGER mode");
        const i64 txn_id = 1;
        const i32 account_id = 1;
        const i32 amount = 100;

        TxnOCC* txn = (TxnOCC *) txn_mgr.start(txn_id);
        txn->set_policy(symbol_t::OCC_EAGER);

        Log::debug("update txn_id=%ld, account_id=%d, amount=%d", txn_id, account_id, amount);
        ResultSet rs = txn->query(tbl, Value(account_id));
        verify(rs.has_next());
        Row* row = rs.next();
        Value v;
        txn->read_column(row, 1, &v);
        v = Value((i32) (v.get_i32() + amount));
        txn->write_column(row, 1, v);
        txn->write_column(row, 1, v);   // check for multiple writes

        bool outcome = txn->commit_prepare_or_abort();
        if (outcome) {
            Log::debug("outcome: COMMIT");
        } else {
            Log::debug("outcome: ABORT");
        }
        EXPECT_EQ(outcome, true);

        delete txn;
    }

    // OCC_LAZY mode test
    {
        Log::info("OCC_LAZY mode");
        const i64 txn_id = 2;
        const i32 account_id = 2;
        const i32 amount = 100;

        TxnOCC* txn = (TxnOCC *) txn_mgr.start(txn_id);
        txn->set_policy(symbol_t::OCC_LAZY);

        Log::debug("update txn_id=%ld, account_id=%d, amount=%d", txn_id, account_id, amount);
        ResultSet rs = txn->query(tbl, Value(account_id));
        verify(rs.has_next());
        Row* row = rs.next();
        Value v;
        txn->read_column(row, 1, &v);
        v = Value((i32) (v.get_i32() + amount));
        txn->write_column(row, 1, v);
        txn->write_column(row, 1, v);   // check for multiple writes

        bool outcome = txn->commit_prepare_or_abort();
        if (outcome) {
            Log::debug("outcome: COMMIT");
        } else {
            Log::debug("outcome: ABORT");
        }
        EXPECT_EQ(outcome, true);

        delete txn;
    }

    delete tbl;
}
