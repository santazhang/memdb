#include "base/all.h"
#include "memdb/txn.h"
#include "memdb/table.h"

#include "test-helper.h"

using namespace std;
using namespace base;
using namespace mdb;

TEST(txn_nested, basic_op_unsafe) {
    TxnMgrUnsafe txnmgr;

    Schema schema;
    schema.add_key_column("id", Value::I32);
    schema.add_column("name", Value::STR);
    UnsortedTable* student_tbl = new UnsortedTable(&schema);

    txnmgr.reg_table("student", student_tbl);

    Txn* txn = txnmgr.start(1);
    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(&schema, row1);
    txn->insert_row(student_tbl, r1);
    {
        Txn* txn_inner = txnmgr.start_nested(txn);
        ResultSet rs = txn_inner->query(student_tbl, Value(i32(1)));
        EXPECT_TRUE(rs.has_next());
        if (rs.has_next()) {
            Row* r = rs.next();
            txn_inner->write_column(r, 1, Value("bob"));
        }
        txn_inner->commit_or_abort();
        delete txn_inner;
    }
    txn->commit_or_abort();

    print_table(student_tbl);

    delete txn;

    delete student_tbl;
}

TEST(txn_nested, basic_op_2pl) {
    TxnMgr2PL txnmgr;

    Schema schema;
    schema.add_key_column("id", Value::I32);
    schema.add_column("name", Value::STR);
    UnsortedTable* student_tbl = new UnsortedTable(&schema);

    txnmgr.reg_table("student", student_tbl);

    Txn* txn = txnmgr.start(1);
    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(&schema, row1);
    txn->insert_row(student_tbl, r1);
    {
        Log::debug("### before inner write");
        print_result(txn->all(student_tbl));

        Txn* txn_inner = txnmgr.start_nested(txn);
        ResultSet rs = txn_inner->query(student_tbl, Value(i32(1)));
        EXPECT_TRUE(rs.has_next());
        if (rs.has_next()) {
            Row* r = rs.next();
            txn_inner->write_column(r, 1, Value("bob"));
        }

        Log::debug("### after inner write");
        print_result(txn->all(student_tbl));
        EXPECT_EQ(txn->all(student_tbl).next()->get_column(1), Value("alice"));

        txn_inner->commit_or_abort();
        delete txn_inner;
    }
    Log::debug("### after inner commit");
    txn->commit_or_abort();
    EXPECT_EQ(txn->all(student_tbl).next()->get_column(1), Value("bob"));

    print_table(student_tbl);

    delete txn;

    delete student_tbl;
}

TEST(txn_nested, basic_op_occ) {
    TxnMgrOCC txnmgr;

    Schema schema;
    schema.add_key_column("id", Value::I32);
    schema.add_column("name", Value::STR);
    UnsortedTable* student_tbl = new UnsortedTable(&schema);

    txnmgr.reg_table("student", student_tbl);

    Txn* txn = txnmgr.start(1);
    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = VersionedRow::create(&schema, row1);
    txn->insert_row(student_tbl, r1);
    {
        Log::debug("### before inner write");
        print_result(txn->all(student_tbl));

        Txn* txn_inner = txnmgr.start_nested(txn);
        ResultSet rs = txn_inner->query(student_tbl, Value(i32(1)));
        EXPECT_TRUE(rs.has_next());
        if (rs.has_next()) {
            Row* r = rs.next();
            txn_inner->write_column(r, 1, Value("bob"));
        }

        Log::debug("### after inner write");
        print_result(txn->all(student_tbl));
        EXPECT_EQ(txn->all(student_tbl).next()->get_column(1), Value("alice"));

        txn_inner->commit_or_abort();
        delete txn_inner;
    }
    Log::debug("### after inner commit");
    txn->commit_or_abort();
    EXPECT_EQ(txn->all(student_tbl).next()->get_column(1), Value("bob"));

    print_table(student_tbl);

    delete txn;

    delete student_tbl;
}
