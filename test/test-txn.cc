#include "base/all.h"
#include "memdb/txn.h"
#include "memdb/table.h"

using namespace std;
using namespace base;
using namespace mdb;

TEST(txn, basic_op_unsafe) {
    TxnMgrUnsafe txnmgr;

    Schema* schema = new Schema;
    schema->add_key_column("id", Value::I32);
    schema->add_column("name", Value::STR);
    UnsortedTable* student_tbl = new UnsortedTable(schema);

    txnmgr.reg_table("student", student_tbl);

    Txn* txn1 = txnmgr.start(1);
    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    txn1->insert_row(student_tbl, r1);
    txn1->commit();

    Txn* txn2 = txnmgr.start(2);
    Row* r = txn2->get_unsorted_table("student")->query(Value(i32(1))).next();
    Value v;
    txn2->read_column(r, 1, &v);
    EXPECT_EQ(v.get_str(), "alice");
    txn2->abort();

    Txn* txn3 = txnmgr.start(3);
    r = txn2->get_unsorted_table("student")->query(Value(i32(1))).next();
    txn3->write_column(r, 1, Value("bob"));
    txn3->commit();

    Txn* txn4 = txnmgr.start(4);
    r = txn4->get_unsorted_table("student")->query(Value(i32(1))).next();
    Value v2;
    txn4->read_column(r, 1, &v2);
    EXPECT_EQ(v2.get_str(), "bob");
    txn4->abort();

    delete txn1;
    delete txn2;
    delete txn3;
    delete txn4;

    delete student_tbl;
    delete schema;
}


TEST(txn, basic_op_2pl) {
    TxnMgr2PL txnmgr;

    Schema* schema = new Schema;
    schema->add_key_column("id", Value::I32);
    schema->add_column("name", Value::STR);
    UnsortedTable* student_tbl = new UnsortedTable(schema);

    txnmgr.reg_table("student", student_tbl);

    Txn* txn1 = txnmgr.start(1);
    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    FineLockedRow* r1 = FineLockedRow::create(schema, row1);
    txn1->insert_row(student_tbl, r1);
    txn1->commit();

    Txn* txn2 = txnmgr.start(2);
    Row* r = txn2->get_unsorted_table("student")->query(Value(i32(1))).next();
    Value v;
    txn2->read_column(r, 1, &v);
    EXPECT_EQ(v.get_str(), "alice");
    txn2->abort();

    Txn* txn3 = txnmgr.start(3);
    r = txn2->get_unsorted_table("student")->query(Value(i32(1))).next();
    txn3->write_column(r, 1, Value("bob"));
    txn3->commit();

    Txn* txn4 = txnmgr.start(4);
    r = txn4->get_unsorted_table("student")->query(Value(i32(1))).next();
    Value v2;
    txn4->read_column(r, 1, &v2);
    EXPECT_EQ(v2.get_str(), "bob");
    txn4->abort();

    delete txn1;
    delete txn2;
    delete txn3;
    delete txn4;

    delete student_tbl;
    delete schema;
}
