#include "base/all.h"
#include "memdb/txn.h"
#include "memdb/table.h"

#include "test-helper.h"

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


TEST(txn, query_in_unsorted_table_staging_area_while_inserting) {
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
    ResultSet rs1 = txn1->query(student_tbl, Value(i32(1)));
    EXPECT_TRUE(rs1.has_next());

    // txn2 cannot see the row that txn1 is trying to insert
    Txn* txn2 = txnmgr.start(2);
    ResultSet rs2 = txn2->query(student_tbl, Value(i32(1)));
    EXPECT_FALSE(rs2.has_next());
    txn2->abort();

    txn1->commit();

    Txn* txn3 = txnmgr.start(3);
    ResultSet rs3 = txn3->query(student_tbl, Value(i32(1)));
    EXPECT_TRUE(rs3.has_next());
    txn3->abort();

    delete txn1;
    delete txn2;
    delete txn3;

    delete student_tbl;
    delete schema;
}

TEST(txn, query_in_unsorted_table_staging_area_while_deleting) {
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
    ResultSet rs1 = txn1->query(student_tbl, Value(i32(1)));
    EXPECT_TRUE(rs1.has_next());
    txn1->commit();

    // txn2 tries to remove row {id=1}
    Txn* txn2 = txnmgr.start(2);
    ResultSet rs2 = txn2->query(student_tbl, Value(i32(1)));
    EXPECT_TRUE(rs2.has_next());
    txn2->remove_row(student_tbl, rs2.next());

    Txn* txn3 = txnmgr.start(3);
    ResultSet rs3 = txn3->query(student_tbl, Value(i32(1)));
    EXPECT_TRUE(rs3.has_next()); // since row {id=1} is still in table
    Row* txn3_r = rs3.next();
    Value txn3_v1, txn3_v2;
    EXPECT_FALSE(txn3->read_column(txn3_r, 0, &txn3_v1)); // since we have locked the row for deleting!
    EXPECT_FALSE(txn3->read_column(txn3_r, 1, &txn3_v2)); // since we have locked the row for deleting!
    txn3->abort();

    // now we actually remove the row {id=1}
    txn2->commit();

    // now txn4 cannot see the row!
    Txn* txn4 = txnmgr.start(4);
    ResultSet rs4 = txn4->query(student_tbl, Value(i32(1)));
    EXPECT_FALSE(rs4.has_next());
    txn4->commit();

    delete txn1;
    delete txn2;
    delete txn3;
    delete txn4;

    delete student_tbl;
    delete schema;
}

TEST(txn, query_sorted_table_ordering) {
    TxnMgr2PL txnmgr;
    Schema schema;
    schema.add_key_column("id", Value::I32);
    schema.add_column("name", Value::STR);

    Table* student_tbl = new SortedTable(&schema);
    txnmgr.reg_table("student", student_tbl);

    Txn* txn1 = txnmgr.start(1);
    {
        vector<Value> row = { Value((i32) 1), Value("alice") };
        FineLockedRow* r = FineLockedRow::create(&schema, row);
        txn1->insert_row(student_tbl, r);
    }
    {
        vector<Value> row = { Value((i32) 1), Value("bob") };
        FineLockedRow* r = FineLockedRow::create(&schema, row);
        txn1->insert_row(student_tbl, r);
    }
    {
        vector<Value> row = { Value((i32) 1), Value("calvin") };
        FineLockedRow* r = FineLockedRow::create(&schema, row);
        txn1->insert_row(student_tbl, r);
    }
    {
        vector<Value> row = { Value((i32) 2), Value("david") };
        FineLockedRow* r = FineLockedRow::create(&schema, row);
        txn1->insert_row(student_tbl, r);
    }
    {
        vector<Value> row = { Value((i32) 2), Value("elvis") };
        FineLockedRow* r = FineLockedRow::create(&schema, row);
        txn1->insert_row(student_tbl, r);
    }
    ResultSet rs = txn1->query(student_tbl, Value(i32(1)));
    print_result(rs);
    Log::debug("---");
    rs = txn1->query(student_tbl, Value(i32(2)));
    print_result(rs);
    Log::debug("---");
    rs = txn1->query(student_tbl, Value(i32(3)));
    print_result(rs);
    EXPECT_EQ(enumerator_count(txn1->query(student_tbl, Value(i32(0)))), 0);
    EXPECT_EQ(enumerator_count(txn1->query(student_tbl, Value(i32(1)))), 3);
    EXPECT_EQ(enumerator_count(txn1->query(student_tbl, Value(i32(2)))), 2);
    EXPECT_EQ(enumerator_count(txn1->query(student_tbl, Value(i32(3)))), 0);
    EXPECT_TRUE(rows_are_sorted(txn1->query(student_tbl, Value(i32(0)))));
    EXPECT_TRUE(rows_are_sorted(txn1->query(student_tbl, Value(i32(1)))));
    EXPECT_TRUE(rows_are_sorted(txn1->query(student_tbl, Value(i32(2)))));
    EXPECT_TRUE(rows_are_sorted(txn1->query(student_tbl, Value(i32(3)))));
    Log::debug("---");
    print_result(txn1->all(student_tbl));
    EXPECT_EQ(enumerator_count(txn1->all(student_tbl)), 5);
    EXPECT_TRUE(rows_are_sorted(txn1->all(student_tbl)));
    Log::debug("---");
    print_result(txn1->all(student_tbl, symbol_t::ORD_DESC));
    EXPECT_EQ(enumerator_count(txn1->all(student_tbl, symbol_t::ORD_DESC)), 5);
    EXPECT_TRUE(rows_are_sorted(txn1->all(student_tbl, symbol_t::ORD_DESC), symbol_t::ORD_DESC));
    txn1->commit();
    delete txn1;

    delete student_tbl;
}
