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
    txn1->commit_or_abort();

    Txn* txn2 = txnmgr.start(2);
    Row* r = txn2->get_unsorted_table("student")->query(Value(i32(1))).next();
    Value v;
    txn2->read_column(r, 1, &v);
    EXPECT_EQ(v.get_str(), "alice");
    txn2->abort();

    Txn* txn3 = txnmgr.start(3);
    r = txn2->get_unsorted_table("student")->query(Value(i32(1))).next();
    txn3->write_column(r, 1, Value("bob"));
    txn3->commit_or_abort();

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
    txn1->commit_or_abort();

    Txn* txn2 = txnmgr.start(2);
    Row* r = txn2->get_unsorted_table("student")->query(Value(i32(1))).next();
    Value v;
    txn2->read_column(r, 1, &v);
    EXPECT_EQ(v.get_str(), "alice");
    txn2->abort();

    Txn* txn3 = txnmgr.start(3);
    r = txn2->get_unsorted_table("student")->query(Value(i32(1))).next();
    txn3->write_column(r, 1, Value("bob"));
    txn3->commit_or_abort();

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

TEST(txn, basic_op_occ) {
    TxnMgrOCC txnmgr;
    Schema schema;
    schema.add_key_column("id", Value::I32);
    schema.add_column("name", Value::STR);
    Table* student_tbl = new UnsortedTable(&schema);

    txnmgr.reg_table("student", student_tbl);

    // test basic read write correctness
    Txn* txn1 = txnmgr.start(1);
    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    VersionedRow* r1 = VersionedRow::create(&schema, row1);
    EXPECT_TRUE(txn1->insert_row(student_tbl, r1));
    EXPECT_TRUE(txn1->commit_or_abort());

    Txn* txn2 = txnmgr.start(2);
    Txn* txn3 = txnmgr.start(3);
    vector<Value> row2 = { Value((i32) 2), Value("bob") };
    VersionedRow* r2 = VersionedRow::create(&schema, row2);
    vector<Value> row3 = { Value((i32) 3), Value("cynthia") };
    VersionedRow* r3 = VersionedRow::create(&schema, row3);
    EXPECT_TRUE(txn2->insert_row(student_tbl, r2));
    EXPECT_TRUE(txn3->insert_row(student_tbl, r3));
    EXPECT_TRUE(txn2->commit_or_abort());
    print_result(txn3->all(student_tbl));
    EXPECT_TRUE(txn3->commit_or_abort());

    Txn* txn4 = txnmgr.start(4);
    EXPECT_EQ(enumerator_count(txn4->all(student_tbl)), 3);
    print_result(txn4->all(student_tbl));
    txn4->remove_row(student_tbl, txn4->query(student_tbl, Value(i32(2))).next());
    Log::debug("---");
    print_result(txn4->all(student_tbl));
    EXPECT_EQ(enumerator_count(txn4->all(student_tbl)), 2);
    txn4->abort();

    // now begin occ!
    Txn* txn5 = txnmgr.start(5);
    Value v1;
    // r1, r2, r3 still in table
    EXPECT_TRUE(txn5->read_column(r1, 1, &v1));
    EXPECT_EQ(v1, Value("alice"));

    Txn* txn6 = txnmgr.start(6);
    Value v2("mad alice");
    EXPECT_TRUE(txn6->write_column(r1, 1, v2));
    EXPECT_TRUE(txn6->commit_or_abort());
    EXPECT_FALSE(txn5->commit_or_abort());

    delete txn1;
    delete txn2;
    delete txn3;
    delete txn4;
    delete txn5;
    delete txn6;
    delete student_tbl;
}

TEST(txn, 2pl_remove_dup_row_in_staging_area) {
    TxnMgr2PL txnmgr;
    Schema schema;
    schema.add_key_column("id", Value::I32);
    schema.add_column("name", Value::STR);

    Table* student_tbl = new SortedTable(&schema);
    txnmgr.reg_table("student", student_tbl);

    Txn* txn1 = txnmgr.start(1);
    {
        vector<Value> row = { Value((i32) 1), Value("alice") };
        CoarseLockedRow* r = CoarseLockedRow::create(&schema, row);
        EXPECT_TRUE(txn1->insert_row(student_tbl, r));
    }
    {
        vector<Value> row = { Value((i32) 1), Value("alice_1") };
        CoarseLockedRow* r = CoarseLockedRow::create(&schema, row);
        EXPECT_TRUE(txn1->insert_row(student_tbl, r));
    }
    {
        vector<Value> row = { Value((i32) 1), Value("alice_2") };
        CoarseLockedRow* r = CoarseLockedRow::create(&schema, row);
        EXPECT_TRUE(txn1->insert_row(student_tbl, r));
    }
    {
        vector<Value> row = { Value((i32) 1), Value("alice_3") };
        CoarseLockedRow* r = CoarseLockedRow::create(&schema, row);
        EXPECT_TRUE(txn1->insert_row(student_tbl, r));
        Log::debug("before remove");
        print_result(txn1->all(student_tbl));
        EXPECT_EQ(enumerator_count(txn1->all(student_tbl)), 4);
        Log::debug("The row to remove is:");
        print_row(r);
        txn1->remove_row(student_tbl, r);
        Log::debug("after remove");
        print_result(txn1->all(student_tbl));
        EXPECT_EQ(enumerator_count(txn1->all(student_tbl)), 3);
        EXPECT_EQ(txn1->all(student_tbl).next()->get_column(1), Value("alice"));
    }
    txn1->commit_or_abort();

    delete txn1;
    delete student_tbl;
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

    txn1->commit_or_abort();

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
    txn1->commit_or_abort();

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
    txn2->commit_or_abort();

    // now txn4 cannot see the row!
    Txn* txn4 = txnmgr.start(4);
    ResultSet rs4 = txn4->query(student_tbl, Value(i32(1)));
    EXPECT_FALSE(rs4.has_next());
    txn4->commit_or_abort();

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


    Log::debug("====");
    print_result(txn1->query_lt(student_tbl,Value(i32(3))));
    EXPECT_EQ(enumerator_count(txn1->query_lt(student_tbl, Value(i32(0)))), 0);
    EXPECT_EQ(enumerator_count(txn1->query_lt(student_tbl, Value(i32(1)))), 0);
    EXPECT_EQ(enumerator_count(txn1->query_lt(student_tbl, Value(i32(2)))), 3);
    EXPECT_EQ(enumerator_count(txn1->query_lt(student_tbl, Value(i32(3)))), 5);
    EXPECT_EQ(enumerator_count(txn1->query_gt(student_tbl, Value(i32(0)))), 5);
    EXPECT_EQ(enumerator_count(txn1->query_gt(student_tbl, Value(i32(1)))), 2);
    EXPECT_EQ(enumerator_count(txn1->query_gt(student_tbl, Value(i32(2)))), 0);
    EXPECT_EQ(enumerator_count(txn1->query_gt(student_tbl, Value(i32(3)))), 0);

    EXPECT_EQ(enumerator_count(txn1->query_in(student_tbl, Value(i32(0)), Value(i32(3)))), 5);
    EXPECT_EQ(enumerator_count(txn1->query_in(student_tbl, Value(i32(1)), Value(i32(2)))), 0);
    EXPECT_EQ(enumerator_count(txn1->query_in(student_tbl, Value(i32(0)), Value(i32(2)))), 3);
    EXPECT_EQ(enumerator_count(txn1->query_in(student_tbl, Value(i32(1)), Value(i32(3)))), 2);

    EXPECT_TRUE(rows_are_sorted(txn1->query_lt(student_tbl, Value(i32(0)))));
    EXPECT_TRUE(rows_are_sorted(txn1->query_lt(student_tbl, Value(i32(1)))));
    EXPECT_TRUE(rows_are_sorted(txn1->query_lt(student_tbl, Value(i32(2)))));
    EXPECT_TRUE(rows_are_sorted(txn1->query_lt(student_tbl, Value(i32(3)))));
    EXPECT_TRUE(rows_are_sorted(txn1->query_lt(student_tbl, Value(i32(0)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(txn1->query_lt(student_tbl, Value(i32(1)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(txn1->query_lt(student_tbl, Value(i32(2)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(txn1->query_lt(student_tbl, Value(i32(3)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(txn1->query_gt(student_tbl, Value(i32(0)))));
    EXPECT_TRUE(rows_are_sorted(txn1->query_gt(student_tbl, Value(i32(1)))));
    EXPECT_TRUE(rows_are_sorted(txn1->query_gt(student_tbl, Value(i32(2)))));
    EXPECT_TRUE(rows_are_sorted(txn1->query_gt(student_tbl, Value(i32(3)))));
    EXPECT_TRUE(rows_are_sorted(txn1->query_gt(student_tbl, Value(i32(0)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(txn1->query_gt(student_tbl, Value(i32(1)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(txn1->query_gt(student_tbl, Value(i32(2)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(txn1->query_gt(student_tbl, Value(i32(3)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));

    txn1->commit_or_abort();
    delete txn1;

    Txn* txn2 = txnmgr.start(2);

    EXPECT_EQ(enumerator_count(txn2->query(student_tbl, Value(i32(0)))), 0);
    EXPECT_EQ(enumerator_count(txn2->query(student_tbl, Value(i32(1)))), 3);
    EXPECT_EQ(enumerator_count(txn2->query(student_tbl, Value(i32(2)))), 2);
    EXPECT_EQ(enumerator_count(txn2->query(student_tbl, Value(i32(3)))), 0);
    EXPECT_TRUE(rows_are_sorted(txn2->query(student_tbl, Value(i32(0)))));
    EXPECT_TRUE(rows_are_sorted(txn2->query(student_tbl, Value(i32(1)))));
    EXPECT_TRUE(rows_are_sorted(txn2->query(student_tbl, Value(i32(2)))));
    EXPECT_TRUE(rows_are_sorted(txn2->query(student_tbl, Value(i32(3)))));
    Log::debug("-- txn2->all(student_tbl) --");
    print_result(txn2->all(student_tbl));
    print_result(((SortedTable*)student_tbl)->all());
    EXPECT_EQ(enumerator_count(txn2->all(student_tbl)), 5);
    EXPECT_TRUE(rows_are_sorted(txn2->all(student_tbl)));
    Log::debug("---");
    print_result(txn2->all(student_tbl, symbol_t::ORD_DESC));
    EXPECT_EQ(enumerator_count(txn2->all(student_tbl, symbol_t::ORD_DESC)), 5);
    EXPECT_TRUE(rows_are_sorted(txn2->all(student_tbl, symbol_t::ORD_DESC), symbol_t::ORD_DESC));

    Log::debug("====");
    print_result(txn2->query_lt(student_tbl,Value(i32(3))));
    EXPECT_EQ(enumerator_count(txn2->query_lt(student_tbl, Value(i32(0)))), 0);
    EXPECT_EQ(enumerator_count(txn2->query_lt(student_tbl, Value(i32(1)))), 0);
    EXPECT_EQ(enumerator_count(txn2->query_lt(student_tbl, Value(i32(2)))), 3);
    EXPECT_EQ(enumerator_count(txn2->query_lt(student_tbl, Value(i32(3)))), 5);
    EXPECT_EQ(enumerator_count(txn2->query_gt(student_tbl, Value(i32(0)))), 5);
    EXPECT_EQ(enumerator_count(txn2->query_gt(student_tbl, Value(i32(1)))), 2);
    EXPECT_EQ(enumerator_count(txn2->query_gt(student_tbl, Value(i32(2)))), 0);
    EXPECT_EQ(enumerator_count(txn2->query_gt(student_tbl, Value(i32(3)))), 0);

    EXPECT_EQ(enumerator_count(txn2->query_in(student_tbl, Value(i32(0)), Value(i32(3)))), 5);
    EXPECT_EQ(enumerator_count(txn2->query_in(student_tbl, Value(i32(1)), Value(i32(2)))), 0);
    EXPECT_EQ(enumerator_count(txn2->query_in(student_tbl, Value(i32(0)), Value(i32(2)))), 3);
    EXPECT_EQ(enumerator_count(txn2->query_in(student_tbl, Value(i32(1)), Value(i32(3)))), 2);

    EXPECT_TRUE(rows_are_sorted(txn2->query_lt(student_tbl, Value(i32(0)))));
    EXPECT_TRUE(rows_are_sorted(txn2->query_lt(student_tbl, Value(i32(1)))));
    EXPECT_TRUE(rows_are_sorted(txn2->query_lt(student_tbl, Value(i32(2)))));
    EXPECT_TRUE(rows_are_sorted(txn2->query_lt(student_tbl, Value(i32(3)))));
    EXPECT_TRUE(rows_are_sorted(txn2->query_lt(student_tbl, Value(i32(0)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(txn2->query_lt(student_tbl, Value(i32(1)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(txn2->query_lt(student_tbl, Value(i32(2)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(txn2->query_lt(student_tbl, Value(i32(3)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(txn2->query_gt(student_tbl, Value(i32(0)))));
    EXPECT_TRUE(rows_are_sorted(txn2->query_gt(student_tbl, Value(i32(1)))));
    EXPECT_TRUE(rows_are_sorted(txn2->query_gt(student_tbl, Value(i32(2)))));
    EXPECT_TRUE(rows_are_sorted(txn2->query_gt(student_tbl, Value(i32(3)))));
    EXPECT_TRUE(rows_are_sorted(txn2->query_gt(student_tbl, Value(i32(0)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(txn2->query_gt(student_tbl, Value(i32(1)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(txn2->query_gt(student_tbl, Value(i32(2)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(txn2->query_gt(student_tbl, Value(i32(3)), symbol_t::ORD_DESC), symbol_t::ORD_DESC));

    txn2->abort();
    delete txn2;

    delete student_tbl;
}


TEST(txn, readonly_txn) {
    TxnMgrOCC txnmgr;
    Schema schema;
    schema.add_key_column("id", Value::I32);
    schema.add_column("name", Value::STR);

    Table* student_tbl = new SnapshotTable(&schema);
    txnmgr.reg_table("student", student_tbl);

    Txn* txn1 = txnmgr.start(1);
    {
        vector<Value> row = { Value((i32) 1), Value("alice") };
        VersionedRow* r = VersionedRow::create(&schema, row);
        txn1->insert_row(student_tbl, r);
    }
    {
        vector<Value> row = { Value((i32) 1), Value("bob") };
        VersionedRow* r = VersionedRow::create(&schema, row);
        txn1->insert_row(student_tbl, r);
    }
    {
        vector<Value> row = { Value((i32) 1), Value("calvin") };
        VersionedRow* r = VersionedRow::create(&schema, row);
        txn1->insert_row(student_tbl, r);
    }
    {
        vector<Value> row = { Value((i32) 2), Value("david") };
        VersionedRow* r = VersionedRow::create(&schema, row);
        txn1->insert_row(student_tbl, r);
    }
    {
        vector<Value> row = { Value((i32) 2), Value("elvis") };
        VersionedRow* r = VersionedRow::create(&schema, row);
        txn1->insert_row(student_tbl, r);
    }
    print_result(txn1->all(student_tbl));
    EXPECT_TRUE(txn1->commit_or_abort());
    delete txn1;

    Txn* txn2 = txnmgr.start(2);
    Txn* txn3 = txnmgr.start(3);
    TxnOCC* txn4 = txnmgr.start_readonly(4, {"student"});
    // try reading something
    print_table(txn3, student_tbl);
    print_table(txn4, txn4->get_snapshot("student"));

    // txn2 writes some thing, which will make txn3 abort, but txn4 commits since it's readonly, and works on a snapshot
    txn2->write_column(txn2->all(student_tbl).next(), 1, Value("mad"));

    EXPECT_TRUE(txn2->commit_or_abort());
    EXPECT_FALSE(txn3->commit_or_abort());
    EXPECT_TRUE(txn4->commit_or_abort());

    delete txn2;
    delete txn3;
    delete txn4;

    delete student_tbl;
}
