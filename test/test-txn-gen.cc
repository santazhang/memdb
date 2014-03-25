// automatically generated from test-txn-gen.txt, do not modify!
#include "base/all.h"
#include "memdb/txn.h"
#include "memdb/table.h"

using namespace std;
using namespace base;
using namespace mdb;


// input: table(student, *id:i32, name:str)
// input: begin_test()[2pl, unsorted, fine]
TEST(txn_gen, 1) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new UnsortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // input: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // input: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // input: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // input: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // input: read(tx1a, student, 1, 0) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_FALSE(txn_tx1a->read_column(query_row, 0, &v_read));
    } while(0);
    // input: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // input: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit());
    delete txn_tx1;
    // input: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // input: read(tx2, student, 1, 1) = "alice" -> ok
    do {
        ResultSet result_set = txn_tx2->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx2->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("alice"));
    } while(0);
    // input: write(tx2, student, 1, 1, "bob") -> ok
    do {
        ResultSet result_set = txn_tx2->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_write("bob");
        EXPECT_TRUE(txn_tx2->write_column(query_row, 1, v_write));
    } while(0);
    // input: commit(tx2)
    txn_tx2->commit();
    delete txn_tx2;
    // input: begin(tx3)
    Txn* txn_tx3 = txnmgr.start(-1495230934652649124);
    // input: read(tx3, student, 1, 1) = "bob" -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx3->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("bob"));
    } while(0);
    // input: begin(tx4)
    Txn* txn_tx4 = txnmgr.start(-1495230934652649125);
    // input: read(tx4, student, 1, 1) = "bob" -> ok
    do {
        ResultSet result_set = txn_tx4->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx4->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("bob"));
    } while(0);
    // input: abort(tx4)
    txn_tx4->abort();
    delete txn_tx4;
    // input: remove(tx3, student, 1) -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_TRUE(txn_tx3->remove_row(tbl_student, query_row));
    } while(0);
    // input: commit(tx3)
    txn_tx3->commit();
    delete txn_tx3;
    // input: end_test
    delete tbl_student;
}


// input: begin_test()[2pl, unsorted, coarse]
TEST(txn_gen, 2) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new UnsortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // input: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // input: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // input: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // input: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // input: read(tx1a, student, 1, 0) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_FALSE(txn_tx1a->read_column(query_row, 0, &v_read));
    } while(0);
    // input: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // input: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit());
    delete txn_tx1;
    // input: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // input: read(tx2, student, 1, 1) = "alice" -> ok
    do {
        ResultSet result_set = txn_tx2->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx2->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("alice"));
    } while(0);
    // input: write(tx2, student, 1, 1, "bob") -> ok
    do {
        ResultSet result_set = txn_tx2->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_write("bob");
        EXPECT_TRUE(txn_tx2->write_column(query_row, 1, v_write));
    } while(0);
    // input: commit(tx2)
    txn_tx2->commit();
    delete txn_tx2;
    // input: begin(tx3)
    Txn* txn_tx3 = txnmgr.start(-1495230934652649124);
    // input: read(tx3, student, 1, 1) = "bob" -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx3->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("bob"));
    } while(0);
    // input: begin(tx4)
    Txn* txn_tx4 = txnmgr.start(-1495230934652649125);
    // input: read(tx4, student, 1, 1) = "bob" -> ok
    do {
        ResultSet result_set = txn_tx4->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx4->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("bob"));
    } while(0);
    // input: abort(tx4)
    txn_tx4->abort();
    delete txn_tx4;
    // input: remove(tx3, student, 1) -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_TRUE(txn_tx3->remove_row(tbl_student, query_row));
    } while(0);
    // input: commit(tx3)
    txn_tx3->commit();
    delete txn_tx3;
    // input: end_test
    delete tbl_student;
}


// input: begin_test()[2pl, sorted, coarse]
TEST(txn_gen, 3) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // input: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // input: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // input: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // input: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // input: read(tx1a, student, 1, 0) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_FALSE(txn_tx1a->read_column(query_row, 0, &v_read));
    } while(0);
    // input: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // input: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit());
    delete txn_tx1;
    // input: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // input: read(tx2, student, 1, 1) = "alice" -> ok
    do {
        ResultSet result_set = txn_tx2->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx2->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("alice"));
    } while(0);
    // input: write(tx2, student, 1, 1, "bob") -> ok
    do {
        ResultSet result_set = txn_tx2->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_write("bob");
        EXPECT_TRUE(txn_tx2->write_column(query_row, 1, v_write));
    } while(0);
    // input: commit(tx2)
    txn_tx2->commit();
    delete txn_tx2;
    // input: begin(tx3)
    Txn* txn_tx3 = txnmgr.start(-1495230934652649124);
    // input: read(tx3, student, 1, 1) = "bob" -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx3->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("bob"));
    } while(0);
    // input: begin(tx4)
    Txn* txn_tx4 = txnmgr.start(-1495230934652649125);
    // input: read(tx4, student, 1, 1) = "bob" -> ok
    do {
        ResultSet result_set = txn_tx4->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx4->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("bob"));
    } while(0);
    // input: abort(tx4)
    txn_tx4->abort();
    delete txn_tx4;
    // input: remove(tx3, student, 1) -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_TRUE(txn_tx3->remove_row(tbl_student, query_row));
    } while(0);
    // input: commit(tx3)
    txn_tx3->commit();
    delete txn_tx3;
    // input: end_test
    delete tbl_student;
}


// input: begin_test()[2pl, sorted, fine]
TEST(txn_gen, 4) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // input: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // input: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // input: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // input: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // input: read(tx1a, student, 1, 0) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_FALSE(txn_tx1a->read_column(query_row, 0, &v_read));
    } while(0);
    // input: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // input: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit());
    delete txn_tx1;
    // input: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // input: read(tx2, student, 1, 1) = "alice" -> ok
    do {
        ResultSet result_set = txn_tx2->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx2->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("alice"));
    } while(0);
    // input: write(tx2, student, 1, 1, "bob") -> ok
    do {
        ResultSet result_set = txn_tx2->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_write("bob");
        EXPECT_TRUE(txn_tx2->write_column(query_row, 1, v_write));
    } while(0);
    // input: commit(tx2)
    txn_tx2->commit();
    delete txn_tx2;
    // input: begin(tx3)
    Txn* txn_tx3 = txnmgr.start(-1495230934652649124);
    // input: read(tx3, student, 1, 1) = "bob" -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx3->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("bob"));
    } while(0);
    // input: begin(tx4)
    Txn* txn_tx4 = txnmgr.start(-1495230934652649125);
    // input: read(tx4, student, 1, 1) = "bob" -> ok
    do {
        ResultSet result_set = txn_tx4->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx4->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("bob"));
    } while(0);
    // input: abort(tx4)
    txn_tx4->abort();
    delete txn_tx4;
    // input: remove(tx3, student, 1) -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_TRUE(txn_tx3->remove_row(tbl_student, query_row));
    } while(0);
    // input: commit(tx3)
    txn_tx3->commit();
    delete txn_tx3;
    // input: end_test
    delete tbl_student;
}


// input: begin_test()[2pl, sorted, fine]
TEST(txn_gen, 5) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // input: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // input: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // input: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // input: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // input: remove(tx1a, student, 1) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_FALSE(txn_tx1a->remove_row(tbl_student, query_row));
    } while(0);
    // input: write(tx1a, student, 1, 1, "santa") -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_write("santa");
        EXPECT_FALSE(txn_tx1a->write_column(query_row, 1, v_write));
    } while(0);
    // input: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // input: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit());
    delete txn_tx1;
    // input: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // input: read(tx2, student, 1, 1) = "alice" -> ok
    do {
        ResultSet result_set = txn_tx2->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx2->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("alice"));
    } while(0);
    // input: commit(tx2)
    txn_tx2->commit();
    delete txn_tx2;
    // input: end_test
    delete tbl_student;
}


// input: begin_test()[2pl, unsorted, fine]
TEST(txn_gen, 6) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new UnsortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // input: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // input: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // input: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // input: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // input: remove(tx1a, student, 1) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_FALSE(txn_tx1a->remove_row(tbl_student, query_row));
    } while(0);
    // input: write(tx1a, student, 1, 1, "santa") -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_write("santa");
        EXPECT_FALSE(txn_tx1a->write_column(query_row, 1, v_write));
    } while(0);
    // input: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // input: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit());
    delete txn_tx1;
    // input: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // input: read(tx2, student, 1, 1) = "alice" -> ok
    do {
        ResultSet result_set = txn_tx2->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx2->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("alice"));
    } while(0);
    // input: commit(tx2)
    txn_tx2->commit();
    delete txn_tx2;
    // input: end_test
    delete tbl_student;
}


// input: begin_test(dummy)[2pl, unsorted, coarse]
TEST(txn_gen, dummy) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new UnsortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // input: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // input: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // input: insert(tx1, student, 2, "bob")
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        txn_tx1->insert_row(tbl_student, insert_row);
    }
    // input: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // input: read(tx1a, student, 1, 0)
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        txn_tx1a->read_column(query_row, 0, &v_read);
    } while(0);
    // input: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // input: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit());
    delete txn_tx1;
    // input: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // input: read(tx2, student, 1, 1) = "alice" -> ok
    do {
        ResultSet result_set = txn_tx2->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx2->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("alice"));
    } while(0);
    // input: commit(tx2)
    txn_tx2->commit();
    delete txn_tx2;
    // input: end_test
    delete tbl_student;
}


// input: begin_test(dummy_2)[2pl, sorted, coarse]
TEST(txn_gen, dummy_2) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // input: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // input: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // input: insert(tx1, student, 2, "bob")
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        txn_tx1->insert_row(tbl_student, insert_row);
    }
    // input: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // input: read(tx1a, student, 1, 0)
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        txn_tx1a->read_column(query_row, 0, &v_read);
    } while(0);
    // input: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // input: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit());
    delete txn_tx1;
    // input: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // input: read(tx2, student, 1, 1) = "alice" -> ok
    do {
        ResultSet result_set = txn_tx2->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx2->read_column(query_row, 1, &v_read));
        EXPECT_EQ(v_read, Value("alice"));
    } while(0);
    // input: commit(tx2)
    txn_tx2->commit();
    delete txn_tx2;
    // input: end_test
    delete tbl_student;
}


