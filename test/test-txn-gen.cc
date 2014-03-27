// automatically generated from test-txn-gen.txt, do not modify!
#include "base/all.h"
#include "memdb/txn.h"
#include "memdb/table.h"

using namespace std;
using namespace base;
using namespace mdb;


// L30: table(student, *id:i32, name:str)
// L32: begin_test()[2pl, unsorted, fine]
TEST(txn_gen, 1) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new UnsortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L34: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L35: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L36: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L38: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // L39: read(tx1a, student, 1, 0) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_FALSE(txn_tx1a->read_column(query_row, 0, &v_read));
    } while(0);
    // L40: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // L42: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L44: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L45: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L46: write(tx2, student, 1, 1, "bob") -> ok
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
    // L47: commit(tx2)
    txn_tx2->commit_or_abort();
    delete txn_tx2;
    // L49: begin(tx3)
    Txn* txn_tx3 = txnmgr.start(-1495230934652649124);
    // L50: read(tx3, student, 1, 1) = "bob" -> ok
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
    // L52: begin(tx4)
    Txn* txn_tx4 = txnmgr.start(-1495230934652649125);
    // L53: read(tx4, student, 1, 1) = "bob" -> ok
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
    // L54: abort(tx4)
    txn_tx4->abort();
    delete txn_tx4;
    // L56: remove(tx3, student, 1) -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_TRUE(txn_tx3->remove_row(tbl_student, query_row));
    } while(0);
    // L58: commit(tx3)
    txn_tx3->commit_or_abort();
    delete txn_tx3;
    // L60: end_test
    delete tbl_student;
}


// L62: begin_test()[2pl, unsorted, coarse]
TEST(txn_gen, 2) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new UnsortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L64: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L65: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L66: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L68: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // L69: read(tx1a, student, 1, 0) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_FALSE(txn_tx1a->read_column(query_row, 0, &v_read));
    } while(0);
    // L70: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // L72: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L74: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L75: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L76: write(tx2, student, 1, 1, "bob") -> ok
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
    // L77: commit(tx2)
    txn_tx2->commit_or_abort();
    delete txn_tx2;
    // L79: begin(tx3)
    Txn* txn_tx3 = txnmgr.start(-1495230934652649124);
    // L80: read(tx3, student, 1, 1) = "bob" -> ok
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
    // L82: begin(tx4)
    Txn* txn_tx4 = txnmgr.start(-1495230934652649125);
    // L83: read(tx4, student, 1, 1) = "bob" -> ok
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
    // L84: abort(tx4)
    txn_tx4->abort();
    delete txn_tx4;
    // L86: remove(tx3, student, 1) -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_TRUE(txn_tx3->remove_row(tbl_student, query_row));
    } while(0);
    // L88: commit(tx3)
    txn_tx3->commit_or_abort();
    delete txn_tx3;
    // L90: end_test
    delete tbl_student;
}


// L93: begin_test()[2pl, sorted, coarse]
TEST(txn_gen, 3) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L95: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L96: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L97: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L99: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // L100: read(tx1a, student, 1, 0) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_FALSE(txn_tx1a->read_column(query_row, 0, &v_read));
    } while(0);
    // L101: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // L103: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L105: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L106: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L107: write(tx2, student, 1, 1, "bob") -> ok
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
    // L108: commit(tx2)
    txn_tx2->commit_or_abort();
    delete txn_tx2;
    // L110: begin(tx3)
    Txn* txn_tx3 = txnmgr.start(-1495230934652649124);
    // L111: read(tx3, student, 1, 1) = "bob" -> ok
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
    // L113: begin(tx4)
    Txn* txn_tx4 = txnmgr.start(-1495230934652649125);
    // L114: read(tx4, student, 1, 1) = "bob" -> ok
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
    // L115: abort(tx4)
    txn_tx4->abort();
    delete txn_tx4;
    // L117: remove(tx3, student, 1) -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_TRUE(txn_tx3->remove_row(tbl_student, query_row));
    } while(0);
    // L119: commit(tx3)
    txn_tx3->commit_or_abort();
    delete txn_tx3;
    // L121: end_test
    delete tbl_student;
}


// L123: begin_test()[2pl, sorted, fine]
TEST(txn_gen, 4) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L125: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L126: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L127: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L129: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // L130: read(tx1a, student, 1, 0) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_FALSE(txn_tx1a->read_column(query_row, 0, &v_read));
    } while(0);
    // L131: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // L133: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L135: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L136: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L137: write(tx2, student, 1, 1, "bob") -> ok
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
    // L138: commit(tx2)
    txn_tx2->commit_or_abort();
    delete txn_tx2;
    // L140: begin(tx3)
    Txn* txn_tx3 = txnmgr.start(-1495230934652649124);
    // L141: read(tx3, student, 1, 1) = "bob" -> ok
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
    // L143: begin(tx4)
    Txn* txn_tx4 = txnmgr.start(-1495230934652649125);
    // L144: read(tx4, student, 1, 1) = "bob" -> ok
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
    // L145: abort(tx4)
    txn_tx4->abort();
    delete txn_tx4;
    // L147: remove(tx3, student, 1) -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_TRUE(txn_tx3->remove_row(tbl_student, query_row));
    } while(0);
    // L149: commit(tx3)
    txn_tx3->commit_or_abort();
    delete txn_tx3;
    // L151: end_test
    delete tbl_student;
}


// L154: begin_test()[2pl, sorted, fine]
TEST(txn_gen, 5) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L156: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L157: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L158: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L160: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // L161: remove(tx1a, student, 1) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_FALSE(txn_tx1a->remove_row(tbl_student, query_row));
    } while(0);
    // L162: write(tx1a, student, 1, 1, "santa") -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_write("santa");
        EXPECT_FALSE(txn_tx1a->write_column(query_row, 1, v_write));
    } while(0);
    // L163: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // L165: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L167: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L168: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L169: commit(tx2)
    txn_tx2->commit_or_abort();
    delete txn_tx2;
    // L171: end_test
    delete tbl_student;
}


// L174: begin_test()[2pl, unsorted, fine]
TEST(txn_gen, 6) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new UnsortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L176: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L177: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L178: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L180: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // L181: remove(tx1a, student, 1) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_FALSE(txn_tx1a->remove_row(tbl_student, query_row));
    } while(0);
    // L182: write(tx1a, student, 1, 1, "santa") -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_write("santa");
        EXPECT_FALSE(txn_tx1a->write_column(query_row, 1, v_write));
    } while(0);
    // L183: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // L185: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L187: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L188: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L189: commit(tx2)
    txn_tx2->commit_or_abort();
    delete txn_tx2;
    // L191: end_test
    delete tbl_student;
}


// L195: begin_test(dummy)[2pl, unsorted, coarse]
TEST(txn_gen, dummy) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new UnsortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L197: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L198: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L199: insert(tx1, student, 2, "bob")
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        txn_tx1->insert_row(tbl_student, insert_row);
    }
    // L201: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // L202: read(tx1a, student, 1, 0)
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        txn_tx1a->read_column(query_row, 0, &v_read);
    } while(0);
    // L203: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // L205: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L207: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L208: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L209: commit(tx2)
    txn_tx2->commit_or_abort();
    delete txn_tx2;
    // L211: end_test
    delete tbl_student;
}


// L213: begin_test(dummy_2)[2pl, sorted, coarse]
TEST(txn_gen, dummy_2) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L215: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L216: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L217: insert(tx1, student, 2, "bob")
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        txn_tx1->insert_row(tbl_student, insert_row);
    }
    // L219: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // L220: read(tx1a, student, 1, 0)
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        txn_tx1a->read_column(query_row, 0, &v_read);
    } while(0);
    // L221: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // L223: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L225: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L226: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L227: commit(tx2)
    txn_tx2->commit_or_abort();
    delete txn_tx2;
    // L229: end_test
    delete tbl_student;
}


// L231: begin_test()[occ, unsorted, versioned]
TEST(txn_gen, 7) {
    TxnMgrOCC txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new UnsortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L232: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L233: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = VersionedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L234: commit(tx1)
    txn_tx1->commit_or_abort();
    delete txn_tx1;
    // L236: begin(tx2a)
    Txn* txn_tx2a = txnmgr.start(2314037222045390847);
    // L237: read(tx2a, student, 1, 1) -> "alice" -> ok
    do {
        ResultSet result_set = txn_tx2a->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx2a->read_column(query_row, 1, &v_read));
    } while(0);
    // L238: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L239: write(tx2, student, 1, 1, "mad_alice") -> ok
    do {
        ResultSet result_set = txn_tx2->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_write("mad_alice");
        EXPECT_TRUE(txn_tx2->write_column(query_row, 1, v_write));
    } while(0);
    // L240: begin(tx2b)
    Txn* txn_tx2b = txnmgr.start(2314037222045390844);
    // L241: read(tx2a, student, 1, 1) -> "mad_alice" -> ok
    do {
        ResultSet result_set = txn_tx2a->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_TRUE(txn_tx2a->read_column(query_row, 1, &v_read));
    } while(0);
    // L242: commit(tx2) -> ok
    EXPECT_TRUE(txn_tx2->commit_or_abort());
    delete txn_tx2;
    // L243: commit(tx2b) -> ok
    EXPECT_TRUE(txn_tx2b->commit_or_abort());
    delete txn_tx2b;
    // L244: commit(tx2a) -> fail
    EXPECT_FALSE(txn_tx2a->commit_or_abort());
    delete txn_tx2a;
    // L245: end_test
    delete tbl_student;
}


// L248: begin_test(crashy)[occ, sorted, versioned]
TEST(txn_gen, crashy) {
    TxnMgrOCC txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L249: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L250: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = VersionedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L251: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L253: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L254: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L255: begin(tx3)
    Txn* txn_tx3 = txnmgr.start(-1495230934652649124);
    // L256: remove(tx3, student, 1) -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_TRUE(txn_tx3->remove_row(tbl_student, query_row));
    } while(0);
    // L257: commit(tx3) -> ok
    EXPECT_TRUE(txn_tx3->commit_or_abort());
    delete txn_tx3;
    // L260: commit(tx2) -> fail
    EXPECT_FALSE(txn_tx2->commit_or_abort());
    delete txn_tx2;
    // L261: end_test
    delete tbl_student;
}


