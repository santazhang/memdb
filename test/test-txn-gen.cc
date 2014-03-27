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


// L123: begin_test()[2pl, snapshot, coarse]
TEST(txn_gen, 4) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SnapshotTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L125: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L126: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L127: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
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


// L153: begin_test()[2pl, sorted, fine]
TEST(txn_gen, 5) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L155: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L156: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L157: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L159: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // L160: read(tx1a, student, 1, 0) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_FALSE(txn_tx1a->read_column(query_row, 0, &v_read));
    } while(0);
    // L161: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // L163: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L165: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L166: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L167: write(tx2, student, 1, 1, "bob") -> ok
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
    // L168: commit(tx2)
    txn_tx2->commit_or_abort();
    delete txn_tx2;
    // L170: begin(tx3)
    Txn* txn_tx3 = txnmgr.start(-1495230934652649124);
    // L171: read(tx3, student, 1, 1) = "bob" -> ok
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
    // L173: begin(tx4)
    Txn* txn_tx4 = txnmgr.start(-1495230934652649125);
    // L174: read(tx4, student, 1, 1) = "bob" -> ok
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
    // L175: abort(tx4)
    txn_tx4->abort();
    delete txn_tx4;
    // L177: remove(tx3, student, 1) -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_TRUE(txn_tx3->remove_row(tbl_student, query_row));
    } while(0);
    // L179: commit(tx3)
    txn_tx3->commit_or_abort();
    delete txn_tx3;
    // L181: end_test
    delete tbl_student;
}


// L183: begin_test()[2pl, snapshot, fine]
TEST(txn_gen, 6) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SnapshotTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L185: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L186: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L187: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L189: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // L190: read(tx1a, student, 1, 0) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        EXPECT_FALSE(txn_tx1a->read_column(query_row, 0, &v_read));
    } while(0);
    // L191: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // L193: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L195: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L196: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L197: write(tx2, student, 1, 1, "bob") -> ok
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
    // L198: commit(tx2)
    txn_tx2->commit_or_abort();
    delete txn_tx2;
    // L200: begin(tx3)
    Txn* txn_tx3 = txnmgr.start(-1495230934652649124);
    // L201: read(tx3, student, 1, 1) = "bob" -> ok
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
    // L203: begin(tx4)
    Txn* txn_tx4 = txnmgr.start(-1495230934652649125);
    // L204: read(tx4, student, 1, 1) = "bob" -> ok
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
    // L205: abort(tx4)
    txn_tx4->abort();
    delete txn_tx4;
    // L207: remove(tx3, student, 1) -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_TRUE(txn_tx3->remove_row(tbl_student, query_row));
    } while(0);
    // L209: commit(tx3)
    txn_tx3->commit_or_abort();
    delete txn_tx3;
    // L211: end_test
    delete tbl_student;
}


// L214: begin_test()[2pl, sorted, fine]
TEST(txn_gen, 7) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L216: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L217: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L218: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L220: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // L221: remove(tx1a, student, 1) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_FALSE(txn_tx1a->remove_row(tbl_student, query_row));
    } while(0);
    // L222: write(tx1a, student, 1, 1, "santa") -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_write("santa");
        EXPECT_FALSE(txn_tx1a->write_column(query_row, 1, v_write));
    } while(0);
    // L223: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // L225: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L227: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L228: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L229: commit(tx2)
    txn_tx2->commit_or_abort();
    delete txn_tx2;
    // L231: end_test
    delete tbl_student;
}


// L234: begin_test()[2pl, unsorted, fine]
TEST(txn_gen, 8) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new UnsortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L236: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L237: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L238: insert(tx1, student, 2, "bob") -> ok
    {
        Row* insert_row = FineLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L240: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // L241: remove(tx1a, student, 1) -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_FALSE(txn_tx1a->remove_row(tbl_student, query_row));
    } while(0);
    // L242: write(tx1a, student, 1, 1, "santa") -> fail
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_write("santa");
        EXPECT_FALSE(txn_tx1a->write_column(query_row, 1, v_write));
    } while(0);
    // L243: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // L245: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L247: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L248: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L249: commit(tx2)
    txn_tx2->commit_or_abort();
    delete txn_tx2;
    // L251: end_test
    delete tbl_student;
}


// L255: begin_test(dummy)[2pl, unsorted, coarse]
TEST(txn_gen, dummy) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new UnsortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L257: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L258: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L259: insert(tx1, student, 2, "bob")
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        txn_tx1->insert_row(tbl_student, insert_row);
    }
    // L261: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // L262: read(tx1a, student, 1, 0)
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        txn_tx1a->read_column(query_row, 0, &v_read);
    } while(0);
    // L263: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // L265: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L267: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L268: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L269: commit(tx2)
    txn_tx2->commit_or_abort();
    delete txn_tx2;
    // L271: end_test
    delete tbl_student;
}


// L273: begin_test(dummy_2)[2pl, sorted, coarse]
TEST(txn_gen, dummy_2) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L275: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L276: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L277: insert(tx1, student, 2, "bob")
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        txn_tx1->insert_row(tbl_student, insert_row);
    }
    // L279: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // L280: read(tx1a, student, 1, 0)
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        txn_tx1a->read_column(query_row, 0, &v_read);
    } while(0);
    // L281: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // L283: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L285: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L286: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L287: commit(tx2)
    txn_tx2->commit_or_abort();
    delete txn_tx2;
    // L289: end_test
    delete tbl_student;
}


// L291: begin_test(dummy_3)[2pl, snapshot, coarse]
TEST(txn_gen, dummy_3) {
    TxnMgr2PL txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SnapshotTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L293: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L294: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L295: insert(tx1, student, 2, "bob")
    {
        Row* insert_row = CoarseLockedRow::create(&schema_student, vector<Value>({Value(i32(2)),Value("bob")}));
        txn_tx1->insert_row(tbl_student, insert_row);
    }
    // L297: begin(tx1a)
    Txn* txn_tx1a = txnmgr.start(2314037222044390706);
    // L298: read(tx1a, student, 1, 0)
    do {
        ResultSet result_set = txn_tx1a->query(tbl_student, Value(i32(1)));
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        Value v_read;
        txn_tx1a->read_column(query_row, 0, &v_read);
    } while(0);
    // L299: abort(tx1a)
    txn_tx1a->abort();
    delete txn_tx1a;
    // L301: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L303: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L304: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L305: commit(tx2)
    txn_tx2->commit_or_abort();
    delete txn_tx2;
    // L307: end_test
    delete tbl_student;
}


// L309: begin_test()[occ, unsorted, versioned]
TEST(txn_gen, 9) {
    TxnMgrOCC txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new UnsortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L310: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L311: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = VersionedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L312: commit(tx1)
    txn_tx1->commit_or_abort();
    delete txn_tx1;
    // L314: begin(tx2a)
    Txn* txn_tx2a = txnmgr.start(2314037222045390847);
    // L315: read(tx2a, student, 1, 1) -> "alice" -> ok
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
    // L316: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L317: write(tx2, student, 1, 1, "mad_alice") -> ok
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
    // L318: begin(tx2b)
    Txn* txn_tx2b = txnmgr.start(2314037222045390844);
    // L319: read(tx2a, student, 1, 1) -> "mad_alice" -> ok
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
    // L320: commit(tx2) -> ok
    EXPECT_TRUE(txn_tx2->commit_or_abort());
    delete txn_tx2;
    // L321: commit(tx2b) -> ok
    EXPECT_TRUE(txn_tx2b->commit_or_abort());
    delete txn_tx2b;
    // L322: commit(tx2a) -> fail
    EXPECT_FALSE(txn_tx2a->commit_or_abort());
    delete txn_tx2a;
    // L323: end_test
    delete tbl_student;
}


// L325: begin_test()[occ, snapshot, versioned]
TEST(txn_gen, 10) {
    TxnMgrOCC txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SnapshotTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L326: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L327: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = VersionedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L328: commit(tx1)
    txn_tx1->commit_or_abort();
    delete txn_tx1;
    // L330: begin(tx2a)
    Txn* txn_tx2a = txnmgr.start(2314037222045390847);
    // L331: read(tx2a, student, 1, 1) -> "alice" -> ok
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
    // L332: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L333: write(tx2, student, 1, 1, "mad_alice") -> ok
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
    // L334: begin(tx2b)
    Txn* txn_tx2b = txnmgr.start(2314037222045390844);
    // L335: read(tx2a, student, 1, 1) -> "mad_alice" -> ok
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
    // L336: commit(tx2) -> ok
    EXPECT_TRUE(txn_tx2->commit_or_abort());
    delete txn_tx2;
    // L337: commit(tx2b) -> ok
    EXPECT_TRUE(txn_tx2b->commit_or_abort());
    delete txn_tx2b;
    // L338: commit(tx2a) -> fail
    EXPECT_FALSE(txn_tx2a->commit_or_abort());
    delete txn_tx2a;
    // L339: end_test
    delete tbl_student;
}


// L342: begin_test(crashy)[occ, sorted, versioned]
TEST(txn_gen, crashy) {
    TxnMgrOCC txnmgr;
    Schema schema_student;
    schema_student.add_key_column("id", Value::I32);
    schema_student.add_column("name", Value::STR);
    Table* tbl_student = new SortedTable(&schema_student);
    txnmgr.reg_table("student", tbl_student);
    // L343: begin(tx1)
    Txn* txn_tx1 = txnmgr.start(-1495230934652649122);
    // L344: insert(tx1, student, 1, "alice") -> ok
    {
        Row* insert_row = VersionedRow::create(&schema_student, vector<Value>({Value(i32(1)),Value("alice")}));
        EXPECT_TRUE(txn_tx1->insert_row(tbl_student, insert_row));
    }
    // L345: commit(tx1) -> ok
    EXPECT_TRUE(txn_tx1->commit_or_abort());
    delete txn_tx1;
    // L347: begin(tx2)
    Txn* txn_tx2 = txnmgr.start(-1495230934652649123);
    // L348: read(tx2, student, 1, 1) = "alice" -> ok
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
    // L349: begin(tx3)
    Txn* txn_tx3 = txnmgr.start(-1495230934652649124);
    // L350: remove(tx3, student, 1) -> ok
    do {
        ResultSet result_set = txn_tx3->query(tbl_student, Value(i32(1)));
        EXPECT_TRUE(result_set.has_next());
        if (!result_set.has_next()) {
            break;
        }
        Row* query_row = result_set.next();
        EXPECT_TRUE(txn_tx3->remove_row(tbl_student, query_row));
    } while(0);
    // L351: commit(tx3) -> ok
    EXPECT_TRUE(txn_tx3->commit_or_abort());
    delete txn_tx3;
    // L354: commit(tx2) -> fail
    EXPECT_FALSE(txn_tx2->commit_or_abort());
    delete txn_tx2;
    // L355: end_test
    delete tbl_student;
}


