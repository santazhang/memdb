#include <vector>
#include <sstream>

#include "memdb/schema.h"
#include "memdb/table.h"
#include "base/all.h"

using namespace base;
using namespace mdb;
using namespace std;

static void print_table(UnsortedTable* tbl) {
    const Schema* sch = tbl->schema();
    UnsortedTable::Cursor cur = tbl->all();
    while (cur) {
        ostringstream ostr;
        Row* r = cur.next();
        for (auto& col : *sch) {
            ostr << " " << r->get_column(col.id);
        }
        Log::info("row:%s", ostr.str().c_str());
    }
}

static void print_result(const Schema* sch, SortedTable::Cursor cur) {
    while (cur) {
        ostringstream ostr;
        Row* r = cur.next();
        for (auto& col : *sch) {
            ostr << " " << r->get_column(col.id);
        }
        Log::info("row:%s", ostr.str().c_str());
    }
}

static void print_table(SortedTable* tbl) {
    const Schema* sch = tbl->schema();
    SortedTable::Cursor cur = tbl->all();
    print_result(sch, cur);
}

TEST(table, create) {
    Schema* schema = new Schema;
    schema->add_key_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    UnsortedTable* ut = new UnsortedTable(schema);

    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    ut->insert(r1);

    UnsortedTable::Cursor cursor = ut->query(r1->get_key());
    std::list<Row*> query_result;
    while (cursor) {
        query_result.push_back(cursor.next());
    }
    EXPECT_EQ(query_result.size(), 1u);
    EXPECT_EQ(query_result.front(), r1);

    map<string, Value> row2;
    row2["id"] = (i32) 2;
    row2["name"] = "bob";
    Row* r2 = Row::create(schema, row2);
    ut->insert(r2);

    print_table(ut);
    Log::debug("update row 2, set name = amy");

    r2->update("name", "amy");
    cursor = ut->query(Value((i32) 2));
    EXPECT_EQ(cursor.count(), 1);
    Row* row = cursor.next();
    EXPECT_EQ(row, r2);
    EXPECT_EQ(r2->get_column("id").get_i32(), 2);
    EXPECT_EQ(r2->get_column("name").get_str(), "amy");

    print_table(ut);

    unordered_map<string, Value> row3;
    row3["id"] = (i32) 3;
    row3["name"] = "cathy";
    Row* r3 = Row::create(schema, row3);
    ut->insert(r3);

    Log::debug("inserted id=3, name=cathy");
    print_table(ut);

    r3->update("id", (i32) 9);
    r3->update("name", "cathy awesome");
    Log::debug("updated row 3, set id=9, name=cathy awesome");
    print_table(ut);
    cursor = ut->query(Value((i32) 9));
    EXPECT_EQ(cursor.count(), 1);
    row = cursor.next();
    EXPECT_EQ(row, r3);
    EXPECT_EQ(r3->get_column("id").get_i32(), 9);
    EXPECT_EQ(r3->get_column("name").get_str(), "cathy awesome");

    Log::debug("all table:");
    print_table(ut);
    EXPECT_EQ(ut->all().count(), 3);

    // try removing row 2
    ut->remove(Value((i32) 2));

    EXPECT_EQ(ut->all().count(), 2);

    // try removing all rows
    ut->clear();

    delete ut;
}


TEST(table, compound_key) {
    Schema* schema = new Schema;
    schema->add_key_column("id", Value::I32);
    schema->add_key_column("name", Value::STR);

    UnsortedTable* ut = new UnsortedTable(schema);

    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    ut->insert(r1);

    UnsortedTable::Cursor cursor = ut->query(r1->get_key());
    std::list<Row*> query_result;
    while (cursor) {
        query_result.push_back(cursor.next());
    }
    EXPECT_EQ(query_result.size(), 1u);
    EXPECT_EQ(query_result.front(), r1);

    map<string, Value> row2;
    row2["id"] = (i32) 2;
    row2["name"] = "bob";
    Row* r2 = Row::create(schema, row2);
    ut->insert(r2);

    unordered_map<string, Value> row3;
    row3["id"] = (i32) 3;
    row3["name"] = "cathy";
    Row* r3 = Row::create(schema, row3);
    ut->insert(r3);

    EXPECT_EQ(ut->all().count(), 3);

    // try removing row 3
    ut->remove(r3);

    EXPECT_EQ(ut->all().count(), 2);

    // try removing all rows
    ut->clear();

    delete ut;
}

TEST(table, sorted_table_create) {
    Schema* schema = new Schema;
    schema->add_key_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    SortedTable* st = new SortedTable(schema);

    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    st->insert(r1);

    SortedTable::Cursor cursor = st->query(r1->get_key());
    std::list<Row*> query_result;
    while (cursor) {
        query_result.push_back(cursor.next());
    }
    EXPECT_EQ(query_result.size(), 1u);
    EXPECT_EQ(query_result.front(), r1);

    map<string, Value> row2;
    row2["id"] = (i32) 2;
    row2["name"] = "bob";
    Row* r2 = Row::create(schema, row2);
    st->insert(r2);

    print_table(st);
    Log::debug("update row 2, set name = amy");

    r2->update("name", "amy");
    cursor = st->query(Value((i32) 2));
    EXPECT_EQ(cursor.count(), 1);
    Row* row = cursor.next();
    EXPECT_EQ(row, r2);
    EXPECT_EQ(r2->get_column("id").get_i32(), 2);
    EXPECT_EQ(r2->get_column("name").get_str(), "amy");

    print_table(st);

    unordered_map<string, Value> row3;
    row3["id"] = (i32) 3;
    row3["name"] = "cathy";
    Row* r3 = Row::create(schema, row3);
    st->insert(r3);

    Log::debug("inserted id=3, name=cathy");
    print_table(st);

    r3->update("id", (i32) 9);
    r3->update("name", "cathy awesome");
    Log::debug("updated row 3, set id=9, name=cathy awesome");
    print_table(st);
    cursor = st->query(Value((i32) 9));
    EXPECT_EQ(cursor.count(), 1);
    row = cursor.next();
    EXPECT_EQ(row, r3);
    EXPECT_EQ(r3->get_column("id").get_i32(), 9);
    EXPECT_EQ(r3->get_column("name").get_str(), "cathy awesome");

    Log::debug("all table:");
    print_table(st);
    EXPECT_EQ(st->all().count(), 3);

    // try removing row 2
    st->remove(Value((i32) 2));

    EXPECT_EQ(st->all().count(), 2);

    // try removing all rows
    st->remove(st->all());
    EXPECT_EQ(st->all().count(), 0);

    delete st;
}

TEST(table, sorted_table_queries) {
    Schema* schema = new Schema;
    schema->add_key_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    SortedTable* st = new SortedTable(schema);

    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    st->insert(r1);

    vector<Value> row1b = { Value((i32) 1), Value("alice_2") };
    Row* r1b = Row::create(schema, row1b);
    st->insert(r1b);

    map<string, Value> row2;
    row2["id"] = (i32) 2;
    row2["name"] = "bob";
    Row* r2 = Row::create(schema, row2);
    st->insert(r2);
    map<string, Value> row2b;
    row2b["id"] = (i32) 2;
    row2b["name"] = "bob_2";
    Row* r2b = Row::create(schema, row2b);
    st->insert(r2b);

    unordered_map<string, Value> row3;
    row3["id"] = (i32) 3;
    row3["name"] = "cathy";
    Row* r3 = Row::create(schema, row3);
    st->insert(r3);
    unordered_map<string, Value> row3b;
    row3b["id"] = (i32) 3;
    row3b["name"] = "cathy_2";
    Row* r3b = Row::create(schema, row3b);
    st->insert(r3b);

    Log::debug("full table:");
    print_table(st);

    Log::debug("key < 2:");
    print_result(st->schema(), st->query_lt(Value((i32) 2)));

    Log::debug("key > 2:");
    print_result(st->schema(), st->query_gt(Value((i32) 2)));

    Log::debug("1 < key < 3:");
    print_result(st->schema(), st->query_in(Value((i32) 1), Value((i32) 3)));

    Log::debug("remove 1 < key < 3:");
    st->remove(st->query_in(Value((i32) 1), Value((i32) 3)));
    print_table(st);

    delete st;
}

TEST(table, sorted_multi_key_cmp) {
    {
        Schema* schema = new Schema;
        schema->add_key_column("id", Value::I32);
        schema->add_column("name", Value::STR);

        Value v1 = Value((i32) 1987);
        Value v2 = Value((i32) 1987);
        MultiBlob mb1(v1.get_blob());
        MultiBlob mb2(v2.get_blob());
        SortedMultiKey smk1(mb1, schema);
        SortedMultiKey smk2(mb2, schema);
        EXPECT_TRUE(smk1 == smk2);
        EXPECT_TRUE(smk1 <= smk2);
        EXPECT_TRUE(smk1 >= smk2);
        EXPECT_FALSE(smk1 != smk2);
        EXPECT_FALSE(smk1 < smk2);
        EXPECT_FALSE(smk1 > smk2);
        delete schema;
    }

    {
        Schema* schema = new Schema;
        schema->add_key_column("id", Value::I32);
        schema->add_column("name", Value::STR);

        Value v1 = Value((i32) 1987);
        Value v2 = Value((i32) 1989);
        MultiBlob mb1(v1.get_blob());
        MultiBlob mb2(v2.get_blob());
        SortedMultiKey smk1(mb1, schema);
        SortedMultiKey smk2(mb2, schema);
        EXPECT_FALSE(smk1 == smk2);
        EXPECT_TRUE(smk1 <= smk2);
        EXPECT_FALSE(smk1 >= smk2);
        EXPECT_TRUE(smk1 != smk2);
        EXPECT_TRUE(smk1 < smk2);
        EXPECT_FALSE(smk1 > smk2);
        delete schema;
    }

    {
        Schema* schema = new Schema;
        schema->add_key_column("id", Value::I32);
        schema->add_key_column("name", Value::STR);

        Value v1a = Value((i32) 1987);
        Value v1b = Value("santa");
        Value v2a = Value((i32) 1989);
        Value v2b = Value("cynthia");
        MultiBlob mb1(2);
        mb1[0] = v1a.get_blob();
        mb1[1] = v1b.get_blob();
        MultiBlob mb2(2);
        mb2[0] = v2a.get_blob();
        mb2[1] = v2b.get_blob();
        SortedMultiKey smk1(mb1, schema);
        SortedMultiKey smk2(mb2, schema);
        EXPECT_TRUE(smk1 != smk2);
        EXPECT_TRUE(smk1 <= smk2);
        EXPECT_TRUE(smk1 < smk2);
        EXPECT_FALSE(smk1 == smk2);
        EXPECT_FALSE(smk1 > smk2);
        EXPECT_FALSE(smk1 >= smk2);
        delete schema;
    }

    {
        Schema* schema = new Schema;
        schema->add_key_column("id", Value::I32);
        schema->add_key_column("name", Value::STR);

        Value v1a = Value((i32) 1987);
        Value v1b = Value("santa");
        Value v2a = Value((i32) 1987);
        Value v2b = Value("cynthia");
        MultiBlob mb1(2);
        mb1[0] = v1a.get_blob();
        mb1[1] = v1b.get_blob();
        MultiBlob mb2(2);
        mb2[0] = v2a.get_blob();
        mb2[1] = v2b.get_blob();
        SortedMultiKey smk1(mb1, schema);
        SortedMultiKey smk2(mb2, schema);
        EXPECT_TRUE(smk1 != smk2);
        EXPECT_TRUE(smk1 >= smk2);
        EXPECT_TRUE(smk1 > smk2);
        EXPECT_FALSE(smk1 == smk2);
        EXPECT_FALSE(smk1 < smk2);
        EXPECT_FALSE(smk1 <= smk2);
        delete schema;
    }

    {
        Schema* schema = new Schema;
        schema->add_key_column("name", Value::STR);
        schema->add_key_column("id", Value::I32);

        Value v1a = Value("santa");
        Value v1b = Value((i32) 1987);
        Value v2a = Value("cynthia");
        Value v2b = Value((i32) 1989);

        MultiBlob mb1(2);
        mb1[0] = v1a.get_blob();
        mb1[1] = v1b.get_blob();
        MultiBlob mb2(2);
        mb2[0] = v2a.get_blob();
        mb2[1] = v2b.get_blob();
        SortedMultiKey smk1(mb1, schema);
        SortedMultiKey smk2(mb2, schema);
        EXPECT_TRUE(smk1 != smk2);
        EXPECT_TRUE(smk1 >= smk2);
        EXPECT_TRUE(smk1 > smk2);
        EXPECT_FALSE(smk1 == smk2);
        EXPECT_FALSE(smk1 < smk2);
        EXPECT_FALSE(smk1 <= smk2);
        delete schema;
    }

    {
        Schema* schema = new Schema;
        schema->add_key_column("name", Value::STR);
        schema->add_key_column("id", Value::I32);

        Value v1a = Value("santa");
        Value v1b = Value((i32) 1987);
        Value v2a = Value("santa");
        Value v2b = Value((i32) 1989);

        MultiBlob mb1(2);
        mb1[0] = v1a.get_blob();
        mb1[1] = v1b.get_blob();
        MultiBlob mb2(2);
        mb2[0] = v2a.get_blob();
        mb2[1] = v2b.get_blob();
        SortedMultiKey smk1(mb1, schema);
        SortedMultiKey smk2(mb2, schema);
        EXPECT_TRUE(smk1 != smk2);
        EXPECT_TRUE(smk1 <= smk2);
        EXPECT_TRUE(smk1 < smk2);
        EXPECT_FALSE(smk1 == smk2);
        EXPECT_FALSE(smk1 > smk2);
        EXPECT_FALSE(smk1 >= smk2);
        delete schema;
    }
}

TEST(table, create_snapshot_table) {
    Schema* schema = new Schema;
    schema->add_key_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    SnapshotTable* st = new SnapshotTable(schema);

    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    st->insert(r1);

    SnapshotTable::Cursor cursor = st->query(r1->get_key());
    EXPECT_EQ(cursor.count(), 1);

    delete st;
}

TEST(table, snapshot_cannot_update_inplace) {
    Schema* schema = new Schema;
    schema->add_key_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    SnapshotTable* st = new SnapshotTable(schema);

    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    st->insert(r1);

    SnapshotTable::Cursor cursor = st->query(r1->get_key());
    EXPECT_EQ(cursor.count(), 1);

    auto row = cursor.next();
    EXPECT_TRUE(row->readonly());

    // snapshot cannot inplace update
    // row->update(0, 0);

    delete st;
}

TEST(table, snapshot_insert_and_snapshots) {
    // the schema will be accessed both by SnapshotTable and Cursors
    // and SnapshotTable will not delete schema, so it's ok to leave it on stack
    Schema schema;
    schema.add_key_column("id", Value::I32);
    schema.add_column("name", Value::STR);

    SnapshotTable* st = new SnapshotTable(&schema);
    EXPECT_EQ(st->all().count(), 0);

    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(&schema, row1);
    st->insert(r1);
    EXPECT_EQ(st->all().count(), 1);

    SnapshotTable::Cursor cursor = st->query(r1->get_key());

    map<string, Value> row2;
    row2["id"] = (i32) 2;
    row2["name"] = "bob";
    Row* r2 = Row::create(&schema, row2);
    st->insert(r2);

    st->remove(Value(i32(1)));
    EXPECT_EQ(st->all().count(), 1);

    // check snapshot properties
    EXPECT_EQ(cursor.count(), 1);

    delete st;

    auto row = cursor.next();
    EXPECT_TRUE(row->readonly());
    EXPECT_EQ(row, r1);
    EXPECT_EQ(r1->get_column(0).get_i32(), 1);
    EXPECT_EQ(r1->get_column(1).get_str(), "alice");
}

TEST(table, snapshot_table_remove) {
    // the schema will be accessed both by SnapshotTable and Cursors
    // and SnapshotTable will not delete schema, so it's ok to leave it on stack
    Schema schema;
    schema.add_key_column("id", Value::I32);
    schema.add_column("name", Value::STR);

    SnapshotTable* st = new SnapshotTable(&schema);
    EXPECT_EQ(st->all().count(), 0);

    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(&schema, row1);
    st->insert(r1);
    EXPECT_EQ(st->all().count(), 1);

    SnapshotTable::Cursor cursor = st->query(r1->get_key());

    map<string, Value> row2;
    row2["id"] = (i32) 1;
    row2["name"] = "bob";
    Row* r2 = Row::create(&schema, row2);
    st->insert(r2);

    st->remove(r1);
    EXPECT_EQ(st->all().count(), 1);

    // check snapshot properties
    EXPECT_EQ(cursor.count(), 1);

    delete st;

    auto row = cursor.next();
    EXPECT_TRUE(row->readonly());
    EXPECT_EQ(row, r1);
    EXPECT_EQ(r1->get_column(0).get_i32(), 1);
    EXPECT_EQ(r1->get_column(1).get_str(), "alice");
}


TEST(table, snapshot_table_remove_range) {
    // the schema will be accessed both by SnapshotTable and Cursors
    // and SnapshotTable will not delete schema, so it's ok to leave it on stack
    Schema schema;
    schema.add_key_column("id", Value::I32);
    schema.add_column("name", Value::STR);

    SnapshotTable* st = new SnapshotTable(&schema);
    EXPECT_EQ(st->all().count(), 0);

    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(&schema, row1);
    st->insert(r1);
    EXPECT_EQ(st->all().count(), 1);

    SnapshotTable::Cursor cursor = st->query(r1->get_key());

    map<string, Value> row2;
    row2["id"] = (i32) 2;
    row2["name"] = "bob";
    Row* r2 = Row::create(&schema, row2);
    st->insert(r2);

    EXPECT_EQ(st->all().count(), 2);

    st->remove(st->all());
    EXPECT_EQ(st->all().count(), 0);

    // check snapshot properties
    EXPECT_EQ(cursor.count(), 1);

    delete st;

    auto row = cursor.next();
    EXPECT_TRUE(row->readonly());
    EXPECT_EQ(row, r1);
    EXPECT_EQ(r1->get_column(0).get_i32(), 1);
    EXPECT_EQ(r1->get_column(1).get_str(), "alice");
}
