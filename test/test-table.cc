#include <vector>
#include <sstream>

#include "memdb/schema.h"
#include "memdb/table.h"
#include "base/all.h"

#include "test-helper.h"

using namespace base;
using namespace mdb;
using namespace std;

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
    delete schema;
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
    delete schema;
}

TEST(table, sorted_table_create) {
    Schema* schema = new Schema;
    schema->add_key_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    SortedTable* st = new SortedTable(schema);
    EXPECT_TRUE(rows_are_sorted(st->all()));

    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    st->insert(r1);
    EXPECT_TRUE(rows_are_sorted(st->all()));

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

    EXPECT_TRUE(rows_are_sorted(st->all()));
    print_table(st);
    Log::debug("update row 2, set name = amy");

    r2->update("name", "amy");
    cursor = st->query(Value((i32) 2));
    EXPECT_EQ(cursor.count(), 1);
    Row* row = cursor.next();
    EXPECT_EQ(row, r2);
    EXPECT_EQ(r2->get_column("id").get_i32(), 2);
    EXPECT_EQ(r2->get_column("name").get_str(), "amy");

    EXPECT_TRUE(rows_are_sorted(st->all()));
    print_table(st);

    unordered_map<string, Value> row3;
    row3["id"] = (i32) 3;
    row3["name"] = "cathy";
    Row* r3 = Row::create(schema, row3);
    st->insert(r3);

    Log::debug("inserted id=3, name=cathy");
    print_table(st);
    EXPECT_TRUE(rows_are_sorted(st->all()));

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

    EXPECT_TRUE(rows_are_sorted(st->all()));
    EXPECT_EQ(st->all().count(), 2);

    // try removing all rows
    st->remove(st->all());
    EXPECT_EQ(st->all().count(), 0);
    EXPECT_TRUE(rows_are_sorted(st->all()));

    delete st;
    delete schema;
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
    EXPECT_TRUE(rows_are_sorted(st->all()));

    Log::debug("full table (reverse):");
    print_result(st->all(symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(st->all(symbol_t::ORD_DESC), symbol_t::ORD_DESC));

    Log::debug("key < 2:");
    print_result(st->query_lt(Value((i32) 2)));
    EXPECT_TRUE(rows_are_sorted(st->query_lt(Value((i32) 2))));

    Log::debug("key < 2 (reverse order):");
    print_result(st->query_lt(Value((i32) 2), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(st->query_lt(Value((i32) 2), symbol_t::ORD_DESC), symbol_t::ORD_DESC));

    Log::debug("key < 3:");
    print_result(st->query_lt(Value((i32) 3)));
    EXPECT_TRUE(rows_are_sorted(st->query_lt(Value((i32) 3))));

    Log::debug("key < 3 (reverse order):");
    print_result(st->query_lt(Value((i32) 3), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(st->query_lt(Value((i32) 3), symbol_t::ORD_DESC), symbol_t::ORD_DESC));

    Log::debug("key > 2:");
    print_result(st->query_gt(Value((i32) 2)));
    EXPECT_TRUE(rows_are_sorted(st->query_gt(Value((i32) 2))));

    Log::debug("key > 2 (reverse order):");
    print_result(st->query_gt(Value((i32) 2), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(st->query_gt(Value((i32) 2), symbol_t::ORD_DESC), symbol_t::ORD_DESC));

    Log::debug("key > 1:");
    print_result(st->query_gt(Value((i32) 1)));
    EXPECT_TRUE(rows_are_sorted(st->query_gt(Value((i32) 1))));

    Log::debug("key > 1 (reverse order):");
    print_result(st->query_gt(Value((i32) 1), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(st->query_lt(Value((i32) 1), symbol_t::ORD_DESC), symbol_t::ORD_DESC));

    Log::debug("1 < key < 3:");
    print_result(st->query_in(Value((i32) 1), Value((i32) 3)));
    EXPECT_TRUE(rows_are_sorted(st->query_in(Value((i32) 1), Value((i32) 3))));

    Log::debug("1 < key < 3 (reverse order):");
    print_result(st->query_in(Value((i32) 1), Value((i32) 3), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(st->query_in(Value((i32) 1), Value((i32) 3), symbol_t::ORD_DESC), symbol_t::ORD_DESC));

    Log::debug("remove 1 < key < 3:");
    st->remove(st->query_in(Value((i32) 1), Value((i32) 3)));
    print_table(st);
    EXPECT_TRUE(rows_are_sorted(st->all()));

    delete st;
    delete schema;
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
    // the schema will be accessed both by SnapshotTable and Cursors
    Schema schema;
    schema.add_key_column("id", Value::I32);
    schema.add_column("name", Value::STR);

    SnapshotTable* st = new SnapshotTable(&schema);

    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(&schema, row1);
    st->insert(r1);

    SnapshotTable::Cursor cursor = st->query(r1->get_key());
    EXPECT_EQ(cursor.count(), 1);

    delete st;
}

TEST(table, snapshot_cannot_update_inplace) {
    // the schema will be accessed both by SnapshotTable and Cursors
    Schema schema;
    schema.add_key_column("id", Value::I32);
    schema.add_column("name", Value::STR);

    SnapshotTable* st = new SnapshotTable(&schema);

    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(&schema, row1);
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


TEST(table, indexed_table_create) {
    IndexedSchema* schema = new IndexedSchema;
    schema->add_key_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    schema->add_index("i_id_name", {0, 1});
    schema->add_index("i_name", {1});
    schema->add_index_by_column_names("i_name_id", {"name", "id"});

    IndexedTable* idxtbl = new IndexedTable(schema);
    EXPECT_TRUE(rows_are_sorted(idxtbl->all()));

    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    idxtbl->insert(r1);
    EXPECT_TRUE(rows_are_sorted(idxtbl->all()));

    SortedTable::Cursor cursor = idxtbl->query(r1->get_key());
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
    idxtbl->insert(r2);

    EXPECT_TRUE(rows_are_sorted(idxtbl->all()));
    print_table(idxtbl);
    Log::debug("update row 2, set name = amy");

    r2->update("name", "amy");
    cursor = idxtbl->query(Value((i32) 2));
    EXPECT_EQ(cursor.count(), 1);
    Row* row = cursor.next();
    EXPECT_EQ(row, r2);
    EXPECT_EQ(r2->get_column("id").get_i32(), 2);
    EXPECT_EQ(r2->get_column("name").get_str(), "amy");

    EXPECT_TRUE(rows_are_sorted(idxtbl->all()));
    print_table(idxtbl);

    unordered_map<string, Value> row3;
    row3["id"] = (i32) 3;
    row3["name"] = "cathy";
    Row* r3 = Row::create(schema, row3);
    idxtbl->insert(r3);

    Log::debug("inserted id=3, name=cathy");
    print_table(idxtbl);
    EXPECT_TRUE(rows_are_sorted(idxtbl->all()));

    r3->update("id", (i32) 9);
    r3->update("name", "cathy awesome");
    Log::debug("updated row 3, set id=9, name=cathy awesome");
    print_table(idxtbl);
    cursor = idxtbl->query(Value((i32) 9));
    EXPECT_EQ(cursor.count(), 1);
    row = cursor.next();
    EXPECT_EQ(row, r3);
    EXPECT_EQ(r3->get_column("id").get_i32(), 9);
    EXPECT_EQ(r3->get_column("name").get_str(), "cathy awesome");

    Log::debug("all table:");
    print_table(idxtbl);
    EXPECT_EQ(idxtbl->all().count(), 3);

    // try removing row 2
    idxtbl->remove(Value((i32) 2));

    EXPECT_TRUE(rows_are_sorted(idxtbl->all()));
    EXPECT_EQ(idxtbl->all().count(), 2);

    // try removing all rows
    idxtbl->remove(idxtbl->all());
    EXPECT_EQ(idxtbl->all().count(), 0);
    EXPECT_TRUE(rows_are_sorted(idxtbl->all()));

    delete idxtbl;
    delete schema;
}

TEST(table, indexed_table_queries) {
    IndexedSchema* schema = new IndexedSchema;
    schema->add_key_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    schema->add_index("i_id_name", {0, 1});
    schema->add_index("i_name", {1});
    schema->add_index("i_name_id", {1, 0});

    IndexedTable* idxtbl = new IndexedTable(schema);

    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    idxtbl->insert(r1);

    vector<Value> row1b = { Value((i32) 1), Value("alice_2") };
    Row* r1b = Row::create(schema, row1b);
    idxtbl->insert(r1b);

    map<string, Value> row2;
    row2["id"] = (i32) 2;
    row2["name"] = "bob";
    Row* r2 = Row::create(schema, row2);
    idxtbl->insert(r2);
    map<string, Value> row2b;
    row2b["id"] = (i32) 2;
    row2b["name"] = "bob_2";
    Row* r2b = Row::create(schema, row2b);
    idxtbl->insert(r2b);

    unordered_map<string, Value> row3;
    row3["id"] = (i32) 3;
    row3["name"] = "cathy";
    Row* r3 = Row::create(schema, row3);
    idxtbl->insert(r3);
    unordered_map<string, Value> row3b;
    row3b["id"] = (i32) 3;
    row3b["name"] = "cathy_2";
    Row* r3b = Row::create(schema, row3b);
    idxtbl->insert(r3b);

    Log::debug("full table:");
    print_table(idxtbl);
    EXPECT_TRUE(rows_are_sorted(idxtbl->all()));

    Log::debug("full table (reverse):");
    print_result(idxtbl->all(symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(idxtbl->all(symbol_t::ORD_DESC), symbol_t::ORD_DESC));

    Log::debug("key < 2:");
    print_result(idxtbl->query_lt(Value((i32) 2)));
    EXPECT_TRUE(rows_are_sorted(idxtbl->query_lt(Value((i32) 2))));

    Log::debug("key < 2 (reverse order):");
    print_result(idxtbl->query_lt(Value((i32) 2), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(idxtbl->query_lt(Value((i32) 2), symbol_t::ORD_DESC), symbol_t::ORD_DESC));

    Log::debug("key < 3:");
    print_result(idxtbl->query_lt(Value((i32) 3)));
    EXPECT_TRUE(rows_are_sorted(idxtbl->query_lt(Value((i32) 3))));

    Log::debug("key < 3 (reverse order):");
    print_result(idxtbl->query_lt(Value((i32) 3), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(idxtbl->query_lt(Value((i32) 3), symbol_t::ORD_DESC), symbol_t::ORD_DESC));

    Log::debug("key > 2:");
    print_result(idxtbl->query_gt(Value((i32) 2)));
    EXPECT_TRUE(rows_are_sorted(idxtbl->query_gt(Value((i32) 2))));

    Log::debug("key > 2 (reverse order):");
    print_result(idxtbl->query_gt(Value((i32) 2), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(idxtbl->query_gt(Value((i32) 2), symbol_t::ORD_DESC), symbol_t::ORD_DESC));

    Log::debug("key > 1:");
    print_result(idxtbl->query_gt(Value((i32) 1)));
    EXPECT_TRUE(rows_are_sorted(idxtbl->query_gt(Value((i32) 1))));

    Log::debug("key > 1 (reverse order):");
    print_result(idxtbl->query_gt(Value((i32) 1), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(idxtbl->query_lt(Value((i32) 1), symbol_t::ORD_DESC), symbol_t::ORD_DESC));

    Log::debug("1 < key < 3:");
    print_result(idxtbl->query_in(Value((i32) 1), Value((i32) 3)));
    EXPECT_TRUE(rows_are_sorted(idxtbl->query_in(Value((i32) 1), Value((i32) 3))));

    Log::debug("1 < key < 3 (reverse order):");
    print_result(idxtbl->query_in(Value((i32) 1), Value((i32) 3), symbol_t::ORD_DESC));
    EXPECT_TRUE(rows_are_sorted(idxtbl->query_in(Value((i32) 1), Value((i32) 3), symbol_t::ORD_DESC), symbol_t::ORD_DESC));

    Log::debug("remove 1 < key < 3:");
    idxtbl->remove(idxtbl->query_in(Value((i32) 1), Value((i32) 3)));
    print_table(idxtbl);
    EXPECT_TRUE(rows_are_sorted(idxtbl->all()));

    delete idxtbl;
    delete schema;
}

TEST(table, indexed_table_queries_on_index) {
    IndexedSchema* schema = new IndexedSchema;
    schema->add_key_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    schema->add_index("i_id_name", {0, 1});
    schema->add_index("i_name", {1});
    schema->add_index("i_name_id", {1, 0});

    IndexedTable* idxtbl = new IndexedTable(schema);

    vector<Value> row1 = { Value((i32) 1), Value("z_alice") };
    Row* r1 = Row::create(schema, row1);
    idxtbl->insert(r1);

    vector<Value> row1b = { Value((i32) 1), Value("y_alice_2") };
    Row* r1b = Row::create(schema, row1b);
    idxtbl->insert(r1b);

    map<string, Value> row2;
    row2["id"] = (i32) 2;
    row2["name"] = "k_bob";
    Row* r2 = Row::create(schema, row2);
    idxtbl->insert(r2);
    map<string, Value> row2b;
    row2b["id"] = (i32) 2;
    row2b["name"] = "n_bob_2";
    Row* r2b = Row::create(schema, row2b);
    idxtbl->insert(r2b);

    unordered_map<string, Value> row3;
    row3["id"] = (i32) 3;
    row3["name"] = "d_cathy";
    Row* r3 = Row::create(schema, row3);
    idxtbl->insert(r3);
    unordered_map<string, Value> row3b;
    row3b["id"] = (i32) 3;
    row3b["name"] = "p_cathy_2";
    Row* r3b = Row::create(schema, row3b);
    idxtbl->insert(r3b);

    Log::debug("======== begin index queries ========");
    Index idx = idxtbl->get_index("i_name");
    print_result(idx.all());

    Log::debug("remove 1 < key < 3:");
    idxtbl->remove(idxtbl->query_in(Value((i32) 1), Value((i32) 3)));
    print_table(idxtbl);
    EXPECT_TRUE(rows_are_sorted(idxtbl->all()));

    Log::debug("----");

    print_result(idx.all());

    Log::debug("remove based on index: name < x");
    idxtbl->remove(idx.query_lt(Value("x")));
    print_table(idxtbl);

    delete idxtbl;
    delete schema;
}
