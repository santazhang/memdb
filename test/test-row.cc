#include "memdb/schema.h"
#include "memdb/row.h"
#include "base/all.h"

using namespace base;
using namespace mdb;
using namespace std;

TEST(row, create) {
    Schema* schema = new Schema;
    schema->add_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    vector<Value> row1 = { Value(1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    EXPECT_EQ(r1->get_column("id").get_i32(), 1);
    EXPECT_EQ(r1->get_column("name").get_str(), "alice");
    r1->release();

    map<string, Value> row2;
    row2["id"] = Value(2);
    row2["name"] = Value("bob");
    Row* r2 = Row::create(schema, row2);
    EXPECT_EQ(r2->get_column("id").get_i32(), 2);
    EXPECT_EQ(r2->get_column("name").get_str(), "bob");
    r2->release();

    unordered_map<string, Value> row3;
    row3["id"] = Value(3);
    row3["name"] = Value("cathy");
    Row* r3 = Row::create(schema, row3);
    EXPECT_EQ(r3->get_column("id").get_i32(), 3);
    EXPECT_EQ(r3->get_column("name").get_str(), "cathy");
    r3->release();

    multimap<string, Value> row4;
    insert_into_map(row4, "id", Value(4));
    insert_into_map(row4, "name", Value("david"));
    Row* r4 = Row::create(schema, row4);
    EXPECT_EQ(r4->get_column("id").get_i32(), 4);
    EXPECT_EQ(r4->get_column("name").get_str(), "david");
    r4->release();

    delete schema;
}

TEST(row, cloning) {
    Schema schema;
    schema.add_column("id", Value::I32);
    schema.add_column("id2", Value::I32);
    schema.add_column("name", Value::STR);
    schema.add_column("name2", Value::STR);
    schema.add_column("name3", Value::STR);

    unordered_map<string, Value> row1;
    row1["id"] = Value(3);
    row1["id2"] = Value(3);
    row1["name"] = Value("cathy");
    row1["name2"] = Value("ca_col2");
    row1["name3"] = Value("ca_col3");
    Row* r1 = Row::create(&schema, row1);
    EXPECT_EQ(r1->get_column("id").get_i32(), 3);
    EXPECT_EQ(r1->get_column("id2").get_i32(), 3);
    EXPECT_EQ(r1->get_column("name").get_str(), "cathy");
    EXPECT_EQ(r1->get_column("name2").get_str(), "ca_col2");
    EXPECT_EQ(r1->get_column("name3").get_str(), "ca_col3");
    Row* r1_copy = r1->copy();
    EXPECT_EQ(r1->schema(), r1_copy->schema());
    EXPECT_TRUE(*r1 == *r1_copy);
    EXPECT_EQ(r1_copy->get_column("id").get_i32(), 3);
    EXPECT_EQ(r1_copy->get_column("id2").get_i32(), 3);
    EXPECT_EQ(r1_copy->get_column("name").get_str(), "cathy");
    EXPECT_EQ(r1_copy->get_column("name2").get_str(), "ca_col2");
    EXPECT_EQ(r1_copy->get_column("name3").get_str(), "ca_col3");
    r1->release();
    r1_copy->release();
}

TEST(row, cloning_on_sparse_row) {
    Schema schema;
    schema.add_column("id", Value::I32);
    schema.add_column("id2", Value::I32);
    schema.add_column("name", Value::STR);
    schema.add_column("name2", Value::STR);
    schema.add_column("name3", Value::STR);

    unordered_map<string, Value> row1;
    row1["id"] = Value(3);
    row1["id2"] = Value(3);
    row1["name"] = Value("cathy");
    row1["name2"] = Value("cathy_col2");
    row1["name3"] = Value("cathy_col3");
    Row* r1 = Row::create(&schema, row1);
    EXPECT_EQ(r1->get_column("id").get_i32(), 3);
    EXPECT_EQ(r1->get_column("id2").get_i32(), 3);
    EXPECT_EQ(r1->get_column("name").get_str(), "cathy");
    EXPECT_EQ(r1->get_column("name2").get_str(), "cathy_col2");
    EXPECT_EQ(r1->get_column("name3").get_str(), "cathy_col3");
    r1->make_sparse();
    Row* r1_copy = r1->copy();
    EXPECT_EQ(r1->schema(), r1_copy->schema());
    EXPECT_TRUE(*r1 == *r1_copy);
    EXPECT_EQ(r1_copy->get_column("id").get_i32(), 3);
    EXPECT_EQ(r1_copy->get_column("id2").get_i32(), 3);
    EXPECT_EQ(r1_copy->get_column("name").get_str(), "cathy");
    EXPECT_EQ(r1_copy->get_column("name2").get_str(), "cathy_col2");
    EXPECT_EQ(r1_copy->get_column("name3").get_str(), "cathy_col3");
    r1->release();
    r1_copy->release();
}

TEST(row, more_cloning) {
    Schema schema;
    schema.add_column("id", Value::I32);
    schema.add_column("name", Value::STR);

    unordered_map<string, Value> row1;
    row1["id"] = Value(3);
    row1["name"] = Value("cathy");

    {
        FineLockedRow* r1 = FineLockedRow::create(&schema, row1);
        FineLockedRow* r2 = (FineLockedRow *) r1->copy();
        r2->wlock_column_by(1, 1);
        r1->release();
        r2->release();
    }

    {
        CoarseLockedRow* r1 = CoarseLockedRow::create(&schema, row1);
        CoarseLockedRow* r2 = (CoarseLockedRow *) r1->copy();
        r2->wlock_row_by(1);
        r1->release();
        r2->release();
    }

    {
        VersionedRow* r1 = VersionedRow::create(&schema, row1);
        VersionedRow* r2 = (VersionedRow *) r1->copy();
        r2->incr_column_ver(1);
        r1->release();
        r2->release();
    }
}

TEST(row, make_sparse) {
    Schema* schema = new Schema;
    schema->add_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    vector<Value> row1 = { Value(1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    EXPECT_EQ(r1->get_column("id").get_i32(), 1);
    EXPECT_EQ(r1->get_column("name").get_str(), "alice");
    r1->make_sparse();
    EXPECT_EQ(r1->get_column("id").get_i32(), 1);
    EXPECT_EQ(r1->get_column("name").get_str(), "alice");
    r1->release();

    map<string, Value> row2;
    row2["id"] = Value(2);
    row2["name"] = Value("bob");
    Row* r2 = Row::create(schema, row2);
    EXPECT_EQ(r2->get_column("id").get_i32(), 2);
    EXPECT_EQ(r2->get_column("name").get_str(), "bob");
    r2->make_sparse();
    EXPECT_EQ(r2->get_column("id").get_i32(), 2);
    EXPECT_EQ(r2->get_column("name").get_str(), "bob");
    r2->release();

    unordered_map<string, Value> row3;
    row3["id"] = Value(3);
    row3["name"] = Value("cathy");
    Row* r3 = Row::create(schema, row3);
    EXPECT_EQ(r3->get_column("id").get_i32(), 3);
    EXPECT_EQ(r3->get_column("name").get_str(), "cathy");
    r3->make_sparse();
    EXPECT_EQ(r3->get_column("id").get_i32(), 3);
    EXPECT_EQ(r3->get_column("name").get_str(), "cathy");
    r3->release();

    delete schema;
}


TEST(row, update) {
    Schema* schema = new Schema;
    schema->add_key_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    vector<Value> row1 = { Value(1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    EXPECT_EQ(r1->get_column("id").get_i32(), 1);
    EXPECT_EQ(r1->get_column("name").get_str(), "alice");
    r1->update(0, (i32) 4);
    r1->update("name", "david awesome");
    EXPECT_EQ(r1->get_column("id").get_i32(), 4);
    EXPECT_EQ(r1->get_column("name").get_str(), "david awesome");
    r1->release();

    delete schema;
}

TEST(row, update_string_key_column) {
    Schema* schema = new Schema;
    schema->add_column("id", Value::I32);
    schema->add_key_column("name", Value::STR);

    vector<Value> row1 = { Value(1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    EXPECT_EQ(r1->get_column("id").get_i32(), 1);
    EXPECT_EQ(r1->get_column("name").get_str(), "alice");
    r1->update(0, (i32) 4);
    r1->update("name", "david awesome");
    EXPECT_EQ(r1->get_column("id").get_i32(), 4);
    EXPECT_EQ(r1->get_column("name").get_str(), "david awesome");
    r1->release();

    delete schema;
}

TEST(row, update_compound_key) {
    Schema* schema = new Schema;
    schema->add_key_column("id", Value::I32);
    schema->add_key_column("name", Value::STR);

    vector<Value> row1 = { Value(1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    EXPECT_EQ(r1->get_column("id").get_i32(), 1);
    EXPECT_EQ(r1->get_column("name").get_str(), "alice");
    r1->update(0, (i32) 4);
    r1->update("name", "david awesome");
    EXPECT_EQ(r1->get_column("id").get_i32(), 4);
    EXPECT_EQ(r1->get_column("name").get_str(), "david awesome");
    r1->release();

    delete schema;
}

TEST(row, compare) {
    Schema* schema = new Schema;
    schema->add_key_column("id", Value::I32);
    schema->add_key_column("name", Value::STR);

    vector<Value> row1 = { Value(i32(1)), Value("alice") };
    Row* r1 = Row::create(schema, row1);

    vector<Value> row2 = { Value(i32(1)), Value("alice") };
    Row* r2 = Row::create(schema, row1);

    EXPECT_FALSE(*r1 < *r2);
    EXPECT_FALSE(*r1 > *r2);
    EXPECT_FALSE(*r1 != *r2);
    EXPECT_TRUE(*r1 == *r2);
    EXPECT_TRUE(*r1 >= *r2);
    EXPECT_TRUE(*r1 <= *r2);

    r1->update(1, Value("bob"));

    EXPECT_FALSE(*r1 < *r2);
    EXPECT_TRUE(*r1 > *r2);
    EXPECT_TRUE(*r1 != *r2);
    EXPECT_FALSE(*r1 == *r2);
    EXPECT_TRUE(*r1 >= *r2);
    EXPECT_FALSE(*r1 <= *r2);

    r1->release();
    r2->release();

    delete schema;
}

TEST(locked_row, coarse_locked_row) {
    Schema* schema = new Schema;
    schema->add_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    vector<Value> row1 = { Value(1), Value("alice") };
    CoarseLockedRow* r1 = CoarseLockedRow::create(schema, row1);
    EXPECT_EQ(r1->get_column("id").get_i32(), 1);
    EXPECT_EQ(r1->get_column("name").get_str(), "alice");
    r1->release();

    map<string, Value> row2;
    row2["id"] = Value(2);
    row2["name"] = Value("bob");
    CoarseLockedRow* r2 = CoarseLockedRow::create(schema, row2);
    EXPECT_EQ(r2->get_column("id").get_i32(), 2);
    EXPECT_EQ(r2->get_column("name").get_str(), "bob");
    r2->release();

    unordered_map<string, Value> row3;
    row3["id"] = Value(3);
    row3["name"] = Value("cathy");
    CoarseLockedRow* r3 = CoarseLockedRow::create(schema, row3);
    EXPECT_EQ(r3->get_column("id").get_i32(), 3);
    EXPECT_EQ(r3->get_column("name").get_str(), "cathy");
    r3->release();

    delete schema;
}

TEST(locked_row, fine_locked_row) {
    Schema* schema = new Schema;
    schema->add_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    vector<Value> row1 = { Value(1), Value("alice") };
    FineLockedRow* r1 = FineLockedRow::create(schema, row1);
    EXPECT_EQ(r1->get_column("id").get_i32(), 1);
    EXPECT_EQ(r1->get_column("name").get_str(), "alice");
    r1->release();

    map<string, Value> row2;
    row2["id"] = Value(2);
    row2["name"] = Value("bob");
    FineLockedRow* r2 = FineLockedRow::create(schema, row2);
    EXPECT_EQ(r2->get_column("id").get_i32(), 2);
    EXPECT_EQ(r2->get_column("name").get_str(), "bob");
    r2->release();

    unordered_map<string, Value> row3;
    row3["id"] = Value(3);
    row3["name"] = Value("cathy");
    FineLockedRow* r3 = FineLockedRow::create(schema, row3);
    EXPECT_EQ(r3->get_column("id").get_i32(), 3);
    EXPECT_EQ(r3->get_column("name").get_str(), "cathy");
    r3->release();

    delete schema;
}

TEST(versioned_row, update_ver) {
    Schema schema;
    schema.add_column("id", Value::I32);
    schema.add_column("name", Value::STR);

    vector<Value> row1 = { Value(1), Value("alice") };
    VersionedRow* r1 = VersionedRow::create(&schema, row1);
    EXPECT_EQ(r1->get_column_ver(0), 0);
    EXPECT_EQ(r1->get_column_ver(1), 0);
    ((Row *) r1)->update(0, i32(1987));
    EXPECT_EQ(r1->get_column(0), Value(i32(1987)));
    r1->update(1, "cynthia");
    EXPECT_EQ(r1->get_column(1), Value("cynthia"));
    r1->release();
}

