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
    delete r1;

    map<string, Value> row2;
    row2["id"] = Value(2);
    row2["name"] = Value("bob");
    Row* r2 = Row::create(schema, row2);
    EXPECT_EQ(r2->get_column("id").get_i32(), 2);
    EXPECT_EQ(r2->get_column("name").get_str(), "bob");
    delete r2;

    unordered_map<string, Value> row3;
    row3["id"] = Value(3);
    row3["name"] = Value("cathy");
    Row* r3 = Row::create(schema, row3);
    EXPECT_EQ(r3->get_column("id").get_i32(), 3);
    EXPECT_EQ(r3->get_column("name").get_str(), "cathy");
    delete r3;

    delete schema;
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
    delete r1;

    map<string, Value> row2;
    row2["id"] = Value(2);
    row2["name"] = Value("bob");
    Row* r2 = Row::create(schema, row2);
    EXPECT_EQ(r2->get_column("id").get_i32(), 2);
    EXPECT_EQ(r2->get_column("name").get_str(), "bob");
    r2->make_sparse();
    EXPECT_EQ(r2->get_column("id").get_i32(), 2);
    EXPECT_EQ(r2->get_column("name").get_str(), "bob");
    delete r2;

    unordered_map<string, Value> row3;
    row3["id"] = Value(3);
    row3["name"] = Value("cathy");
    Row* r3 = Row::create(schema, row3);
    EXPECT_EQ(r3->get_column("id").get_i32(), 3);
    EXPECT_EQ(r3->get_column("name").get_str(), "cathy");
    r3->make_sparse();
    EXPECT_EQ(r3->get_column("id").get_i32(), 3);
    EXPECT_EQ(r3->get_column("name").get_str(), "cathy");
    delete r3;

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
    delete r1;

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
    delete r1;

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
    delete r1;

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

    delete r1;
    delete r2;

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
    delete r1;

    map<string, Value> row2;
    row2["id"] = Value(2);
    row2["name"] = Value("bob");
    CoarseLockedRow* r2 = CoarseLockedRow::create(schema, row2);
    EXPECT_EQ(r2->get_column("id").get_i32(), 2);
    EXPECT_EQ(r2->get_column("name").get_str(), "bob");
    delete r2;

    unordered_map<string, Value> row3;
    row3["id"] = Value(3);
    row3["name"] = Value("cathy");
    CoarseLockedRow* r3 = CoarseLockedRow::create(schema, row3);
    EXPECT_EQ(r3->get_column("id").get_i32(), 3);
    EXPECT_EQ(r3->get_column("name").get_str(), "cathy");
    delete r3;

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
    delete r1;

    map<string, Value> row2;
    row2["id"] = Value(2);
    row2["name"] = Value("bob");
    FineLockedRow* r2 = FineLockedRow::create(schema, row2);
    EXPECT_EQ(r2->get_column("id").get_i32(), 2);
    EXPECT_EQ(r2->get_column("name").get_str(), "bob");
    delete r2;

    unordered_map<string, Value> row3;
    row3["id"] = Value(3);
    row3["name"] = Value("cathy");
    FineLockedRow* r3 = FineLockedRow::create(schema, row3);
    EXPECT_EQ(r3->get_column("id").get_i32(), 3);
    EXPECT_EQ(r3->get_column("name").get_str(), "cathy");
    delete r3;

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
    delete r1;
}

