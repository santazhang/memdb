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

    schema->release();
}
