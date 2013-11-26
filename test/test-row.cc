#include "pkv/schema.h"
#include "pkv/row.h"
#include "base/all.h"

using namespace base;
using namespace pkv;
using namespace std;

TEST(row, create) {
    Schema schema;
    schema.add_column("id", Value::I32);
    schema.add_column("name", Value::STR);
    vector<Value> row1 = { Value(1), Value("alice") };
    map<string, Value> row2;
    row2["id"] = Value(2);
    row2["name"] = Value("bob");
    Row* r1 = Row::create(&schema, row1);
//    Row* r2 = Row::create(&schema, row2);
    EXPECT_EQ(r1->get_column("id").get_i32(), 1);
    EXPECT_EQ(r1->get_column("name").get_str(), "alice");
//    EXPECT_EQ(r2->get_column("id").get_i32(), 2);
//    EXPECT_EQ(r2->get_column("name").get_str(), "bob");
}
