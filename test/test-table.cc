#include <vector>

#include "memdb/schema.h"
#include "memdb/table.h"
#include "base/all.h"

using namespace base;
using namespace mdb;
using namespace std;

TEST(table, create) {
    Schema* schema = new Schema;
    schema->add_primary_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    Table* t = new Table(schema);

    vector<Value> row1 = { Value((i32) 1), Value("alice") };
    Row* r1 = Row::create(schema, row1);
    t->insert(r1);

    EXPECT_EQ(t->query(r1->get_primary()), r1);
    r1->release();

    map<string, Value> row2;
    row2["id"] = Value((i32) 2);
    row2["name"] = Value("bob");
    Row* r2 = Row::create(schema, row2);
    t->insert(r2);
    r2->release();

    unordered_map<string, Value> row3;
    row3["id"] = Value((i32) 3);
    row3["name"] = Value("cathy");
    Row* r3 = Row::create(schema, row3);
    t->insert(r3);
    r3->release();

    // try removing row 3
    t->remove(Value((i32) 3));

    t->release();
    schema->release();
}
