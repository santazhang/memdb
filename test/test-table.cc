#include <vector>

#include "memdb/schema.h"
#include "memdb/table.h"
#include "base/all.h"

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

    unordered_map<string, Value> row3;
    row3["id"] = (i32) 3;
    row3["name"] = "cathy";
    Row* r3 = Row::create(schema, row3);
    ut->insert(r3);

    EXPECT_EQ(ut->all().count(), 3);

    // try removing row 3
    ut->remove(Value((i32) 3));

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
