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

    std::pair<UnsortedTable::iterator, UnsortedTable::iterator> query_range = ut->query(r1->get_key());
    std::list<Row*> query_result;
    for (auto it = query_range.first; it != query_range.second; ++it) {
        query_result.push_back(it->second);
    }
    EXPECT_EQ(query_result.size(), 1);
    EXPECT_EQ(query_result.front(), r1);

    map<string, Value> row2;
    row2["id"] = Value((i32) 2);
    row2["name"] = Value("bob");
    Row* r2 = Row::create(schema, row2);
    ut->insert(r2);

    unordered_map<string, Value> row3;
    row3["id"] = Value((i32) 3);
    row3["name"] = Value("cathy");
    Row* r3 = Row::create(schema, row3);
    ut->insert(r3);

    // try removing row 3
    ut->remove(Value((i32) 3));

    // try removing all rows
    int rows_left = 0;
    auto it = ut->begin();
    while (it != ut->end()) {
        it = ut->remove(it);
        rows_left++;
    }
    EXPECT_EQ(rows_left, 2);

    delete ut;
}
