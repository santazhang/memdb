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
