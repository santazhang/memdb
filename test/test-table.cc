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
    schema->add_indexed_column("name", Value::STR);

    Table* t = new Table(schema);

    vector<Value> row1 = { Value(1), Value("alice") };
    Row* r1 = Row::create(schema, row1);

    t->insert(r1);

    EXPECT_EQ(t->query(r1->get_primary()), r1);

    r1->release();

    t->release();
    schema->release();
}
