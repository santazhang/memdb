#include <string>

#include "memdb/table.h"
#include "base/all.h"

using namespace base;
using namespace mdb;
using namespace std;

TEST(bench, table_insert) {
    Schema* schema = new Schema;
    schema->add_key_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    Table* t = new Table(schema);

    const int n = 1000 * 1000;
    Timer timer;
    timer.start();
    for (int i = 0; i < n; i++) {
        vector<Value> row = { Value((i32) i), Value("dummy!") };
        Row* r = Row::create(schema, row);
        t->insert(r);
        r->release();
    }
    timer.stop();
    Log::info("inserting %d rows times takes %.2lf seconds, op/s=%.0lf",
        n, timer.elapsed(), n / timer.elapsed());

    t->release();
}

TEST(bench, stringhash32) {
    const int n = 1000 * 1000 * 10;
    string str = "hello, world";
    Timer t;
    t.start();
    for (int i = 0; i < n; i++) {
        stringhash32(str);
    }
    t.stop();
    Log::info("stringhash32('%s') %d times takes %.2lf seconds, op/s=%.0lf",
        str.c_str(), n, t.elapsed(), n / t.elapsed());
}
