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

    UnsortedTable* ut = new UnsortedTable(schema);

    const int batch_size = 100000;
    int n_batches = 0;
    Timer timer;
    timer.start();
    for (;;) {
        for (int i = 0; i < batch_size; i++) {
            vector<Value> row = { Value((i32) i), Value("dummy!") };
            Row* r = Row::create(schema, row);
            ut->insert(r);
        }
        n_batches++;
        if (timer.elapsed() > 2.0) {
            break;
        }
    }
    timer.stop();
    int n = n_batches * batch_size;
    Log::info("inserting %d rows times takes %.2lf seconds, op/s=%.0lf (UnsortedTable)",
        n, timer.elapsed(), n / timer.elapsed());

    delete ut;
}

TEST(bench, table_insert_sorted) {
    Schema* schema = new Schema;
    schema->add_key_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    SortedTable* st = new SortedTable(schema);

    const int batch_size = 100000;
    int n_batches = 0;
    Timer timer;
    timer.start();
    for (;;) {
        for (int i = 0; i < batch_size; i++) {
            vector<Value> row = { Value((i32) i), Value("dummy!") };
            Row* r = Row::create(schema, row);
            st->insert(r);
        }
        n_batches++;
        if (timer.elapsed() > 2.0) {
            break;
        }
    }
    timer.stop();
    int n = n_batches * batch_size;
    Log::info("inserting %d rows times takes %.2lf seconds, op/s=%.0lf (SortedTable)",
        n, timer.elapsed(), n / timer.elapsed());

    delete st;
}

TEST(bench, table_insert_snapshot) {
    Schema* schema = new Schema;
    schema->add_key_column("id", Value::I32);
    schema->add_column("name", Value::STR);

    SnapshotTable* st = new SnapshotTable(schema);

    const int batch_size = 100000;
    int n_batches = 0;
    Timer timer;
    timer.start();
    for (;;) {
        for (int i = 0; i < batch_size; i++) {
            vector<Value> row = { Value((i32) i), Value("dummy!") };
            Row* r = Row::create(schema, row);
            st->insert(r);
        }
        n_batches++;
        if (timer.elapsed() > 2.0) {
            break;
        }
    }
    timer.stop();
    int n = n_batches * batch_size;
    Log::info("inserting %d rows times takes %.2lf seconds, op/s=%.0lf (SnapshotTable)",
        n, timer.elapsed(), n / timer.elapsed());

    delete st;
}

TEST(bench, stringhash32) {
    string str = "hello, world";
    const int batch_size = 100000;
    int n_batches = 0;
    Timer t;
    t.start();
    for (;;) {
        for (int i = 0; i < batch_size; i++) {
            stringhash32(str);
        }
        n_batches++;
        if (t.elapsed() > 2.0) {
            break;
        }
    }
    t.stop();
    int n = n_batches * batch_size;
    Log::info("stringhash32('%s') %d times takes %.2lf seconds, op/s=%.0lf",
        str.c_str(), n, t.elapsed(), n / t.elapsed());
}
