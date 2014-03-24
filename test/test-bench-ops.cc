#include <string>

#include "memdb/table.h"
#include "base/all.h"

#include "test-helper.h"

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
    report_qps("inserting (UnsortedTable) rows", n, timer.elapsed());

    delete ut;
    delete schema;
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
    report_qps("inserting (SortedTable) rows", n, timer.elapsed());

    delete st;
    delete schema;
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
    report_qps("inserting (SnapshotTable) rows", n, timer.elapsed());

    delete st;
    delete schema;
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
    report_qps("stringhash32 (hello, world)", n, t.elapsed());
}

TEST(bench, stringhash64) {
    string str = "hello, world";
    const int batch_size = 100000;
    int n_batches = 0;
    Timer t;
    t.start();
    for (;;) {
        for (int i = 0; i < batch_size; i++) {
            stringhash64(str);
        }
        n_batches++;
        if (t.elapsed() > 2.0) {
            break;
        }
    }
    t.stop();
    int n = n_batches * batch_size;
    report_qps("stringhash64 (hello, world)", n, t.elapsed());
}


TEST(bench, inthash32) {
    uint32_t key1 = 1987;
    uint32_t key2 = 1001;
    const int batch_size = 100000;
    int n_batches = 0;
    Timer t;
    t.start();
    for (;;) {
        for (int i = 0; i < batch_size; i++) {
            inthash32(key1, key2);
        }
        n_batches++;
        if (t.elapsed() > 2.0) {
            break;
        }
    }
    t.stop();
    int n = n_batches * batch_size;
    report_qps("inthash32 (1987, 1001)", n, t.elapsed());
}


TEST(bench, inthash64) {
    uint64_t key1 = 1987;
    uint64_t key2 = 1001;
    const int batch_size = 100000;
    int n_batches = 0;
    Timer t;
    t.start();
    for (;;) {
        for (int i = 0; i < batch_size; i++) {
            inthash64(key1, key2);
        }
        n_batches++;
        if (t.elapsed() > 2.0) {
            break;
        }
    }
    t.stop();
    int n = n_batches * batch_size;
    report_qps("inthash64 (1987, 1001)", n, t.elapsed());
}