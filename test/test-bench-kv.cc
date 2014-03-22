#include <array>

#include "base/all.h"
#include "memdb/schema.h"
#include "memdb/row.h"
#include "memdb/table.h"
#include "memdb/txn.h"

using namespace std;
using namespace mdb;
using namespace base;

static void report_qps(const char* action, int n_ops, double duration) {
    Log::info("%s: %d ops, took %.2lf sec, qps=%s", action, n_ops, duration, format_decimal(n_ops / duration).c_str());
}

static void benchmark_kv(TxnMgr* mgr) {
    Schema* schema = new Schema;
    schema->add_key_column("key", Value::I32);
    schema->add_column("value", Value::STR);

    Table* table = new SortedTable(schema);

    int n_populate = 100 * 1000;
    Timer timer;
    timer.start();
    for (i32 i = 0; i < n_populate; i++) {
        array<Value, 2> row_data = { { Value(i), Value("dummy") } };
        Row* row = Row::create(schema, row_data);
        table->insert(row);
    }
    timer.stop();
    report_qps("populating table", n_populate, timer.elapsed());

    // do updates
    Counter txn_counter;
    const int batch_size = 10 * 1000;
    int n_batches = 0;
    timer.start();
    Rand rnd;
    for (;;) {
        for (int i = 0; i < batch_size; i++) {
            txn_id_t txnid = txn_counter.next();
            Txn* txn = mgr->start(txnid);
            ResultSet rs = txn->query(table, Value(i32(rnd.next(0, n_populate))));
            txn->commit();
        }
        n_batches++;
        if (timer.elapsed() > 2.0) {
            break;
        }
    }
    timer.stop();
    int n_update = n_batches * batch_size;
    report_qps("update rows", n_update, timer.elapsed());

    delete table;
}

TEST(benchmark, kv_unsafe) {
    TxnMgrUnsafe mgr;
    benchmark_kv(&mgr);
}

TEST(benchmark, kv_2pl) {
    TxnMgr2PL mgr;
    benchmark_kv(&mgr);
}
