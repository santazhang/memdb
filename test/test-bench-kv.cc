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

static void benchmark_kv(TxnMgr* mgr, symbol_t table_type, symbol_t row_type) {
    Schema* schema = new Schema;
    schema->add_key_column("key", Value::I32);
    schema->add_column("value", Value::STR);

    Table* table = nullptr;
    if (table_type == TBL_UNSORTED) {
        table = new UnsortedTable(schema);
    } else if (table_type == TBL_SORTED) {
        table = new SortedTable(schema);
    } else if (table_type == TBL_SNAPSHOT) {
        table = new SnapshotTable(schema);
    }

    int n_populate = 100 * 1000;
    Timer timer;
    timer.start();
    for (i32 i = 0; i < n_populate; i++) {
        array<Value, 2> row_data = { { Value(i), Value("dummy") } };
        Row* row = nullptr;
        if (row_type == ROW_BASIC) {
            row = Row::create(schema, row_data);
        } else if (row_type == ROW_COARSE) {
            row = CoarseLockedRow::create(schema, row_data);
        } else if (row_type == ROW_FINE) {
            row = FineLockedRow::create(schema, row_data);
        }
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
            while (rs) {
                Row* row = rs.next();
                //row->update(1, Value("dummy 2"));
                txn->write_column(row, 1, Value("dummy 2"));
            }
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
    benchmark_kv(&mgr, TBL_UNSORTED, ROW_BASIC);
}

TEST(benchmark, kv_2pl) {
    TxnMgr2PL mgr;
    benchmark_kv(&mgr, TBL_UNSORTED, ROW_FINE);
}
