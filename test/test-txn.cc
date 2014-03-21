#include "base/all.h"
#include "memdb/txn.h"

using namespace std;
using namespace base;
using namespace mdb;

TEST(txn, basic_op) {
    TxnMgrUnsafe txnmgr;
    Txn* txn_unsafe = txnmgr.start(1);
    delete txn_unsafe;
}
