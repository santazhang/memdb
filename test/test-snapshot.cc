#include <string>

#include "base/all.h"
#include "memdb/snapshot.h"

using namespace base;
using namespace mdb;
using namespace std;

TEST(snapshot, snapshot_on_empty_table) {
    snapshot_sortedmap<int, string> data;
    EXPECT_EQ(data.all_snapshots().count(), 1);
    {
        snapshot_sortedmap<int, string> snapshot = data.snapshot();
        EXPECT_EQ(data.all_snapshots().count(), 2);
        EXPECT_EQ(snapshot.all_snapshots().count(), 2);
        data.snapshot();
        data.snapshot();
        snapshot = snapshot.snapshot();
        EXPECT_EQ(snapshot.all_snapshots().count(), 2);
        EXPECT_EQ(data.version(), 0);
        EXPECT_EQ(snapshot.version(), 0);
        EXPECT_TRUE(data.valid());
        EXPECT_TRUE(snapshot.valid());
        EXPECT_FALSE(data.readonly());
        data = snapshot.snapshot();
        EXPECT_TRUE(data.readonly());
        EXPECT_TRUE(snapshot.readonly());
        EXPECT_EQ(data.all_snapshots().count(), 2);
    }
    EXPECT_EQ(data.all_snapshots().count(), 1);
}
